package mlab.dataviz.pipelines;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.prometheus.client.Gauge;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.dataviz.entities.BQPipelineRun;
import mlab.dataviz.entities.BQPipelineRunDatastore;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.BigQueryIOHelpers;
import mlab.dataviz.util.Formatters;
import mlab.dataviz.util.Schema;
import mlab.dataviz.util.PipelineConfig;

public class BigqueryTransferPipeline implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(BigqueryTransferPipeline.class);

	// Config options download/upload + day/hour property names
	private static String PIPELINE_CONFIG_FILENAME = "./data/bigquery/pipeline_config.json";
	private static String DOWNLOADS_BY_DAY = "downloads_ip_by_day_base";
	private static String DOWNLOADS_BY_HOUR = "downloads_ip_by_hour_base";
	private static String UPLOADS_BY_DAY = "uploads_ip_by_day_base";
	private static String UPLOADS_BY_HOUR = "uploads_ip_by_hour_base";

	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP);

	private BQPipelineRun status;
	private BQPipelineRunDatastore datastore = null;

	private final String[] args;
	private boolean isRunning = false;
	private String timePeriod;

	/**
	 * @constructor
	 * Creates a new historic pipeline
	 */
	public BigqueryTransferPipeline(String[] args, String timePeriod) {
		 this.args = args;
		 this.timePeriod = timePeriod;

		 try {
			this.datastore = new BQPipelineRunDatastore();
		 } catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		 }
	}

	/**
	 * @private
	 * Switches the pipeline config to the one that is tailored for this run.
	 * @param type  "downloads" or "uploads"
	 * @return PipelineConfig config object for the correct run.
	 */
	private PipelineConfig getPiplineConfig(String type) throws Exception {
		String keyName = null;
		if (type.equals("downloads")) {
			if (this.timePeriod.equals("hour")) {
				keyName = DOWNLOADS_BY_HOUR;
			} else if (this.timePeriod.equals("day")) {
				keyName = DOWNLOADS_BY_DAY;
			}
		} else if (type.equals("uploads")) {
			if (this.timePeriod.equals("hour")) {
				keyName = UPLOADS_BY_HOUR;
			} else if (this.timePeriod.equals("day")) {
				keyName = UPLOADS_BY_DAY;
			}
		}

		return new PipelineConfig(PIPELINE_CONFIG_FILENAME, keyName);
	}

	/**
	 * Get the run dates. If they were provided, use those.
	 * If not, try to find them in datastore.
	 * If not, find them in bq.
	 * @param options
	 * @return
	 */
	private static String[] getDatesFromCommandline(HistoricPipelineOptions options) {
		String[] dates = new String[2];

		if (options.getStartDate() != null && (options.getEndDate() != null)) {
			dates[0] = options.getStartDate() + "T00:00:00Z"; // start of day
			dates[1] = options.getEndDate() + "T23:59:59Z"; // end of day
			return dates;
		}
		return null;
	}

	/**
	 * Creates a datastore record of this pipeline run.
	 * Initializes its start date and run status and write it to the store.
	 * @return VizPipelineRun record.
	 * @throws SQLException
	 */
	private BQPipelineRun createRunRecord() throws SQLException {
		Calendar c = Calendar.getInstance();
		Date time = c.getTime();
		String runDate = dateFormatter.format(time);

		BQPipelineRun record = new BQPipelineRun.Builder()
				.run_start_date(runDate)
				.status(BQPipelineRun.STATUS_RUNNING)
				.build();

		// write it to the datastore
		long id = this.datastore.createBQPipelineRunEntity(record);
		record.setId(id);
		return record;
	}

	/**
	 * Runs the historic pipeline. Core logic goes here.
	 */
	public void run() {
		this.isRunning = true;

		PipelineOptionsFactory.register(HistoricPipelineOptions.class);
		HistoricPipelineOptions options = PipelineOptionsFactory.fromArgs(this.args)
				.withValidation()
				.as(HistoricPipelineOptions.class);

		Gauge duration = Gauge.build().name("mlab_vis_pipeline_historic_bigquery_duration_" + this.timePeriod)
				.help("Historic pipeline duration - Bigquery").register();

		Gauge.Timer durationTimer = duration.startTimer();

		try {
			// record when this is running in the datastore so we have
			// a record of when we are running.
			this.status = createRunRecord();
			int skipNDTRead = options.getSkipNDTRead();

			int test = options.getTest();
			Boolean shouldExecute = true;
			if (test == 1) {
				shouldExecute = false;
			}

			// configuration objects for the two parallel pipelines.
			PipelineConfig downloadsConfig = this.getPiplineConfig("downloads");
			PipelineConfig uploadsConfig = this.getPiplineConfig("uploads");

			// the downloads and uploads pipelines are blocking, so in order to make them
			// only blocking within their own run routines, we are extracting them
			// into their own threads, that will then rejoin when they are complete.
			// this allows these two to run in parallel.

			boolean next = true;
			if (skipNDTRead != 1) {

				// prometheus, ndt read duration
				Gauge ndtReadDuration = Gauge.build().name("mlab_vis_pipeline_historic_bigquery_ndtread_duration_" +  this.timePeriod)
						.help("Historic pipeline duration - Bigquery NDT Read").register();

				Gauge.Timer ndtTimer = ndtReadDuration.startTimer();

				// === get downloads for timePeriod
				options.setAppName("HistoricPipeline-Download-" + this.timePeriod + "-" + System.currentTimeMillis());

				NDTReadPipeline ehrPDL = new NDTReadPipeline(options, this.status);
				Thread dlPipeThread = new Thread(ehrPDL);

				// See if dates were provided via the command line.
				String[] dates =  getDatesFromCommandline(options);

				LOG.info("Downloads configuration for " + this.timePeriod);
				ehrPDL.setPipelineConfiguration(downloadsConfig)
						.setDates(dates)
						.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.setWriteDisposition(WriteDisposition.WRITE_APPEND)
						.shouldExecute(shouldExecute);

				// === get uploads for timePeriod
				// set up big query IO options (it doesn't seem to let us share the download
				// ones)
				HistoricPipelineOptions optionsUl = options.cloneAs(HistoricPipelineOptions.class);
				optionsUl.setAppName("HistoricPipeline-Upload-" + this.timePeriod + "-" + System.currentTimeMillis());

				NDTReadPipeline ehrPUL = new NDTReadPipeline(optionsUl, this.status);
				Thread ulPipeThread = new Thread(ehrPUL);


				LOG.info("Uploads configuration for " + this.timePeriod);
				ehrPUL.setPipelineConfiguration(uploadsConfig)
						.setDates(dates)
						.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.setWriteDisposition(WriteDisposition.WRITE_APPEND)
						.shouldExecute(shouldExecute);

				// write the status to datastore to capture he run dates
				this.datastore.updateBQPipelineRunEntity(this.status);

				// start the two threads
				LOG.info("Starting upload/download threads");
				dlPipeThread.start();
				ulPipeThread.start();

				// wait for the two threads to finish
				LOG.info("Joining upload/download threads");
				dlPipeThread.join();
				ulPipeThread.join();

				next = ehrPUL.getState() == State.DONE && ehrPDL.getState() == State.DONE;

				// prometheus, ndt read duration write.
				ndtTimer.setDuration();
			}

			// when both pipelines are done, proceed to merge, add ISP and local time
			// information.
			if (next) {

				// prometheus, augmentation & merge pipeline write
				Gauge mergeDuration = Gauge.build().name("mlab_vis_pipeline_historic_bigquery_merge_duration_" +  this.timePeriod)
						.help("Historic pipeline duration - Bigquery Merge").register();

				Gauge.Timer mergeTimer = mergeDuration.startTimer();

				// set up big query IO options
				HistoricPipelineOptions optionsMergeAndISP = options.cloneAs(HistoricPipelineOptions.class);
				optionsMergeAndISP
						.setAppName("HistoricPipeline-MergeAndISP-" + this.timePeriod + "-" + System.currentTimeMillis());
				Pipeline pipe = Pipeline.create(optionsMergeAndISP);

				BQTableUtils bqUtils = new BQTableUtils(options);

				// === merge upload and download into a single set of rows (outputs a table and
				// also gives the rows back)
				MergeUploadDownloadPipeline mergeUploadDownload = new MergeUploadDownloadPipeline(pipe);

				mergeUploadDownload
						.setDownloadTable(bqUtils.wrapTableWithBrakets(downloadsConfig.getOutputTable()))
						.setUploadTable(bqUtils.wrapTableWithBrakets(uploadsConfig.getOutputTable()))
						.setOutputTable(bqUtils.wrapTable(downloadsConfig.getMergeTable()))
						.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
						.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

				PCollection<TableRow> rows = mergeUploadDownload.apply();

				// === add ISPs
				rows = new AddISPsPipeline(pipe, bqUtils).apply(rows);

				// === add server locations and m-lab site info
				rows = new AddMlabSitesInfoPipeline(pipe, bqUtils).apply(rows);

				// === merge ASNs
				rows = new MergeASNsPipeline(pipe, bqUtils).apply(rows);

				// === add local time
				rows = new AddLocalTimePipeline(pipe, bqUtils).apply(rows);

				// === clean locations (important to do before resolving location names)
				rows = new LocationCleaningPipeline(pipe, bqUtils).apply(rows);

				// === add location names
				rows = new AddLocationPipeline(pipe, bqUtils).apply(rows);

				// write to the final table
				BigQueryIOHelpers.writeTable(rows, bqUtils.wrapTable(downloadsConfig.getWithISPTable()),
						Schema.fromJSONFile((String) downloadsConfig.getWithISPTableSchema()),
						WriteDisposition.WRITE_TRUNCATE, CreateDisposition.CREATE_IF_NEEDED);

				if (test == 0) {
					// kick off the pipeline
					DataflowPipelineJob resultsMergeAndISPs = (DataflowPipelineJob) pipe.run();

					// wait for the pipeline to finish executing
					resultsMergeAndISPs.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));

					LOG.info("Merge + ISPs job completed for" + this.timePeriod + ", with status: " + resultsMergeAndISPs.getState().toString());
				}

				// prometheus, augmentation & merge pipeline write
				mergeTimer.setDuration();

				// mark this run Done.
				this.status.setStatus(BQPipelineRun.STATUS_DONE);
				this.datastore.updateBQPipelineRunEntity(this.status);
			} else {
				LOG.error("Download or Upload pipelines failed.");
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			this.status.setStatus(BQPipelineRun.STATUS_FAILED);
			try {
				this.datastore.updateBQPipelineRunEntity(this.status);
			} catch (SQLException e1) {
				LOG.error(e1.getMessage());
				e1.printStackTrace();
			}

		} finally {

			// write progress to datastore
			durationTimer.setDuration();
			Calendar c = Calendar.getInstance();
			Date t = c.getTime();
			String runDate = dateFormatter.format(t);
			this.status.setRunEndDate(runDate);
			try {
				this.datastore.updateBQPipelineRunEntity(this.status);
			} catch (SQLException e) {
				LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}

		this.isRunning = false;
	}

	/**
	 * Returns true if pipeline is currently running.
	 * @return
	 */
	public boolean getStatus() {
		return this.isRunning;
	}
	/**
	 * Main program.
	 *
	 * Full runtime arguments example:
	 * --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
	 * --project=mlab-sandbox --stagingLocation="gs://mlab-data-viz-sandbox"
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		BigqueryTransferPipeline hp = new BigqueryTransferPipeline(args, "day");
		hp.run();
	}
}