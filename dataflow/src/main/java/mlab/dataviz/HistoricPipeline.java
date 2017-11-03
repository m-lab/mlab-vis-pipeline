package mlab.dataviz;

import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.BigQueryIOHelpers;
import mlab.dataviz.util.Schema;

public class HistoricPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(HistoricPipeline.class);

	private static String DOWNLOADS_BY_DAY = "./data/bigquery/batch-runs/historic/downloads_ip_by_day_base.json";
	private static String DOWNLOADS_BY_HOUR = "./data/bigquery/batch-runs/historic/downloads_ip_by_hour_base.json";
	private static String UPLOADS_BY_DAY = "./data/bigquery/batch-runs/historic/uploads_ip_by_day_base.json";
	private static String UPLOADS_BY_HOUR = "./data/bigquery/batch-runs/historic/uploads_ip_by_hour_base.json";
	private static String DOWNLOADS_SAMPLE = "./data/bigquery/batch-runs/historic/sample_download_base.json";
	private static String UPLOADS_SAMPLE = "./data/bigquery/batch-runs/historic/sample_upload_base.json";

	private static String getRunnerConfigFilename(String timePeriod, String type) {
		if (type.equals("downloads")) {
			if (timePeriod.equals("hour")) {
				return DOWNLOADS_BY_HOUR;
			} else if (timePeriod.equals("day")){
				return DOWNLOADS_BY_DAY;
			} else if (timePeriod.equals("sample")) {
				return DOWNLOADS_SAMPLE;
			}
		} else if (type.equals("uploads")) {
			if (timePeriod.equals("hour")) {
				return UPLOADS_BY_HOUR;
			} else if (timePeriod.equals("day")) {
				return UPLOADS_BY_DAY;
			} else if (timePeriod.equals("sample")){
				return UPLOADS_SAMPLE;
			}
		}
		return null;
	}
	/**
	 * @private
	 * Determine which configuration file to use based on the time period and
	 * type (downloads/uploads)
	 * @param type  "downloads" or "uploads"
	 * @param timePeriod  "day" or "hour"
	 * @return runnerConfigObj  the configuration object of the run.
	 * @throws Exception if the type is not set to the correct value.
	 */
	private static JSONObject getRunnerConfig(String timePeriod, String type) throws Exception {
		JSONParser jp = new JSONParser();
		String fileName = getRunnerConfigFilename(timePeriod, type);
		return  (JSONObject) jp.parse(new FileReader(fileName));
	}

	private static String[] getDates(HistoricPipelineOptions options) {

		String [] dates = new String[2];

		dates[0] = options.getStartDate() + "T00:00:00Z"; // start of day
		dates[1] = options.getEndDate() +  "T23:59:59Z";  // end of day

		return dates;
	}

	/**
	 * Main program. Run with --timePeriod equal to "day", "hour" or "sample".
	 *
	 * Full runtime arguments example:
	 * --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
	 * --timePeriod="sample" --project=mlab-sandbox --stagingLocation="gs://mlab-data-viz-sandbox"
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		PipelineOptionsFactory.register(HistoricPipelineOptions.class);
		HistoricPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(HistoricPipelineOptions.class);

		CollectorRegistry registry = new CollectorRegistry();
		Gauge duration = Gauge.build()
			.name("mlab_vis_pipeline_historic_bigquery_duration")
		     .help("Historic pipeline duration - Bigquery")
		     .register(registry);

		Gauge.Timer durationTimer = duration.startTimer();

		try {

			String timePeriod = options.getTimePeriod();
			int skipNDTRead = options.getSkipNDTRead();

			int test = options.getTest();
			Boolean shouldExecute = true;
			if (test == 1) {
				shouldExecute = false;
			}


		    // the downloads and uploads pipelines are blocking, so in order to make them
		    // only blocking within their own run routines, we are extracting them
		    // into their own threads, that will then rejoin when they are complete.
		    // this allows these two to run in parallel.

			boolean next = true;
			if (skipNDTRead != 1) {

				// prometheus, ndt read duration
				Gauge ndtReadDuration = Gauge.build()
						.name("mlab_vis_pipeline_historic_bigquery_ndtread_duration")
					     .help("Historic pipeline duration - Bigquery NDT Read")
					     .register(registry);

				Gauge.Timer ndtTimer = ndtReadDuration.startTimer();

				// === get downloads for timePeriod
				options.setAppName("HistoricPipeline-Download-" + timePeriod + "-" + System.currentTimeMillis());

				ExtractHistoricRowsPipeline ehrPDL = new ExtractHistoricRowsPipeline(options);
				Thread dlPipeThread = new Thread(ehrPDL);
				String downloadsConfigFile = getRunnerConfigFilename(timePeriod, "downloads");

				// we want to avoid having to have the dates in the config file.
				// NOTE: doing this means that you *must* supply dates via the command line options,
				// as the defaults will be returned from this function otherwise.
				String [] dates = getDates(options);


			    	LOG.info("Downloads configuration: " + downloadsConfigFile);
			    	ehrPDL.setConfigurationFile(downloadsConfigFile)
			    		.setDates(dates)
			    		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
			    		.setWriteDisposition(WriteDisposition.WRITE_APPEND)
			    		.shouldExecute(shouldExecute);

			    	//=== get uploads for timePeriod
			    	// set up big query IO options (it doesn't seem to let us share the download ones)
			    	HistoricPipelineOptions optionsUl = options.cloneAs(HistoricPipelineOptions.class);
					optionsUl.setAppName("HistoricPipeline-Upload-"  + timePeriod + "-" + System.currentTimeMillis());

			    	ExtractHistoricRowsPipeline ehrPUL = new ExtractHistoricRowsPipeline(optionsUl);
			    	Thread ulPipeThread = new Thread(ehrPUL);
			    	String uploadsConfigFile = getRunnerConfigFilename(timePeriod, "uploads");


			    	LOG.info("Uploads configuration: " + uploadsConfigFile);
			    	ehrPUL.setConfigurationFile(uploadsConfigFile)
			    		.setDates(dates)
			    		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
			    		.setWriteDisposition(WriteDisposition.WRITE_APPEND)
			    		.shouldExecute(shouldExecute);

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

		    // when both pipelines are done, proceed to merge, add ISP and local time information.
		    if (next) {

		    		// prometheus, augmentation & merge pipeline write
				Gauge mergeDuration = Gauge.build()
						.name("mlab_vis_pipeline_historic_bigquery_merge_duration")
					     .help("Historic pipeline duration - Bigquery Merge")
					     .register(registry);

				Gauge.Timer mergeTimer = mergeDuration.startTimer();

				// ==== get tables merged (a pipeline for merge + ISPs)
				JSONObject downloadsConfig = getRunnerConfig(timePeriod, "downloads");
				JSONObject uploadsConfig = getRunnerConfig(timePeriod, "uploads");

				// set up big query IO options
				HistoricPipelineOptions optionsMergeAndISP = options.cloneAs(HistoricPipelineOptions.class);
				optionsMergeAndISP.setAppName("HistoricPipeline-MergeAndISP-" + timePeriod + "-" + System.currentTimeMillis());
				Pipeline pipe = Pipeline.create(optionsMergeAndISP);

				BQTableUtils bqUtils = new BQTableUtils(options);

				// === merge upload and download into a single set of rows (outputs a table and also gives the rows back)
				MergeUploadDownloadPipeline mergeUploadDownload = new MergeUploadDownloadPipeline(pipe);

				mergeUploadDownload.setDownloadTable(bqUtils.wrapTableWithBrakets((String) downloadsConfig.get("outputTable")))
						.setUploadTable(bqUtils.wrapTableWithBrakets((String) uploadsConfig.get("outputTable")))
						.setOutputTable(bqUtils.wrapTable((String) downloadsConfig.get("mergeTable")))
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
				BigQueryIOHelpers.writeTable(
						rows,
						bqUtils.wrapTable((String) downloadsConfig.get("withISPTable")),
						Schema.fromJSONFile((String) downloadsConfig.get("withISPTableSchema")),
						WriteDisposition.WRITE_TRUNCATE,
						CreateDisposition.CREATE_IF_NEEDED);

				if (test == 0) {
					// kick off the pipeline
					DataflowPipelineJob resultsMergeAndISPs = (DataflowPipelineJob) pipe.run();

					// wait for the pipeline to finish executing
					resultsMergeAndISPs.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));

					LOG.info("Merge + ISPs job completed, with status: " + resultsMergeAndISPs.getState().toString());
				}

				// prometheus, augmentation & merge pipeline write
				mergeTimer.setDuration();
		    } else {
		    		LOG.error("Download or Upload pipelines failed.");
		    }
		} catch (Exception e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace().toString());

		// write pipeline stats
		} finally {
			durationTimer.setDuration();
			PushGateway pg = new PushGateway(options.getPrometheus());
			try {
				pg.pushAdd(registry, "mlab_vis_pipeline");
			} catch (Exception e) {
				LOG.error(e.getMessage());
				LOG.error(e.getStackTrace().toString());
			}
		}
	}
}
