package mlab.dataviz.pipelines;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.beam.CloudBigtableIO;

import io.prometheus.client.Gauge;
import mlab.dataviz.entities.BTPipelineRun;
import mlab.dataviz.entities.BTPipelineRunDatastore;
import mlab.dataviz.pipelineopts.BigtableTransferPipelineOptions;
import mlab.dataviz.util.Formatters;

/**
 * Runs in parallel all the big table transfers based on the json files that
 * live inside the data/bigtable folder.
 */
public class BigtableTransferPipeline implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(BigtableTransferPipeline.class);
	private static final String BIGTABLE_CONFIG_DIR = "./data/bigtable/";
	private static final boolean RUN_IN_PARALLEL = false;

	private String[] args;
	private Gauge duration;
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP);
	private BTPipelineRun status;
	private BTPipelineRunDatastore datastore = null;

	/**
	 * @constructor Creates a new big table transfer pipeline
	 * @param args
	 *            Command line arguments
	 */
	public BigtableTransferPipeline(String[] args) {
		this.args = args;
		
		 this.duration = Gauge.build().name("mlab_vis_pipeline_bigtable")
					.help("Historic pipeline duration - Bigquery").register();
		try {
			this.datastore = new BTPipelineRunDatastore();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Returns a timestamp of the current time.
	 *
	 * @return String of the timestamp.
	 */
	private String getNowTimestamp() {
		Calendar c = Calendar.getInstance();
		Date time = c.getTime();
		return dateFormatter.format(time);
	}

	/**
	 * Creates a datastore record of this pipeline run. Initializes its start date
	 * and run status and write it to the store.
	 *
	 * @return VizPipelineRun record.
	 * @throws SQLException
	 */
	private BTPipelineRun createRunRecord() throws SQLException {
		BTPipelineRun record = new BTPipelineRun.Builder().datastore(this.datastore)
				.run_start_date(this.getNowTimestamp()).status(BTPipelineRun.STATUS_RUNNING).build();

		// write it to the datastore
		long id = this.datastore.createBTPipelineRunEntity(record);
		record.setId(id);
		return record;
	}

	private BigtableTransferPipelineOptions getOptions() {
		PipelineOptionsFactory.register(BigtableTransferPipelineOptions.class);
		BigtableTransferPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigtableTransferPipelineOptions.class);
		return options;
	}

	@Override
	public void run() {
		Pipeline pipe = null;
		
		try {
			// check to see if we already have a pipeline running.
			LOG.info("Bigtable pipeline running in parallel: " + RUN_IN_PARALLEL);
			BTPipelineRun lastRun = this.datastore.getLastBTPipelineRun();

			if (lastRun == null || lastRun.isDone()) {
				Gauge.Timer durationTimer = this.duration.startTimer();
				
				this.status = createRunRecord();

				BigtableTransferPipelineOptions options = getOptions();
				
				if (!RUN_IN_PARALLEL) {
					pipe = Pipeline.create(options);
					// TODO
				//	CloudBigtableIO.initializeForWrite(pipe);
				}
				
				int test = options.getTest();
				String configPrefix = options.getConfigPrefix();
				String configSuffix = options.getConfigSuffix();

				File folder = new File(BIGTABLE_CONFIG_DIR);
				File[] listOfFiles = folder.listFiles();

				if (configSuffix.length() == 0) {
					configSuffix = ".json";
				}
				
				ArrayList<Thread> threads = new ArrayList<Thread>();
				for (File f : listOfFiles) {
					if (f.isFile() && f.getName().endsWith(configSuffix)) {
						if (configPrefix.length() == 0 || f.getName().startsWith(configPrefix)) {
							String configFilename = BIGTABLE_CONFIG_DIR + f.getName();
							LOG.debug("Running bigtable transfer for file: " + configFilename);
							BigtablePipeline btPipeline;
							if (RUN_IN_PARALLEL) {
								// parallel, so build and kick off threads.
								btPipeline = new BigtablePipeline(this.args, configFilename);
								if (test == 0) {
									Thread btPipeThread = new Thread(btPipeline);
									btPipeThread.start();
									threads.add(btPipeThread);
								} else {
									LOG.info("Test mode, not running pipeline for " + configFilename);
								}
								
							} else {
								
								// not threaded, so just apply the transforms to the pipe and don't execute.
								btPipeline = new BigtablePipeline(pipe, configFilename);
								btPipeline.run();
							}
						}
					}
				}

				// once all jobs have been queued up, wait for them all
				if (test == 0) {
					if (RUN_IN_PARALLEL) {
						for (Thread t : threads) {
							try {
								t.join();
							} catch (InterruptedException ex) {
								LOG.error(ex.getMessage());
								ex.printStackTrace();
							}
						}
					} else if (pipe != null) {
						DataflowPipelineJob result = (DataflowPipelineJob) pipe.run();
			            try {
			                result.waitUntilFinish(Duration.ZERO, new MonitoringUtil.LoggingHandler());
			            } catch (InterruptedException | IOException e) {
			                LOG.error(e.getMessage());
			                e.printStackTrace();
			            }
					}
				}

				// once we get here, we are complete with all runs. Write status and exit.
				this.status.setRunEndDate(this.getNowTimestamp());
				this.status.setStatus(BTPipelineRun.STATUS_DONE);
				this.status.save();
				durationTimer.setDuration();
			} else {
				LOG.info("Last pipeline still running since " + lastRun.getRunStartDate() + ". Skipping this one.");
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
	}
}