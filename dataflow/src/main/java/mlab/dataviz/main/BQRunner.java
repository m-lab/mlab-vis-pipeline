package mlab.dataviz.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mlab.dataviz.pipelines.BigqueryTransferPipeline;

public class BQRunner {
	private static final Logger LOG = LoggerFactory.getLogger(BQRunner.class);
	private static int dayCounter = 0;

	public static void main(String[] args) throws InterruptedException {

		LOG.info(">>> Starting Pipeline Runner server");
		MetricsServer m = new MetricsServer();
		BigqueryTransferPipeline dayPipeline = new BigqueryTransferPipeline(args, "day");
		BigqueryTransferPipeline hourPipeline = new BigqueryTransferPipeline(args, "hour");

		// first run, refresh NDT tables.
		dayPipeline.setRefreshNDTTable(true);
		hourPipeline.setRefreshNDTTable(true);

		// historic pipeline thread
		Thread dayPipelineThread = new Thread(dayPipeline);
		Thread hourPipelineThread = new Thread(hourPipeline);

		// Prometheus http server
		Thread metricsThread = new Thread(m);

		metricsThread.start();
		dayPipelineThread.start();
		hourPipelineThread.start();
		dayCounter++;

		LOG.info(">>> Metrics server running");
		LOG.info(">>> Pipeline threads running");

		// default number of days at which to run the bq pipeline. 1 by default.
		float everyNDays = 1;
		String everyNDaysEnv = System.getenv("RUN_BQ_UPDATE_EVERY");
		if (everyNDaysEnv != null) {
			everyNDays = Float.parseFloat(everyNDaysEnv);
		}

		// days between full refreshes of the bq tables. 7 by default.
		float daysBeforeFullRefresh = 7;
		String daysBeforeFullRefreshEnv = System.getenv("DAYS_BEFORE_FULL_BQ_REFRESH");
		if (daysBeforeFullRefreshEnv != null) {
			daysBeforeFullRefresh = Float.parseFloat(daysBeforeFullRefreshEnv);
		}

		while (metricsThread.isAlive()) {
			Thread.sleep((long) Math.round(8.64e+7 * everyNDays)); // 1 day's milliseconds * days
			LOG.info(">>> Wake up to check if pipeline still running.");
			Thread.sleep(1000);
			System.out.println("Checking in");

			// restart day pipeline
			if (!dayPipelineThread.isAlive() || !dayPipeline.getStatus()) {
				LOG.info(">>> Starting pipeline thread");
				dayPipelineThread = new Thread(dayPipeline);

				// if we've run enough cycles, do a clean refresh of the NDT tables
				if (dayCounter >= daysBeforeFullRefresh) {
					LOG.info(">>>>> Doing a refresh of our Day NDT tables");
					dayPipeline.setRefreshNDTTable(true);
				} else {
					dayPipeline.setRefreshNDTTable(false);
				}
				dayPipelineThread.start();
			} else {
				LOG.info(">>> Day Pipeline still running. Going back to sleep.");
			}

			// restart hour pipeline
			if (!hourPipelineThread.isAlive() || !hourPipeline.getStatus()) {
				LOG.info(">>> Starting pipeline thread");
				// reset the pipeline
				hourPipelineThread = new Thread(hourPipeline);

				// if we've run enough cycles, do a clean refresh of the NDT tables
				if (dayCounter >= daysBeforeFullRefresh) {
					LOG.info(">>>>> Doing a refresh of our Hour NDT tables");
					hourPipeline.setRefreshNDTTable(true);
				} else {
					hourPipeline.setRefreshNDTTable(false);
				}
				hourPipelineThread.start();
			} else {
				LOG.info(">>> Hour Pipeline still running. Going back to sleep.");
			}

			// reset the counter if we need to, otherwise increment it
			// since only a regular run occured.
			if (dayCounter >= daysBeforeFullRefresh) {
				dayCounter = 0;
			} else {
				dayCounter += 1;
			}
		}
	}
}
