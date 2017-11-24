package mlab.dataviz.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mlab.dataviz.pipelines.BigqueryTransferPipeline;

public class BQRunner {
	private static final Logger LOG = LoggerFactory.getLogger(BQRunner.class);


	public static void main(String[] args) throws InterruptedException {

		LOG.info(">>> Starting Pipeline Runner server");
		MetricsServer m = new MetricsServer();
		BigqueryTransferPipeline dayPipeline = new BigqueryTransferPipeline(args, "day");
		BigqueryTransferPipeline hourPipeline = new BigqueryTransferPipeline(args, "hour");

		// historic pipeline thread
		Thread dayPipelineThread = new Thread(dayPipeline);
		Thread hourPipelineThread = new Thread(hourPipeline);

		// Prometheus http server
		Thread metricsThread = new Thread(m);

		metricsThread.start();
		dayPipelineThread.start();
		hourPipelineThread.start();

		LOG.info(">>> Metrics server running");
		LOG.info(">>> Pipeline threads running");

		while (metricsThread.isAlive()) {
			Thread.sleep((long) 8.64e+7); // 1 day
			LOG.info(">>> Wake up to check if pipeline still running.");
			Thread.sleep(1000);
			System.out.println("Checking in");

			// restart day pipeline
			if (!dayPipelineThread.isAlive() || !dayPipeline.getStatus()) {
				LOG.info(">>> Starting pipeline thread");
				dayPipelineThread.start();
			} else {
				LOG.info(">>> Day Pipeline still running. Going back to sleep.");
			}

			// restart hour pipeline
			if (!hourPipelineThread.isAlive() || !hourPipeline.getStatus()) {
				LOG.info(">>> Starting pipeline thread");
				// reset the pipeline
				hourPipelineThread.start();
			} else {
				LOG.info(">>> Hour Pipeline still running. Going back to sleep.");
			}
		}
	}
}
