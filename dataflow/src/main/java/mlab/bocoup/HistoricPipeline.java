package mlab.bocoup;

import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import mlab.bocoup.pipelineopts.HistoricPipelineOptions;
import mlab.bocoup.query.BigQueryIONoLargeResults.Write.CreateDisposition;
import mlab.bocoup.query.BigQueryIONoLargeResults.Write.WriteDisposition;
import mlab.bocoup.util.PipelineOptionsSetup;
import mlab.bocoup.util.Schema;

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
	
	/**
	 * Main program. Run with --timePeriod equal to "day", "hour" or "sample".
	 * 
	 * Full runtime arguments example:
	 * --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner --numWorkers=5 
	 * --timePeriod="sample" --project=mlab-oti --stagingLocation="gs://bocoup"
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    PipelineOptionsFactory.register(HistoricPipelineOptions.class);
		HistoricPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(HistoricPipelineOptions.class);
		
		String timePeriod = options.getTimePeriod();
		int skipNDTRead = options.getSkipNDTRead();
	    
	    // the downloads and uploads pipelines are blocking, so in order to make them
	    // only blocking within their own run routines, we are extracting them
	    // into their own threads, that will then rejoin when they are complete.
	    // this allows these two to run in parallel.
	    //=== get downloads for timePeriod
	    boolean next = true;
	    if (skipNDTRead != 1) {
	    	
	    	options.setAppName("HistoricPipeline-Download");
	    	ExtractHistoricRowsPipeline ehrPDL = new ExtractHistoricRowsPipeline(options);
	    	Thread dlPipeThread = new Thread(ehrPDL);
	    	String downloadsConfigFile = getRunnerConfigFilename(timePeriod, "downloads");
	    	LOG.info("Downloads configuration: " + downloadsConfigFile);
	    	ehrPDL.setConfigurationFile(downloadsConfigFile)
	    		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
	    		.setWriteDisposition(WriteDisposition.WRITE_APPEND);
	    
	    	dlPipeThread.start();
	    
	    	// TODO. Change this asap.
	    	// Wait for this to join before kicking off the next one. This is a result of the
	    	// hardcoded 100 writes to bq limit that we can't resolve right now, so we have to
	    	// artificially slow this down. 
	    	dlPipeThread.join();
	    
	    	//=== get downloads for timePeriod (many pipelines)
	    	// set up big query IO options
	    	HistoricPipelineOptions optionsUl = options.cloneAs(HistoricPipelineOptions.class);
	    	optionsUl.setAppName("HistoricPipeline-Upload");
	    
	    	ExtractHistoricRowsPipeline ehrPUL = new ExtractHistoricRowsPipeline(optionsUl);
	    	Thread ulPipeThread = new Thread(ehrPUL);
	    	String uploadsConfigFile = getRunnerConfigFilename(timePeriod, "uploads");
	    	LOG.info("Uploads configuration: " + uploadsConfigFile);
	    	ehrPUL.setConfigurationFile(uploadsConfigFile)
	    		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
	    		.setWriteDisposition(WriteDisposition.WRITE_APPEND);
	    	
	    	ulPipeThread.start();
		    ulPipeThread.join();
		    
		    next = ehrPUL.getState() == State.DONE && ehrPDL.getState() == State.DONE;
	    }
	    
	    // when both pipelines are done, proceed to merge, add ISP and local time information.
	    if (next) {
	    
			// ==== get tables merged (a pipeline for merge + ISPs)
			JSONObject downloadsConfig = getRunnerConfig(timePeriod, "downloads");
			JSONObject uploadsConfig = getRunnerConfig(timePeriod, "uploads");

			// set up big query IO options
			HistoricPipelineOptions optionsMergeAndISP = options.cloneAs(HistoricPipelineOptions.class);
			optionsMergeAndISP.setAppName("HistoricPipeline-MergeAndISP");
			Pipeline pipe = Pipeline.create(optionsMergeAndISP);
			MergeUploadDownloadPipeline mudP = new MergeUploadDownloadPipeline(pipe);

			mudP.setDownloadTable((String) downloadsConfig.get("outputTable"))
					.setUploadTable((String) uploadsConfig.get("outputTable"))
					.setOutputTable((String) downloadsConfig.get("mergeTable"))
					.setWriteDisposition(
							com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
					.setCreateDisposition(
							com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

			PCollection<TableRow> mergedRows = mudP.apply();

			// ==== add ISPs
			AddISPsPipeline addISPs = new AddISPsPipeline(pipe);
			addISPs.setWriteData(false)
					.setOutputTable((String) downloadsConfig.get("withISPTable"))
					.setInputTable((String) downloadsConfig.get("mergeTable"))
					.setOutputSchema(Schema.fromJSONFile((String) downloadsConfig.get("withISPTableSchema")))
					.setWriteDisposition(
							com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.setCreateDisposition(
							com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

			PCollection<TableRow> ispdRows = addISPs.apply(mergedRows);

			// ==== add local time
			AddLocalTimePipeline addLocalTime = new AddLocalTimePipeline(pipe);
			addLocalTime.setWriteData(false)
					.setOutputSchema(Schema.fromJSONFile((String) downloadsConfig.get("withISPTableSchema")))
					.setOutputTable((String) downloadsConfig.get("withISPTable"))
					.setWriteDisposition(
							com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
					.setCreateDisposition(
							com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

			PCollection<TableRow> timedRows = addLocalTime.apply(ispdRows);
			
			// ==== add location names
			AddLocationPipeline addLocations = new AddLocationPipeline(pipe);
			addLocations
				.setWriteData(true)
				.setOutputSchema(Schema.fromJSONFile((String) downloadsConfig.get("withISPTableSchema")))
				.setOutputTable((String) downloadsConfig.get("withISPTable"))
				.setWriteDisposition(
						com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.setCreateDisposition(
						com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
			
			addLocations.apply(timedRows);
			
			DataflowPipelineJob resultsMergeAndISPs = (DataflowPipelineJob) pipe.run();
			resultsMergeAndISPs.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));
			LOG.info("Merge + ISPs job completed, with status: " + resultsMergeAndISPs.getState().toString());
	    } else {
	    	LOG.error("Download or Upload pipelines failed.");
	    }
	}
}
