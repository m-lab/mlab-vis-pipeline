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
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.bocoup.pipelineopts.HistoricPipelineOptions;
import mlab.bocoup.util.BigQueryIOHelpers;
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
	 * --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner 
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

	    boolean next = true;
	    if (skipNDTRead != 1) {
			// === get downloads for timePeriod
	    	options.setAppName("HistoricPipeline-Download");
	    	
	    	ExtractHistoricRowsPipeline ehrPDL = new ExtractHistoricRowsPipeline(options);
	    	Thread dlPipeThread = new Thread(ehrPDL);
	    	String downloadsConfigFile = getRunnerConfigFilename(timePeriod, "downloads");
	    	
	    	LOG.info("Downloads configuration: " + downloadsConfigFile);
	    	ehrPDL.setConfigurationFile(downloadsConfigFile)
	    		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
	    		.setWriteDisposition(WriteDisposition.WRITE_APPEND)
	    		.shouldExecute(true);
	    	
	    	//=== get uploads for timePeriod
	    	// set up big query IO options (it doesn't seem to let us share the download ones)
	    	HistoricPipelineOptions optionsUl = options.cloneAs(HistoricPipelineOptions.class);
	    	optionsUl.setAppName("HistoricPipeline-Upload");
	    
	    	ExtractHistoricRowsPipeline ehrPUL = new ExtractHistoricRowsPipeline(optionsUl);
	    	Thread ulPipeThread = new Thread(ehrPUL);
	    	String uploadsConfigFile = getRunnerConfigFilename(timePeriod, "uploads");
	    
	    	LOG.info("Uploads configuration: " + uploadsConfigFile);
	    	ehrPUL.setConfigurationFile(uploadsConfigFile)
	    		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
	    		.setWriteDisposition(WriteDisposition.WRITE_APPEND)
	    		.shouldExecute(true);
	     	
	    	// start the two threads 
	    	LOG.info("Starting upload/download threads");
	    	dlPipeThread.start();
	    	ulPipeThread.start();
	    	
	    	// wait for the two threads to finish
	    	LOG.info("Joining upload/download threads");
	    	dlPipeThread.join();
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
			
			// ==== merge upload and download into a single set of rows (outputs a table and also gives the rows back)
			MergeUploadDownloadPipeline mudP = new MergeUploadDownloadPipeline(pipe);

			mudP.setDownloadTable((String) downloadsConfig.get("outputTable"))
					.setUploadTable((String) uploadsConfig.get("outputTable"))
					.setOutputTable((String) downloadsConfig.get("mergeTable"))
					.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
					.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

			PCollection<TableRow> mergedRows = mudP.apply();

			// ==== add ISPs
			PCollection<TableRow> ispdRows = new AddISPsPipeline(pipe).apply(mergedRows);

			// ==== add server locations and mlab site info
			PCollection<TableRow> infodRows = new AddMlabSitesInfoPipeline(pipe).apply(ispdRows);
			
			// ==== merge ASNs
			PCollection<TableRow> mergedAsnsRows = new MergeASNsPipeline(pipe).apply(infodRows);
			
			// ==== add local time
			PCollection<TableRow> timedRows = new AddLocalTimePipeline(pipe).apply(mergedAsnsRows);
			
			// ==== add location names
			PCollection<TableRow> locationNamedRows = new AddLocationPipeline(pipe).apply(timedRows);
			
			// write to the final table
			BigQueryIOHelpers.writeTable(locationNamedRows, (String) downloadsConfig.get("withISPTable"), 
					Schema.fromJSONFile((String) downloadsConfig.get("withISPTableSchema")),
					WriteDisposition.WRITE_TRUNCATE, CreateDisposition.CREATE_IF_NEEDED);
			
			// kick off the pipeline
			DataflowPipelineJob resultsMergeAndISPs = (DataflowPipelineJob) pipe.run();
			
			// wait for the pipeline to finish executing
			resultsMergeAndISPs.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));
			
			LOG.info("Merge + ISPs job completed, with status: " + resultsMergeAndISPs.getState().toString());
	    } else {
	    	LOG.error("Download or Upload pipelines failed.");
	    }
	}
}
