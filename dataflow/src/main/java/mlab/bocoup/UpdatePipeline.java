package mlab.bocoup;

import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.bocoup.pipelineopts.UpdatePipelineOptions;
import mlab.bocoup.util.Schema;

public class UpdatePipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(UpdatePipeline.class);
	
	/**
	 * To run, pass --timePeriod as either "day" or "hour".
	 */
	public static void main(String[] args) throws Exception {
		
		PipelineOptionsFactory.register(UpdatePipelineOptions.class);
		UpdatePipelineOptions options = PipelineOptionsFactory.fromArgs(args)
					.withValidation()
					.as(UpdatePipelineOptions.class);
		
	    String timePeriod = options.getTimePeriod();
	    String projectId = options.getProject();
	    
	    // Pipeline 1:
	    // ====  make daily update tables
	    UpdatePipelineOptions extractNewRowsOptions = options.cloneAs(UpdatePipelineOptions.class);
	    extractNewRowsOptions.setAppName("UpdatePipeline-ExtractRows");
	    Pipeline pipe = Pipeline.create(extractNewRowsOptions);
	    ExtractUpdateRowsPipeline dup = new ExtractUpdateRowsPipeline(pipe);
	    dup.setProjectId(projectId)
	    	.setTimePeriod(timePeriod)
	    	.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
	    	.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    
	    dup.apply();
	    
	    DataflowPipelineJob resultUpdate = (DataflowPipelineJob) pipe.run();
	    resultUpdate.waitToFinish(-1, TimeUnit.MINUTES, 
	    		new MonitoringUtil.PrintHandler(System.out));
		LOG.info("Update table job completed, with status: " + 
	    		resultUpdate.getState().toString());
		
		// Pipeline 2:
		// ===== merge the tables
		JSONObject downloadsConfig = dup.getDownloadsConfig();
		JSONObject uploadsConfig = dup.getUploadsConfig();
		
		UpdatePipelineOptions optionsMergeAndISP = options.cloneAs(UpdatePipelineOptions.class);
	    optionsMergeAndISP.setAppName("UpdatePipeline-MergeAndISP");
		Pipeline pipe2 = Pipeline.create(optionsMergeAndISP);
		
		MergeUploadDownloadPipeline mudP = new MergeUploadDownloadPipeline(pipe2);
		mudP.setDownloadTable((String) downloadsConfig.get("outputTable"))
			.setUploadTable((String) uploadsConfig.get("outputTable"))
			.setOutputTable((String) downloadsConfig.get("mergeTable"))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
		
		PCollection<TableRow> mergedRows = mudP.apply();
		
		// ==== add ISPs
		AddISPsPipeline addISPs = new AddISPsPipeline(pipe2);
		addISPs.setWriteData(false)
			.setOutputTable((String) downloadsConfig.get("withISPTable"))
			.setInputTable((String) downloadsConfig.get("mergeTable"))
			.setOutputSchema(Schema.fromJSONFile(
					(String) downloadsConfig.get("withISPTableSchema")))
			.setWriteDisposition(WriteDisposition.WRITE_APPEND)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
		
		PCollection<TableRow> ispdRows = addISPs.apply(mergedRows);
		
		// ==== add local time
		AddLocalTimePipeline addLocalTime = new AddLocalTimePipeline(pipe2);
		addLocalTime.setWriteData(true)
				.setOutputSchema(Schema.fromJSONFile((String) downloadsConfig.get("withISPTableSchema")))
				.setOutputTable((String) downloadsConfig.get("withISPTable"))
				.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
				.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

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
		
		DataflowPipelineJob resultsMergeAndISPs = (DataflowPipelineJob) pipe2.run();
		resultsMergeAndISPs.waitToFinish(-1, TimeUnit.MINUTES, 
				new MonitoringUtil.PrintHandler(System.out));
		LOG.info("Merge + ISPs job completed, with status: " + 
				resultsMergeAndISPs.getState().toString());
	}
}
