package mlab.bocoup;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import mlab.bocoup.pipelineopts.ExtractHistoricRowsPipelineOptions;
import mlab.bocoup.pipelineopts.HistoricPipelineOptions;
import mlab.bocoup.query.BigQueryIONoLargeResults;
import mlab.bocoup.query.BigQueryIONoLargeResults.Write.CreateDisposition;
import mlab.bocoup.query.BigQueryIONoLargeResults.Write.WriteDisposition;
import mlab.bocoup.util.QueryPipeIterator;
import mlab.bocoup.util.Schema;

public class ExtractHistoricRowsPipeline implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ExtractHistoricRowsPipeline.class);
	private BigQueryOptions options;
	private String configFile;
	private JSONObject configurationObject;
	
	
	private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;
	private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;
	private State state;
	
	public ExtractHistoricRowsPipeline(BigQueryOptions options) {
		this.options = options;
	}
	
	public ExtractHistoricRowsPipeline setConfigurationFile(String configFile) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
		this.configFile = configFile;		
		JSONParser jp = new JSONParser();
		Object obj = jp.parse(new FileReader(configFile));
		this.configurationObject = (JSONObject) obj;

		return this;
	}
	
	public String getConfigurationFile() {
		return this.configFile;
	}
	
	public WriteDisposition getWriteDisposition() {
		return this.writeDisposition;
	}

	public ExtractHistoricRowsPipeline setWriteDisposition(WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	public CreateDisposition getCreateDisposition() {
		return this.createDisposition;
	}

	public ExtractHistoricRowsPipeline setCreateDisposition(CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}
	 
	public State getState() {
		return this.state;
	}
	
	public void setState(State s) {
		this.state = s;
	}
	/**
	 * Runs a series of pipelines, each capturing a subset of ndt rows.
	 * Specifications should include:
	 *  {
	 *     "queryFile": "./data/queries/base_downloads_ip_by_day.sql",
  	 *     "schemaFile": "./data/schemas/base_downloads_ip.json",
     *     "outputTable": "bocoup.base_downloads_ip_by_day",
     *     "numberOfDays": 7,
     *     "dates": [
     *       "2009-01-15T00:00:00Z",
     *       "2010-01-01T00:00:00Z",
     *       "2011-01-01T00:00:00Z",
     *       "2012-01-01T00:00:00Z",
     *       "2013-01-01T00:00:00Z"
     *      ]
     * }
	 */
	public void run() {
		
		String queryFile = (String) this.configurationObject.get("queryFile");
		TableSchema tableSchema = Schema.fromJSONFile((String) this.configurationObject.get("schemaFile"));
		String outputTableName =  (String) this.configurationObject.get("outputTable");
		long numberOfDays =  (long) this.configurationObject.get("numberOfDays");
		JSONArray dates = (JSONArray) this.configurationObject.get("dates");
		
		int pos = 0;
		
		while (dates.size() > (pos + 1)) {
			String startDate = (String) dates.get(pos);
			String endDate = (String) dates.get(pos+1);
			
			LOG.debug(">>> Kicking off pipeline for dates: " + startDate + " - " + endDate);
			LOG.debug("Setup - Query file: " + queryFile);
			try {
				LOG.debug("Setup - Table Schema: " + tableSchema.toPrettyString());
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
			LOG.debug("Setup - Output table: " + outputTableName);
			LOG.debug("Setup - Start date: " + startDate);
			LOG.debug("Setup - End date: " + endDate);
			LOG.debug("Setup - Incrementing by: " + numberOfDays);
			
			Pipeline pipe = Pipeline.create(this.options);
			
			Object[] params = { };
			
			QueryPipeIterator qpi = null;
			try {
				qpi = new QueryPipeIterator(pipe, queryFile, params, 
						startDate, endDate, (int) numberOfDays);
			} catch (ParseException e) {
				LOG.error(e.getMessage());
			}
			
			int counter = 0;
			while (qpi.hasNext()) {
				LOG.debug("Queueing up run " + counter++);
				
				PCollection<TableRow> rows = qpi.next();
				
				rows.apply(BigQueryIONoLargeResults.Write
						 .named("write table " + counter)
						 .to(outputTableName)
				         .withSchema(tableSchema)
				         .withCreateDisposition(this.createDisposition)
				         .withWriteDisposition(this.writeDisposition));
			}
			
			DataflowPipelineJob result = (DataflowPipelineJob) pipe.run();
			try {
				result.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));
				this.setState(result.getState());
			} catch (IOException | InterruptedException e) {
				LOG.error(e.getMessage());
			}
			LOG.info("Job completed, with status: " + result.getState().toString());
			pos++;
		}
	}
	/**
	 * Run this as a regular Java application with the following arguments:
	 * --runner com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner 
	 * --numWorkers=5 
	 * --configfile="./data/batch-runs/historic/uploads_ip_by_day_base.json" 
	 * --project=mlab-oti 
	 * --stagingLocation="gs://bocoup"
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 * @throws org.json.simple.parser.ParseException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, ParseException, InterruptedException, org.json.simple.parser.ParseException, ClassNotFoundException {
		
		PipelineOptionsFactory.register(ExtractHistoricRowsPipelineOptions.class);
		ExtractHistoricRowsPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(ExtractHistoricRowsPipelineOptions.class);
		
	    options.setAppName("ExtractHistoricRowsPipeline");
		
	    ExtractHistoricRowsPipeline ehrP = new ExtractHistoricRowsPipeline(options);
	    ehrP.setConfigurationFile(options.getConfigfile())
	    	.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
	    	.setWriteDisposition(WriteDisposition.WRITE_APPEND);
	    
	    ehrP.run();
		
	}

}
