package mlab.bocoup;

import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.bocoup.pipelineopts.ExtractUpdateRowsPipelineOptions;
import mlab.bocoup.query.BigQueryJob;
import mlab.bocoup.util.Formatters;
import mlab.bocoup.util.QueryPipeIterator;
import mlab.bocoup.util.Schema;

public class ExtractUpdateRowsPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(ExtractUpdateRowsPipeline.class);
	
	private static String DOWNLOADS_BY_DAY = "./data/bigquery/batch-runs/daily/downloads_ip_by_day_base.json";
	private static String DOWNLOADS_BY_HOUR = "./data/bigquery/batch-runs/daily/downloads_ip_by_hour_base.json";
	private static String UPLOADS_BY_DAY = "./data/bigquery/batch-runs/daily/uploads_ip_by_day_base.json";
	private static String UPLOADS_BY_HOUR = "./data/bigquery/batch-runs/daily/uploads_ip_by_hour_base.json";
	
	private static SimpleDateFormat sdfIn = new SimpleDateFormat(Formatters.TIMESTAMP2);
	private static SimpleDateFormat sdfOut = new SimpleDateFormat(Formatters.TIMESTAMP);
	private static Calendar cal = Calendar.getInstance();

	private String projectId;
	private String timePeriod;
	private JSONObject downloadsConfig;
	private JSONObject uploadsConfig;
	private Pipeline p;

	private WriteDisposition writeDisposition = WriteDisposition.WRITE_TRUNCATE;
	private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;
	
	public ExtractUpdateRowsPipeline(Pipeline p) {
		this.p = p;
	}
	
	/**
	 * Set project id, for querying purposes.
	 * @param projectId
	 * @return 
	 */
	public ExtractUpdateRowsPipeline setProjectId(String projectId) {
		this.projectId = projectId;
		return this;
	}
	
	/**
	 * Get project id
	 * @return
	 */
	public String getProjectId() {
		return this.projectId;
	}
	
	/**
	 * set time period, should either be "day" or "hour"
	 * @param timePeriod
	 * @return
	 * @throws Exception 
	 */
	public ExtractUpdateRowsPipeline setTimePeriod(String timePeriod) throws Exception {
		this.timePeriod = timePeriod;
		if (!this.timePeriod.equals("day") && !this.timePeriod.equals("hour")) {
			throw new Exception("Time period should be either 'day' or 'hour'");
		}
		return this;
	}
	
	/**
	 * Returns time period
	 * @return
	 */
	public String getTimePeriod() {
		return this.timePeriod;
	}
	
	/**
	 * Returns the configuration file for download table
	 * @return 
	 */
	public JSONObject getDownloadsConfig() {
		return this.downloadsConfig;
	}
	
	/**
	 * Returns the upload configuration file
	 * @return
	 */
	public JSONObject getUploadsConfig() {
		return this.uploadsConfig;
	}
	
	public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
		return writeDisposition;
	}

	public ExtractUpdateRowsPipeline setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
		return createDisposition;
	}

	public ExtractUpdateRowsPipeline setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
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
	private JSONObject setupRunnerOptions(String type) throws Exception {
		JSONParser jp = new JSONParser();
		Object runnerConfigObj;
		
		if (type.equals("downloads")) {
			if (this.timePeriod.equals("hour")) {
				runnerConfigObj = jp.parse(new FileReader(DOWNLOADS_BY_HOUR));
			} else {
				runnerConfigObj = jp.parse(new FileReader(DOWNLOADS_BY_DAY));
			}
		} else if (type.equals("uploads")) {
			if (this.timePeriod.equals("hour")) {
				runnerConfigObj = jp.parse(new FileReader(UPLOADS_BY_HOUR));
			} else {
				runnerConfigObj = jp.parse(new FileReader(UPLOADS_BY_DAY));
			} 
		} else {
			throw new Exception("Type must be set to 'uploads' or 'downloads'");
		}
		
		return (JSONObject) runnerConfigObj;
		
	}
	
	/**
	 * @private
	 * Gets the last date from a table.
	 * @param config  the configuration run object specifying which table to query
	 * @param cmdOpts  runtime parameters specifying the project we're working with.
	 * @return
	 * @throws IOException if the query cannot be executed.
	 */
	private String getLastDate(JSONObject config, String projectId) throws IOException {
		String maxDateQuery = "select STRING(max(test_date)) as max_date from " + 
				(String) config.get("lastDateFrom");
		
		BigQueryJob bqj = new BigQueryJob(projectId);
		java.util.List<TableRow> rows = bqj.executeQuery(maxDateQuery);
		
		String timestamp = null;
		for (TableRow row : rows) {
			for (TableCell field : row.getF()) {
				timestamp = (String) field.getV();
		      }
		}
		return timestamp;
	}
	
	/**
	 * @private
	 * Gets the date range for the daily update
	 * @param cmdOpts
	 * @param downloadsConfig
	 * @return
	 * @throws IOException
	 * @throws java.text.ParseException
	 */
	private String[] getDateRange(JSONObject downloadsConfig) 
			throws IOException, java.text.ParseException {
		
		String[] dates = new String[3];
		String startDateStr = getLastDate(downloadsConfig, this.projectId);
		Date startDate = sdfIn.parse(startDateStr);
		
		Date today = new Date();
		cal.setTime(today);
		cal.add(Calendar.DATE, -2);
		Date endDate = cal.getTime();
		endDate = DateUtils.truncate(endDate, Calendar.DATE);
		
		dates[0] = sdfOut.format(startDate);
		dates[1] = sdfOut.format(endDate);
		//# of days between two dates
		dates[2] = String.valueOf(ChronoUnit.DAYS.between(startDate.toInstant(),endDate.toInstant())); 
				
		return dates;
	}
	
	/**
	 * Creates the update tables from the last date in the final table
	 * to 2 days before current date.
	 * @param pipe Pipeline
	 * @param dates Date range
	 * @param config  the downloads or uploads config
	 * @return
	 * @throws NumberFormatException
	 * @throws java.text.ParseException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private PCollection<TableRow> makeUpdateTables(Pipeline pipe, JSONObject config) throws NumberFormatException, java.text.ParseException, IOException, InterruptedException {
		Object[] params = {};
		String[] dates = getDateRange(config);
	    
		String queryFile = (String) config.get("queryFile");
	    String outputTable = (String) config.get("outputTable");
	    TableSchema tableSchema = Schema.fromJSONFile((String) config.get("schemaFile"));
	    
		LOG.info(">>> Kicking off pipeline for dates: " + dates[0] + " - " + dates[1]);
		LOG.info("Setup - Query file: " + queryFile);
		LOG.info("Setup - Table Schema: " + tableSchema.toPrettyString());
		LOG.info("Setup - Output table: " + outputTable);
		LOG.info("Setup - Start date: " + dates[0]);
		LOG.info("Setup - End date: " + dates[1]);
		LOG.info("Setup - Incrementing by: " + dates[2]);
		
		// Read the latest NDT records for the dates
	    QueryPipeIterator qpi = new QueryPipeIterator(pipe, queryFile, 
	    		params, dates[0], dates[1], Integer.parseInt(dates[2])+1);
	    
	    PCollection<TableRow> rows = qpi.next();
		rows.apply(BigQueryIO.Write
				 .named("write table " + outputTable)
				 .to(outputTable)
		         .withSchema(tableSchema)
		         .withCreateDisposition(this.createDisposition)
		         .withWriteDisposition(this.writeDisposition));
		return rows;
	}
	
	/**
	 * Prepares the daily pipeline to extract the update
	 * @throws Exception
	 */
	public void apply() throws Exception {
		// read download and upload tables for the time period selected.
	    this.downloadsConfig = setupRunnerOptions("downloads");
	    this.uploadsConfig = setupRunnerOptions("uploads");
	    
	    makeUpdateTables(this.p, this.downloadsConfig);
	    makeUpdateTables(this.p, this.uploadsConfig);
	}
	
	/**
	 * Main run, to get update tables for a specific date range.
	 * To execute run with parameters:
	 * 
	 * --runner com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner 
	 * --numWorkers=2 --timePeriod="day" -project=mlab-oti --stagingLocation="gs://bocoup"
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	
		PipelineOptionsFactory.register(ExtractUpdateRowsPipelineOptions.class);
		ExtractUpdateRowsPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(ExtractUpdateRowsPipelineOptions.class);
		
		String timePeriod = options.getTimePeriod();
		String projectId = options.getProject();
	
	    options.setAppName("ExtractUpdateRowsPipeline");
	    
	    // ====  make daily update tables
	    Pipeline pipe = Pipeline.create(options);
	    ExtractUpdateRowsPipeline dup = new ExtractUpdateRowsPipeline(pipe);
	    dup.setProjectId(projectId)
	    	.setTimePeriod(timePeriod);
	    
	    dup.apply();
	    
	    DataflowPipelineJob resultUpdate = (DataflowPipelineJob) pipe.run();
	    resultUpdate.waitToFinish(-1, TimeUnit.MINUTES, 
	    		new MonitoringUtil.PrintHandler(System.out));
		LOG.info("Update table job completed, with status: " + 
	    		resultUpdate.getState().toString());
	}

}
