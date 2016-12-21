package mlab.bocoup;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.bocoup.pipelineopts.ExtractHistoricRowsPipelineOptions;
import mlab.bocoup.query.BigQueryJob;
import mlab.bocoup.query.QueryBuilder;
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
	private Pipeline pipeline;
	private boolean runPipeline = false;

	private String[] dates;

	public ExtractHistoricRowsPipeline(Pipeline p, BigQueryOptions options) {
		this.pipeline = p;
		this.options = options;
	}

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

	public ExtractHistoricRowsPipeline shouldExecute(boolean execute) {
		this.runPipeline  = execute;
		return this;
	}

	public boolean isExecutable() {
		return this.runPipeline;
	}

	public ExtractHistoricRowsPipeline setDates(String [] dates) {
		this.dates = dates;
		return this;
	}

	public String[] getDatesAuto(JSONObject config) throws IOException {
		String dateRangeQuery = "SELECT STRING(max(web100_log_entry.log_time)) as max_test_date, " +
				"STRING(min(web100_log_entry.log_time)) as min_test_date " +
				"FROM " + (String) config.get("lastDateFromTable");

		BigQueryJob bqj = new BigQueryJob((String) config.get("projectId"));
		java.util.List<TableRow> rows = bqj.executeQuery(dateRangeQuery);

		String [] timestamps = new String[2];
		int i = 0;
		for (TableRow row : rows) {
			for (TableCell field : row.getF()) {
				timestamps[i++] = (String) field.getV();
		      }
		}
		return timestamps;
	}

	public String[] getDatesFromConfig() {
		JSONArray dates = (JSONArray) this.configurationObject.get("dates");
		Object [] dateRanges;
		dateRanges = dates.toArray();

		String [] dateRangesStr = new String[2];
		dateRangesStr[0] = (String) dateRanges[0];
		dateRangesStr[1] = (String) dateRanges[1];

		return dateRangesStr;
	}

	/**
	* Runs one main pipeline read and write given that we have a table that
	* does not have allowLargeResutls set to false.
	*
	* Specification in that scenario do not require a "dates" array or a "numberOfDays"
	* by which to batch things up.
	*
	* Example:
	* <code>
	* {
	*     "queryFile": "./data/queries/base_downloads_ip_by_day.sql",
	*     "schemaFile": "./data/schemas/base_downloads_ip.json",
	*     "outputTable": "bocoup.base_downloads_ip_by_day"
	* }
	* </code>
	*/
	public void run() {
		String queryFile = (String) this.configurationObject.get("queryFile");
		TableSchema tableSchema = Schema.fromJSONFile((String) this.configurationObject.get("schemaFile"));
		String outputTableName =  (String) this.configurationObject.get("outputTable");
		String [] dates = this.dates;

		QueryBuilder qb;

		try {

			// if no dates are specified in the config file, auto detect the range we have
			// and work with that. Otherwise, use that dates array. Note that it should have two
			// values which will be the full range of our data.


			if (dates == null) {
				LOG.info(">>> Dates not provided on command line. Looking for them in config");
				dates = this.getDatesFromConfig();

			} else {
				LOG.info(">>> Running with dates from the command line.");
			}

			if (dates == null) {
				LOG.info(">>> Dates not in config. Attempting to generate automatically.");
				dates = getDatesAuto(this.configurationObject);

			}


			LOG.info(">>> Kicking off pipeline for dates: " + dates[0] + " " + dates[1]);
			LOG.info("Setup - Query file: " + queryFile);
			qb = new QueryBuilder(queryFile, dates);
			try {
				LOG.info("Setup - Table Schema: " + tableSchema.toPrettyString());
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
			LOG.info("Setup - Output table: " + outputTableName);

			if (this.pipeline == null) {
				LOG.info("CREATING PIPELINE");
				this.pipeline = Pipeline.create(this.options);
			}

			PCollection<TableRow> rows = this.pipeline.apply(
					BigQueryIO.Read
					.named("Running query " + queryFile)
					.fromQuery(qb.getQuery()));

			rows.apply(BigQueryIO.Write
					.named("write table for " + queryFile)
					.to(outputTableName)
					.withSchema(tableSchema)
					.withCreateDisposition(this.createDisposition)
					.withWriteDisposition(this.writeDisposition));

			// by default, the pipeline will not run.
			if (this.isExecutable()) {
				DataflowPipelineJob result = (DataflowPipelineJob) this.pipeline.run();
				result.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));
				this.setState(result.getState());
				LOG.info("Job completed, with status: " + result.getState().toString());
			} else {
				LOG.info("will not run");
			}

		} catch (IOException |  InterruptedException e) {
			LOG.error(e.getMessage());
		}
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
	public void runBatched() {

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

				rows.apply(BigQueryIO.Write
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
	 * --runner=com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
	 * --configfile="./data/bigquery/batch-runs/historic/uploads_ip_by_day_base.json"
	 * --project=mlab-staging
	 * --stagingLocation="gs://mlab-data-viz"
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
	    	.setWriteDisposition(WriteDisposition.WRITE_APPEND)
	    	.shouldExecute(true);

	    ehrP.run();

	}

}
