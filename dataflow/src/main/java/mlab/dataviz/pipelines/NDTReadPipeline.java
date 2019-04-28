package mlab.dataviz.pipelines;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQueryException;

import mlab.dataviz.entities.BQPipelineRun;
import mlab.dataviz.entities.BQPipelineRunDatastore;
import mlab.dataviz.query.BigQueryJob;
import mlab.dataviz.query.QueryBuilder;
import mlab.dataviz.util.Formatters;
import mlab.dataviz.util.PipelineConfig;
import mlab.dataviz.util.Schema;

public class NDTReadPipeline implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(NDTReadPipeline.class);
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP2);

	private BigQueryOptions options;
	private PipelineConfig config;

	private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;
	private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;
	private State state;
	private Pipeline pipeline;
	private boolean runPipeline = false;
	private BQPipelineRun pipelineRunRecord = null;
	private String[] dates;

	/**
	 * Create a new NDT read pipeline. Pass a pipeline to share, options to share
	 * and a status to track.
	 * 
	 * @param p
	 * @param options
	 * @param status
	 */
	public NDTReadPipeline(Pipeline p, BigQueryOptions options, BQPipelineRun status) {
		this.pipeline = p;
		this.options = options;
		this.pipelineRunRecord = status;
	}

	/**
	 * Create a new NDT read pipeline. A new pipeline will be created, from shared
	 * options and shared status.
	 * 
	 * @param options
	 * @param status
	 */
	public NDTReadPipeline(BigQueryOptions options, BQPipelineRun status) {
		this.options = options;
		this.pipelineRunRecord = status;
	}

	/**
	 * Set the configuration for this pipeline.
	 * 
	 * @param config
	 * @return
	 */
	public NDTReadPipeline setPipelineConfiguration(PipelineConfig config) {
		this.config = config;
		return this;
	}

	/**
	 * Returns the pipeline configuration object.
	 * 
	 * @return
	 */
	public PipelineConfig getPipelineConfig() {
		return this.config;
	}

	/**
	 * Returns the write disposition for this pipeline.
	 * 
	 * @return
	 */
	public WriteDisposition getWriteDisposition() {
		return this.writeDisposition;
	}

	/**
	 * Sets the write disposition of this pipeline. (truncate/append etc)
	 * 
	 * @param writeDisposition
	 * @return
	 */
	public NDTReadPipeline setWriteDisposition(WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	/**
	 * Gets the create disposition
	 * 
	 * @return
	 */
	public CreateDisposition getCreateDisposition() {
		return this.createDisposition;
	}

	/**
	 * Gets the create disposition
	 * 
	 * @param createDisposition
	 * @return
	 */
	public NDTReadPipeline setCreateDisposition(CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}

	/**
	 * Gets state - running or not
	 * 
	 * @return State
	 */
	public State getState() {
		return this.state;
	}

	/**
	 * Sets the state - running or not.
	 * 
	 * @param s
	 */
	public void setState(State s) {
		this.state = s;
	}

	/**
	 * Indicates whether a pipeline should execute or not
	 * 
	 * @param execute boolean
	 * @return
	 */
	public NDTReadPipeline shouldExecute(boolean execute) {
		this.runPipeline = execute;
		return this;
	}

	/**
	 * Returns whether a pipeline is executable
	 * 
	 * @return
	 */
	public boolean isExecutable() {
		return this.runPipeline;
	}

	/**
	 * Sets the date range for the queries.
	 * 
	 * @param dates
	 * @return
	 */
	public NDTReadPipeline setDates(String[] dates) {
		this.dates = dates;
		return this;
	}

	/**
	 * Gets a date range based on the last test recorded in our ndt write tables.
	 *
	 * The start date is the max test date that we have parsed in our table. The end
	 * date is the max test date that is in the NDT table.
	 *
	 * @todo change the test_date to parse_date when that data gets there.
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public synchronized String[] getDatesFromBQ() throws IOException, InterruptedException, BigQueryException {

		// read date from our output table as A
		// read max date from NDT as B
		// if empty
		// read min date from NDT as C
		// return {C, B}
		// else
		// return {A, B}

		// read from one of our tables
		String startDateRangeQuery = "select STRFTIME_UTC_USEC(max(test_date), \"%F %X\") as max_test_date from "
				+ this.config.getOutputTable();

		// read from NDT
		String endDateRangeQuery = "SELECT FORMAT_TIMESTAMP(\"%F %X\", TIMESTAMP_TRUNC(TIMESTAMP_MICROS(max(web100_log_entry.log_time) * 1000000), SECOND, 'UTC'))"
				+ " as max_test_date FROM " + this.config.getNDTTable() + ";";

		BigQueryJob bqj = new BigQueryJob();

		String[] timestamps = new String[2];
		boolean seekNDT = false;
		try {
			String startDate = bqj.executeQueryForValue(startDateRangeQuery);
			if (startDate.length() == 0) {
				seekNDT = true;
			} else {
				timestamps[0] = startDate;
			}
		} catch (GoogleJsonResponseException | ClassCastException | InterruptedException | BigQueryException e) {
			// our table doesn't exist yet.
			seekNDT = true;
		}

		if (seekNDT) {
			startDateRangeQuery = "SELECT FORMAT_TIMESTAMP(\"%F %X\", TIMESTAMP_TRUNC(TIMESTAMP_MICROS(min(web100_log_entry.log_time) * 1000000), SECOND, 'UTC'))"
					+ " as min_test_date FROM " + this.config.getNDTTable() + ";";
			timestamps[0] = bqj.executeQueryForValue(startDateRangeQuery, false);
		}

		timestamps[1] = bqj.executeQueryForValue(endDateRangeQuery, false);

		LOG.info("NDT pipeline date range comptued as: " + timestamps[0] + "-" + timestamps[1]);
		return timestamps;
	}

	/**
	 * Computes the full NDT date range. To be used when truncating the tables and
	 * starting over.
	 * 
	 * @return dates String array of range.
	 */
	private synchronized String[] getFullDateRange()
			throws IOException, SQLException, GeneralSecurityException, InterruptedException, BigQueryException {

		String[] timestamps = new String[2];

		// start date from NDT
		String startDateRangeQuery = "SELECT FORMAT_TIMESTAMP(\"%F %X\", TIMESTAMP_TRUNC(TIMESTAMP_MICROS(min(web100_log_entry.log_time) * 1000000), SECOND, 'UTC'))"
				+ " as min_test_date FROM " + this.config.getNDTTable() + ";";

		// end dates from NDT
		String endDateRangeQuery = "SELECT FORMAT_TIMESTAMP(\"%F %X\", TIMESTAMP_TRUNC(TIMESTAMP_MICROS(max(web100_log_entry.log_time) * 1000000), SECOND, 'UTC'))"
				+ " as max_test_date FROM " + this.config.getNDTTable() + ";";

		BigQueryJob bqj = new BigQueryJob();
		timestamps[0] = bqj.executeQueryForValue(startDateRangeQuery, false);
		timestamps[1] = bqj.executeQueryForValue(endDateRangeQuery, false);

		this.pipelineRunRecord.setDataStartDate(timestamps[0]);
		this.pipelineRunRecord.setDataEndDate(timestamps[1]);

		LOG.info("Full NDT table refresh. Dates: " + timestamps[0] + "-" + timestamps[1]);

		return timestamps;
	}

	/**
	 * @private Determines the run dates for the pipeline. The order is as follows:
	 *          - command-line dates take precedence - then last run in datastore.
	 *          Use the end date there, alongside the last date in NDT. - auto
	 *          detect. Start date is our last test, end date is last test in NDT.
	 *
	 * @throws IOException
	 * @throws SQLException
	 * @throws GeneralSecurityException
	 * @throws InterruptedException
	 */
	private synchronized String[] determineRunDates()
			throws IOException, SQLException, GeneralSecurityException, InterruptedException, BigQueryException {

		// if we already have the dates from a previous run, use them.
		// since this pipeline is used twice, once for daily and one for hourly
		// we expect this to be set the first time and then be used.
		if (!this.pipelineRunRecord.getDataStartDate().equals("")
				&& !this.pipelineRunRecord.getDataEndDate().equals("")) {
			this.dates = new String[] { this.pipelineRunRecord.getDataStartDate(),
					this.pipelineRunRecord.getDataEndDate() };
			LOG.info("Using existing dates from status: " + this.pipelineRunRecord.toString());
			return dates;
			// Pipeline run dates provided via the command-line. Use those.
		} else if (dates != null & !dates[0].equals("") && !dates[1].equals("")) {
			LOG.info(">>> Dates passed via commandline.");

			this.pipelineRunRecord.setDataStartDate(dates[0]);
			this.pipelineRunRecord.setDataEndDate(dates[1]);
			this.pipelineRunRecord.save();
			LOG.info("Using dates from commandline " + this.pipelineRunRecord.toString());
			return dates;

			// get auto dates from BQ. We will either use our last test date from
			// previous runs or we will just both dates automatically.
		} else {
			LOG.info(">>> Looking for last pipeline run in datastore");

			dates = getDatesFromBQ();

			BQPipelineRunDatastore d = new BQPipelineRunDatastore();
			BQPipelineRun lastRun = d.getLastBQPipelineRun(this.pipelineRunRecord.getType());

			if (lastRun != null) {
				if (lastRun.getDataEndDate().length() > 0) {
					this.pipelineRunRecord.setDataStartDate(lastRun.getDataEndDate()); // our last run
				} else {
					this.pipelineRunRecord.setDataStartDate(dates[0]); // last ndt date. We've never had a good run.
				}
			} else {
				this.pipelineRunRecord.setDataStartDate(dates[0]); // our last test date in table
			}

			this.pipelineRunRecord.setDataEndDate(dates[1]);
			this.pipelineRunRecord.save();
			dates = this.pipelineRunRecord.getDates();
		}

		LOG.info("Dates computed: " + dates[0] + "-" + dates[1]);
		return dates;
	}

	/**
	 * Slows down the end date by 48 hours. If the start date is now later than the
	 * end date then returns null all together.
	 *
	 * @param runDates
	 * @return
	 */
	private String[] slowDownDates(String[] runDates) {
		String startDate = runDates[0];
		String endDate = runDates[1];

		Calendar startDateCal = Calendar.getInstance();
		Calendar endDateCal = Calendar.getInstance();
		try {
			startDateCal.setTime(dateFormatter.parse(startDate));
			endDateCal.setTime(dateFormatter.parse(endDate));

			int daysToSlowDownBy = -2;
			if (System.getenv("SLOW_DOWN_NDT_READ_BY_DAYS") != null) {
				LOG.debug("Using config for number of days to slow down by: "
						+ System.getenv("SLOW_DOWN_NDT_READ_BY_DAYS"));
				daysToSlowDownBy = Integer.parseInt(System.getenv("SLOW_DOWN_NDT_READ_BY_DAYS"));
			}
			endDateCal.add(Calendar.DAY_OF_YEAR, daysToSlowDownBy);

			// as long as the end date is larger than the start date, return
			// the new slowed down dates.
			if (endDateCal.compareTo(startDateCal) > 0) {
				LOG.info("Slowing down pipeline by " + daysToSlowDownBy + " days.");
				return new String[] { startDate, dateFormatter.format(endDateCal.getTime()) };
			} else {
				return null;
			}
		} catch (ParseException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Runs one main pipeline read and write given that we have a table that does
	 * not have allowLargeResutls set to false.
	 *
	 * Specification in that scenario do not require a "dates" array or a
	 * "numberOfDays" by which to batch things up.
	 *
	 * Example: <code>
	* {
	*     "queryFile": "./data/queries/base_downloads_ip_by_day.sql",
	*     "schemaFile": "./data/schemas/base_downloads_ip.json",
	*     "outputTable": "data_viz.base_downloads_ip_by_day"
	* }
	* </code>
	 */
	public void run() {
		String queryFile = this.config.getQueryFile();
		TableSchema tableSchema = Schema.fromJSONFile(this.config.getSchemaFile());
		String outputTableName = this.config.getOutputTable();

		QueryBuilder qb;

		try {
			// if we are appending, figure out the time range. If we are not
			// then we are replacing the tables, so just get the full NDT range
			// and use that.
			if (this.getWriteDisposition() == WriteDisposition.WRITE_APPEND) {
				determineRunDates();
			} else {
				this.dates = getFullDateRange();
			}

			// slow down dates
			this.dates = this.slowDownDates(this.dates);
			if (this.dates == null) {
				LOG.info(
						"Slowed down dates invalid. Likley end date now less than start date because of incremental update. Waiting another day for next pipeline.");
				this.setState(State.CANCELLED);
			} else {
				LOG.info(">>> Kicking off pipeline for dates: " + this.dates[0] + " " + this.dates[1]);
				LOG.info("Setup - Query file: " + queryFile);

				// build query:
				// {0} - start date
				// {1} - end date
				// {2} - table name from which we read.
				String[] templateData = { this.dates[0], this.dates[1], this.config.getNDTTable() };
				qb = new QueryBuilder(queryFile, templateData);
				try {
					LOG.info("Setup - Table Schema: " + tableSchema.toPrettyString());
				} catch (IOException e) {
					LOG.error(e.getMessage());
					LOG.error(e.getStackTrace().toString());
				}
				LOG.info("Setup - Output table: " + outputTableName);

				if (this.pipeline == null) {
					LOG.info("CREATING PIPELINE");
					this.pipeline = Pipeline.create(this.options);
				}

				// DataFlow doesn't show names with / in it
				String normalizedQueryFile = queryFile.replaceAll("/", "__");

				PCollection<TableRow> rows = this.pipeline.apply("Running query " + normalizedQueryFile,
						BigQueryIO.readTableRows().fromQuery(qb.getQuery()).usingStandardSql());

				rows.apply("Write " + outputTableName,
						BigQueryIO.writeTableRows().to(outputTableName).withSchema(tableSchema)
								.withCreateDisposition(this.createDisposition)
								.withWriteDisposition(this.writeDisposition));

				// by default, the pipeline will not run.
				if (this.isExecutable()) {
					DataflowPipelineJob result = (DataflowPipelineJob) this.pipeline.run();
					result.waitUntilFinish(Duration.ZERO, new MonitoringUtil.LoggingHandler());
					this.setState(result.getState());
					LOG.info("Job completed, with status: " + result.getState().toString());
				} else {
					LOG.info("Will not run");
				}
			}
		} catch (IOException | InterruptedException | SQLException | GeneralSecurityException | BigQueryException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
	}
}
