package mlab.dataviz.pipelines;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.dataviz.entities.BQPipelineRun;
import mlab.dataviz.entities.BQPipelineRunDatastore;
import mlab.dataviz.query.BigQueryJob;
import mlab.dataviz.query.QueryBuilder;
import mlab.dataviz.util.Formatters;
import mlab.dataviz.util.Schema;
import mlab.dataviz.util.PipelineConfig;

public class NDTReadPipeline implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(NDTReadPipeline.class);
	private BigQueryOptions options;
	private PipelineConfig config;

	private WriteDisposition writeDisposition = WriteDisposition.WRITE_APPEND;
	private CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;
	private State state;
	private Pipeline pipeline;
	private boolean runPipeline = false;
	private BQPipelineRun pipelineRunRecord = null;
	private static SimpleDateFormat dtf = new SimpleDateFormat(Formatters.TIMESTAMP);

	private String[] dates;

	public NDTReadPipeline(Pipeline p, BigQueryOptions options, BQPipelineRun status) {
		this.pipeline = p;
		this.options = options;
		this.pipelineRunRecord = status;
	}

	public NDTReadPipeline(BigQueryOptions options, BQPipelineRun status) {
		this.options = options;
		this.pipelineRunRecord = status;
	}

	public NDTReadPipeline setPipelineConfiguration(PipelineConfig config) {
		this.config = config;
		return this;
	}

	public PipelineConfig getPipelineConfig() {
		return this.config;
	}

	public WriteDisposition getWriteDisposition() {
		return this.writeDisposition;
	}

	public NDTReadPipeline setWriteDisposition(WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	public CreateDisposition getCreateDisposition() {
		return this.createDisposition;
	}

	public NDTReadPipeline setCreateDisposition(CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}

	public State getState() {
		return this.state;
	}

	public void setState(State s) {
		this.state = s;
	}

	public NDTReadPipeline shouldExecute(boolean execute) {
		this.runPipeline  = execute;
		return this;
	}

	public boolean isExecutable() {
		return this.runPipeline;
	}

	public NDTReadPipeline setDates(String [] dates) {
		this.dates = dates;
		return this;
	}

	/**
	 * Gets a date range based on the last test recorded in our ndt write tables.
	 *
	 * The start date is the max test date that we have parsed in our table.
	 * The end date is the max test date that is in the NDT table.
	 *
	 * @todo change the test_date to parse_date when that data gets there.
	 * @return
	 * @throws IOException
	 */
	public String[] getDatesFromBQ() throws IOException {

		// read from one of our tables
		String startDateRangeQuery = "select STRING(max(test_date)) as min_test_date from "
				+ this.config.getStartDateFromTable();

		// read from NDT
		String endDateRangeQuery = "SELECT STRING(USEC_TO_TIMESTAMP(UTC_USEC_TO_DAY(max(web100_log_entry.log_time * INTEGER(POW(10, 6))))))"
				+ " as max_test_date FROM " + this.config.getEndDateFromTable();

		BigQueryJob bqj = new BigQueryJob(this.options.getProject());
		java.util.List<TableRow> startRows;
		String[] timestamps = new String[2];
		boolean seekNDT = false;
		try {
			startRows = bqj.executeQuery(startDateRangeQuery);
			for (TableRow row : startRows) {
				for (TableCell field : row.getF()) {
					if (!field.containsValue(null)) {
						// for some reason even when the value is null, the above test doesn't work
						// and we have a ClassCastException. It's been added to the catch, but surely
						// it should be something different here.
						timestamps[0] = (String) field.getV();
					} else {
						seekNDT = true;
					}
				}
			}
		} catch (GoogleJsonResponseException | ClassCastException e) {
			// our table doesn't exist yet.
			seekNDT = true;
		}

		if (seekNDT) {
			startDateRangeQuery = "SELECT STRING(USEC_TO_TIMESTAMP(UTC_USEC_TO_DAY(min(web100_log_entry.log_time * INTEGER(POW(10, 6))))))"
					+ " as min_test_date FROM " + this.config.getEndDateFromTable();
			startRows = bqj.executeQuery(startDateRangeQuery);

			for (TableRow row : startRows) {
				for (TableCell field : row.getF()) {
					timestamps[0] = (String) field.getV();
				}
			}
		}

		java.util.List<TableRow> endRows = bqj.executeQuery(endDateRangeQuery);

		for (TableRow row : endRows) {
			for (TableCell field : row.getF()) {
				timestamps[1] = (String) field.getV();
			}
		}

		return timestamps;
	}

	/**
	 * @private
	 * Determines the run dates for the pipeline.
	 * The order is as follows:
	 *   - command-line dates take precedence
	 *   - then last run in datastore. Use the end date there, alongside the last date in NDT.
	 *   - auto detect. Start date is our last test, end date is last test in NDT.
	 *
	 * @throws IOException
	 * @throws SQLException
	 * @throws GeneralSecurityException 
	 */
	private String[] determineRunDates() throws IOException, SQLException, GeneralSecurityException {

		// if we already have the dates from a previous run, use them.
		// since this pipeline is used twice, once for daily and one for hourly
		// we expect this to be set the first time and then be used.
		if (!this.pipelineRunRecord.getDataStartDate().equals("") &&
			!this.pipelineRunRecord.getDataEndDate().equals("")) {
			this.dates = new String[] {
					this.pipelineRunRecord.getDataStartDate(),
					this.pipelineRunRecord.getDataEndDate()
			};
		// Pipeline run dates provided via the command-line. Use those.
		} else if (dates != null & !dates[0].equals("") && !dates[1].equals("")) {
			LOG.info(">>> Dates passed via commandline.");

			this.pipelineRunRecord.setDataStartDate(dates[0]);
			this.pipelineRunRecord.setDataEndDate(dates[1]);
			this.pipelineRunRecord.save();

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
					this.pipelineRunRecord.setDataStartDate(dates[0]); // our last run
				}
			} else {
				LOG.info(">>> Dates not in config. Attempting to generate automatically.");
				this.pipelineRunRecord.setDataStartDate(dates[0]); // our last test date in table
			}

			this.pipelineRunRecord.setDataEndDate(dates[1]);
			this.pipelineRunRecord.save();
			dates = this.pipelineRunRecord.getDates();
		}
		LOG.info("Dates computed: " + dates);
		return dates;
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
	*     "outputTable": "data_viz.base_downloads_ip_by_day"
	* }
	* </code>
	*/
	public void run() {
		String queryFile = this.config.getQueryFile();
		TableSchema tableSchema = Schema.fromJSONFile(this.config.getSchemaFile());
		String outputTableName =  this.config.getOutputTable();

		QueryBuilder qb;

		try {
			determineRunDates();

			LOG.info(">>> Kicking off pipeline for dates: " + this.dates[0] + " " + this.dates[1]);
			LOG.info("Setup - Query file: " + queryFile);
			qb = new QueryBuilder(queryFile, this.dates);
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

			PCollection<TableRow> rows = this.pipeline.apply(
					BigQueryIO.Read
					.named("Running query " + normalizedQueryFile)
					.fromQuery(qb.getQuery()));

			rows.apply(BigQueryIO.Write
					.named("Write " + outputTableName)
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
			e.printStackTrace();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		} catch (GeneralSecurityException e1) {
			LOG.error(e1.getMessage());
			e1.printStackTrace();
		}
	}
}
