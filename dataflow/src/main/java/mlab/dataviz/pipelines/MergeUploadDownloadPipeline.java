package mlab.dataviz.pipelines;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQueryException;

import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.query.BigQueryJob;
import mlab.dataviz.query.QueryBuilder;
import mlab.dataviz.util.Formatters;
import mlab.dataviz.util.Schema;

public class MergeUploadDownloadPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(MergeUploadDownloadPipeline.class);

	private static final String QUERY_FILE = "./data/bigquery/queries/merge_upload_download.sql";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private String queryFile = QUERY_FILE;
	private TableSchema outputSchema = Schema.fromJSONFile(OUTPUT_SCHEMA);
	private String downloadTable;
	private String uploadTable;
	private String outputTable;
	private String dataStartDate;
	private String dataEndDate;
	private Pipeline p;
	private HistoricPipelineOptions options;

	private static SimpleDateFormat dtf = new SimpleDateFormat(Formatters.TIMESTAMP2);

	private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
	private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;

	public MergeUploadDownloadPipeline(HistoricPipelineOptions options, Pipeline p) {
		this.p = p;
		this.options = options;
	}

	public MergeUploadDownloadPipeline setDataStartDate(String startDate) {
		this.dataStartDate = startDate;
		return this;
	}

	public MergeUploadDownloadPipeline setDataEndDate(String endDate) {
		this.dataEndDate = endDate;
		return this;
	}

	public MergeUploadDownloadPipeline setQueryFile(String queryFile) {
		this.queryFile = queryFile;
		return this;
	}

	public MergeUploadDownloadPipeline setOutputSchema(TableSchema outputSchema) {
		this.outputSchema = outputSchema;
		return this;
	}

	public MergeUploadDownloadPipeline setDownloadTable(String downloadTable) {
		this.downloadTable = downloadTable;
		return this;
	}

	public MergeUploadDownloadPipeline setUploadTable(String uploadTable) {
		this.uploadTable = uploadTable;
		return this;
	}

	public MergeUploadDownloadPipeline setOutputTable(String outputTable) {
		this.outputTable = outputTable;
		return this;
	}

	public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
		return this.writeDisposition;
	}

	public MergeUploadDownloadPipeline setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
		return this.createDisposition;
	}

	public MergeUploadDownloadPipeline setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}

	/**
	 * Returns the date range appropriate for the merge table. The logic is as
	 * follows: get last date from merge table as C if empty, get min date from
	 * download table as A get min date from upload table as B use min {A,B} else
	 * get max date from upload table as A if empty, error get max date from
	 * download table as B if empty, error if not empty, use the earlier of {A,B,C}
	 *
	 * @return String[] of dates
	 */
	private String[] getDates() {
		String[] dates = new String[2];

		String dateMinQueryDownloadResult;
		String dateMinQueryUploadResult;
		String dateMaxQueryDownloadResult;
		String dateMaxQueryUploadResult;
		String dateMaxQueryMergeResult = "";

		String queryBaseMin = "select STRFTIME_UTC_USEC(min(test_date), \"%F %X\") as date from ";
		String queryBaseMax = "select STRFTIME_UTC_USEC(max(test_date), \"%F %X\") as date from ";

		String dateMinQueryDownload = queryBaseMin + this.downloadTable;
		String dateMinQueryUpload = queryBaseMin + this.uploadTable;
		String dateMaxQueryDownload = queryBaseMax + this.downloadTable;
		String dateMaxQueryUpload = queryBaseMax + this.uploadTable;
		String dateMaxQueryMerge = queryBaseMax + "[" + this.outputTable + "]";

		// sort all dates
		SortedSet<Date> sortedSet = new TreeSet<Date>();
		try {
			BigQueryJob bqj = new BigQueryJob();

			// merge table
			boolean found = true;
			try {
				dateMaxQueryMergeResult = bqj.executeQueryForValue(dateMaxQueryMerge);
			} catch (Exception e) {
				// not found
				found = false;
			}

			if (!found) {
				// merge table empty, just use the full date range from the upload/download
				// tables.
				// download table
				dateMinQueryDownloadResult = bqj.executeQueryForValue(dateMinQueryDownload);
				if (dateMinQueryDownloadResult.length() == 0) {
					throw new IOException(
							"Download min date not found - is the table " + this.downloadTable + " empty?");
				}

				// upload table
				dateMinQueryUploadResult = bqj.executeQueryForValue(dateMinQueryUpload);
				if (dateMinQueryUpload.length() == 0) {
					throw new IOException("Upload min date not found - is the table " + this.uploadTable + " empty?");
				}

				sortedSet.add(dtf.parse(dateMinQueryDownloadResult));
				sortedSet.add(dtf.parse(dateMinQueryUploadResult));

			} else {
				// we have some rows in the merge table, so get the minimum value of the max
				// of all tables, and use that.
				dateMaxQueryDownloadResult = bqj.executeQueryForValue(dateMaxQueryDownload);
				if (dateMaxQueryDownloadResult.length() == 0) {
					throw new IOException(
							"Download max date not found - is the table " + this.downloadTable + " empty?");
				}

				dateMaxQueryUploadResult = bqj.executeQueryForValue(dateMaxQueryUpload);
				if (dateMaxQueryUploadResult.length() == 0) {
					throw new IOException("Upload max date not found - is the table " + this.uploadTable + " empty?");
				}

				sortedSet.add(dtf.parse(dateMaxQueryDownloadResult));
				sortedSet.add(dtf.parse(dateMaxQueryUploadResult));
				sortedSet.add(dtf.parse(dateMaxQueryMergeResult));
			}

			if (this.dataStartDate.length() > 0) {
				sortedSet.add(dtf.parse(this.dataStartDate));
			}

			return new String[] {
					// get first (earliest date).
					dtf.format(sortedSet.first()), this.dataEndDate };

			// parse as dates
		} catch (IOException | ParseException | InterruptedException | BigQueryException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
		return dates;
	}

	//
	// Prepares Pipeline.
	//
	public PCollection<TableRow> apply() throws IOException, ParseException {

		LOG.debug("MergeUploadDownloadPipeline - Query file: " + this.queryFile);
		LOG.debug("MergeUploadDownloadPipeline - Table Schema: " + this.outputSchema.toPrettyString());
		LOG.debug("MergeUploadDownloadPipeline - Download table: " + this.downloadTable);
		LOG.debug("MergeUploadDownloadPipeline - Upload table: " + this.uploadTable);
		LOG.debug("MergeUploadDownloadPipeline - Output table: " + this.outputTable);

		// Build query string:
		// Query will run on full tables and truncate original table each time.
		// For now, that is fast enough.
		// String [] dates = getDates();
		// LOG.debug("Merge pipeline running on dates " + dates[0] + "-" + dates[1]);
		Object[] queryParams = { downloadTable, uploadTable };

		QueryBuilder qb = new QueryBuilder(queryFile, queryParams);
		String queryString = qb.getQuery();
		String queryName = qb.toString();

		// set up the big query read
		PCollection<TableRow> rows = this.p.apply("run query: " + queryName,
				BigQueryIO.readTableRows().fromQuery(queryString).usingStandardSql());

		rows.apply("write table " + this.outputTable,
				BigQueryIO.writeTableRows().to(this.outputTable).withSchema(this.outputSchema)
						.withCreateDisposition(createDisposition).withWriteDisposition(writeDisposition));

		return rows;
	}

	/**
	 * The main program, merges two upload and download tables.
	 *
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException {

		String queryFile = "./data/bigquery/queries/merge_upload_download.sql";
		String outputSchema = "./data/bigquery/schemas/merged_all_ip.json";
		String downloadTable = "[mlab-oti:data_viz.base_downloads_ip_by_day]";
		String uploadTable = "[mlab-oti:data_viz.base_uploads_ip_by_day]";
		String outputTable = "mlab-oti:data_viz.all_ip_by_day";

		HistoricPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(HistoricPipelineOptions.class);

		Pipeline pipe = Pipeline.create(options);

		MergeUploadDownloadPipeline mudP = new MergeUploadDownloadPipeline(options, pipe);
		mudP.setQueryFile(queryFile).setDataStartDate("2017-01-01 01:00:00").setDataEndDate("2017-01-15 10:00:00")
				.setDownloadTable(downloadTable).setOutputSchema(Schema.fromJSONFile(outputSchema))
				.setOutputTable(outputTable).setUploadTable(uploadTable);

		mudP.apply();
		DataflowPipelineJob result = (DataflowPipelineJob) pipe.run();
		LOG.info("Job State: " + result.getState().toString());

	}
}
