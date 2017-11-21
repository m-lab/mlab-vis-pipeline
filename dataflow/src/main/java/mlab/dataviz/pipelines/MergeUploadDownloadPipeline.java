package mlab.dataviz.pipelines;

import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.values.PCollection;

import mlab.dataviz.query.QueryBuilder;
import mlab.dataviz.util.BigQueryIOHelpers;
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
	private Pipeline p;

	private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
	private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;

	public MergeUploadDownloadPipeline(Pipeline p) {
		this.p = p;
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

	//
	// Prepares Pipeline.
	//
	public PCollection<TableRow> apply() throws IOException, ParseException {

		LOG.debug("MergeUploadDownloadPipeline - Query file: " + this.queryFile);
		LOG.debug("MergeUploadDownloadPipeline - Table Schema: " + this.outputSchema.toPrettyString());
		LOG.debug("MergeUploadDownloadPipeline - Download table: " + this.downloadTable);
		LOG.debug("MergeUploadDownloadPipeline - Upload table: " + this.uploadTable);
		LOG.debug("MergeUploadDownloadPipeline - Output table: " + this.outputTable);

		// Build query string
		Object[] queryParams = { downloadTable, uploadTable };

		QueryBuilder qb = new QueryBuilder(queryFile, queryParams);
		String queryString = qb.getQuery();
		String queryName = qb.toString();

		// set up the big query read
		PCollection<TableRow> rows = this.p
				.apply(BigQueryIO.read().fromQuery(queryString)).setName("run query: " + queryName);
		
		BigQueryIOHelpers.writeTable(rows, this.outputTable,
				this.outputSchema,
				writeDisposition, createDisposition);

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
		String downloadTable = "[mlab-sandbox:data_viz.base_downloads_ip_by_day]";
		String uploadTable = "[mlab-sandbox:data_viz.base_uploads_ip_by_day]";
		String outputTable = "data_viz.base_all_ip_by_day";

		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryOptions.class);

		Pipeline pipe = Pipeline.create(options);

		MergeUploadDownloadPipeline mudP = new MergeUploadDownloadPipeline(pipe);
		mudP.setQueryFile(queryFile)
			.setDownloadTable(downloadTable)
			.setOutputSchema(Schema.fromJSONFile(outputSchema))
			.setOutputTable(outputTable)
			.setUploadTable(uploadTable);

		mudP.apply();
		DataflowPipelineJob result = (DataflowPipelineJob) pipe.run();
		LOG.info("Job State: " + result.getState().toString());

	}
}
