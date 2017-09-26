package mlab.dataviz;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import mlab.dataviz.dofn.ExtractAsnNumFn;
import mlab.dataviz.dofn.MergeAsnsFn;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.Schema;

public class MergeASNsPipeline extends BasePipeline {
	private static final Logger LOG = LoggerFactory.getLogger(MergeASNsPipeline.class);

	private static final String MERGE_ASN_TABLE = "data_viz_helpers.asn_merge";

	//	for running main()
	private static final String INPUT_TABLE = "data_viz_testing.merge_asn_test";
	private static final String OUTPUT_TABLE = "data_viz_testing.merge_asn_test_output";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private String mergeAsnTable;
	private BQTableUtils bqUtils;

	/**
	 * Create a new pipeline.
	 * @param p The Dataflow pipeline to build on
	 */
	public MergeASNsPipeline(Pipeline p, BQTableUtils bqUtils) {
		super(p);
		this.bqUtils = bqUtils;
		this.setMergeASNTable(MERGE_ASN_TABLE);
	}


	/**
	 * Merges ASNs according to the asn_merge table
	 * @return The PCollection with ASNs merged
	 */
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		// add in ISPs
		data = this.mergeASNs(data);
		return data;
	}

	/**
	 * Get the merge ASN table name
	 * @return
	 */
	public String getMergeASNTable() {
		return this.mergeAsnTable;
	}
	
	/**
	 * Set the table name used for the merge ASN table.
	 * @param mergeASNTable String name
	 */
	public void setMergeASNTable(String mergeASNTable) {
		this.mergeAsnTable = this.bqUtils.wrapTable(mergeASNTable);
	}
	/**
	 * Merge ASNs information for the client ASN.
	 *
	 * @param data The PCollection representing the rows to have ISP information added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> mergeASNs(PCollection<TableRow> data) {
		// Read in the MaxMind ISP data
		PCollection<TableRow> mergeAsn = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.getMergeASNTable())
				.from(this.getMergeASNTable()));

		PCollection<KV<String, TableRow>> mergeAsnKeys =
				mergeAsn.apply(ParDo.named("Extract ASN from " + this.getMergeASNTable())
						.of(new ExtractAsnNumFn()));

		PCollectionView<Map<String, TableRow>> mergeAsnMap = mergeAsnKeys.apply(View.asMap());


		// Use the side-loaded Merge ASN table to merge the ASNs for clients and servers
		PCollection<TableRow> byIpDataWithISPs = data.apply(
				ParDo
				.named("Merge ASNs")
				.withSideInputs(mergeAsnMap)
				.of(new MergeAsnsFn(mergeAsnMap)));

		return byIpDataWithISPs;
	}

	/**
	 * The main program: start the pipeline, add in ISP information and write it to a table.
	 * @param args
	 */
	public static void main(String[] args) {
		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigQueryOptions.class);

		// pipeline object
		Pipeline p = Pipeline.create(options);

		HistoricPipelineOptions optionsMergeASN = options.cloneAs(HistoricPipelineOptions.class);
		BQTableUtils bqTableUtils = new BQTableUtils(optionsMergeASN);
		
		MergeASNsPipeline mergeASNs = new MergeASNsPipeline(p, bqTableUtils);
		mergeASNs
		.setWriteData(true)
		.setInputTable(bqTableUtils.wrapTable(INPUT_TABLE))
		.setOutputTable(bqTableUtils.wrapTable(OUTPUT_TABLE))
		.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
		.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		mergeASNs.apply();

		p.run();
	}
}
