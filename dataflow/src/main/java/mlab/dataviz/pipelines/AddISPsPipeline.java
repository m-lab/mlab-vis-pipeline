package mlab.dataviz.pipelines;

import java.util.NavigableMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.coder.NavigableMapCoder;
import mlab.dataviz.dofn.AddISPsFn;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.transform.CombineAsNavigableMapHex;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.Schema;

/**
 * Pipeline for adding server and client ISPs to a dataset. Makes use of the
 * `mlab_sites` table for server ISPs and the `maxmind_asn` table for client
 * ISPs.
 *
 * @author pbeshai
 *
 */
public class AddISPsPipeline extends BasePipeline {
	private static final String MAXMIND_ISPS_TABLE = "data_viz_helpers.maxmind_asn";

	// for running main()
	private static final String INPUT_TABLE = "data_viz_testing.maxmind_asn_test";
	private static final String OUTPUT_TABLE = "data_viz_testing.maxmind_asn_test_out";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private String maxmindIspsTable;

	private BQTableUtils bqUtils;

	/**
	 * Create a new pipeline.
	 * 
	 * @param p The Dataflow pipeline to build on
	 */
	public AddISPsPipeline(Pipeline p, BQTableUtils bqUtils) {
		super(p);
		this.bqUtils = bqUtils;
		this.setMaxmindIspsTable(MAXMIND_ISPS_TABLE); // default
	}

	/**
	 * Adds in ISP information for the client.
	 *
	 * @param p        The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information
	 *                 added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addServerISPs(PCollection<TableRow> byIpData,
			PCollectionView<NavigableMap<String, TableRow>> maxMindAsnView) {

		// Use the side-loaded MaxMind ISP data to get server ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply("Add Server ISPs", ParDo
				.of(new AddISPsFn(maxMindAsnView, "server_ip_family", "server_ip_base64", "min_ip_hex", "max_ip_hex",
						"min_ip_hex", "max_ip_hex", "server_asn_name", "server_asn_number", "asn_name", "asn_number"))
				.withSideInputs(maxMindAsnView));
		return byIpDataWithISPs;
	}

	/**
	 * Adds in ISP information for the client.
	 *
	 * @param p        The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information
	 *                 added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addClientISPs(PCollection<TableRow> byIpData,
			PCollectionView<NavigableMap<String, TableRow>> maxMindAsnView) {

		AddISPsFn addIspFn = new AddISPsFn(maxMindAsnView, "client_ip_family", "client_ip_base64", "min_ip_hex",
				"max_ip_hex", "min_ip_hex", "max_ip_hex", "client_asn_name", "client_asn_number", "asn_name",
				"asn_number");

		// Use the side-loaded MaxMind ISP data to get client ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply("Add Client ISPs",
				ParDo.of(addIspFn).withSideInputs(maxMindAsnView));
		return byIpDataWithISPs;
	}

	/**
	 * Adds necessary configuration to a Pipeline for adding ISPs to work.
	 *
	 * @param p The pipeline being used
	 * @return The pipeline being used (for convenience)
	 */
	@SuppressWarnings("serial")
	public void preparePipeline() {
		// needed to use NavigableMap as an output/accumulator for adding ISPs
		// efficiently
		pipeline.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(NavigableMap.class, NavigableMapCoder.class));
	}

	/**
	 * Add in the steps to add ISP information to a given pipeline. Writes the data
	 * to a table if writeData field is true.
	 *
	 * @param p        A pipeline to add the steps to. If null, a new pipeline is
	 *                 created.
	 * @param byIpData If provided, ISPs are added to this collection, otherwise
	 *                 they are read in from a BigQuery table.
	 *
	 * @return The PCollection with ISPs added
	 */
	@Override
	protected PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		// Read in the MaxMind ISP data
		PCollection<TableRow> maxMindAsn = pipeline.apply("Read " + this.maxmindIspsTable,
				BigQueryIO.readTableRows().from(getMaxmindIspsTable()));

		// Make the MaxMind ISP data ready for side input
		PCollectionView<NavigableMap<String, TableRow>> maxMindAsnView = maxMindAsn.apply("Combine ISPs as NavMap",
				Combine.globally(new CombineAsNavigableMapHex())
						// make a view so it can be used as side input
						.asSingletonView());

		// add in ISPs
		data = this.addServerISPs(data, maxMindAsnView);
		data = this.addClientISPs(data, maxMindAsnView);

		return data;
	}

	public String getMaxmindIspsTable() {
		return maxmindIspsTable;
	}

	public AddISPsPipeline setMaxmindIspsTable(String clientIspsTable) {
		this.maxmindIspsTable = this.bqUtils.wrapTable(clientIspsTable);
		return this;
	}

	/**
	 * The main program: start the pipeline, add in ISP information and write it to
	 * a table.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryOptions.class);

		// pipeline object
		Pipeline p = Pipeline.create(options);
		HistoricPipelineOptions optionsAddISPs = options.as(HistoricPipelineOptions.class);
		BQTableUtils bqTableUtils = new BQTableUtils(optionsAddISPs);

		AddISPsPipeline addISPs = new AddISPsPipeline(p, bqTableUtils);
		addISPs.setWriteData(true).setInputTable(bqTableUtils.wrapTable(INPUT_TABLE))
				.setOutputTable(bqTableUtils.wrapTable(OUTPUT_TABLE))
				.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
				.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
				.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addISPs.apply();

		p.run();
	}
}
