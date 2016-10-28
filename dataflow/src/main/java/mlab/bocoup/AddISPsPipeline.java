package mlab.bocoup;

import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import mlab.bocoup.coder.NavigableMapCoder;
import mlab.bocoup.dofn.AddISPsFn;
import mlab.bocoup.transform.CombineAsNavigableMapHex;
import mlab.bocoup.util.Schema;


/**
 * Pipeline for adding server and client ISPs to a dataset. Makes use of
 * the `mlab_sites` table for server ISPs and the `maxmind_asn` table for
 * client ISPs.
 *
 * @author pbeshai
 *
 */
public class AddISPsPipeline extends BasePipeline {
	private static final Logger LOG = LoggerFactory.getLogger(AddISPsPipeline.class);
	private static final String MAXMIND_ISPS_TABLE = "mlab-oti:bocoup.maxmind_asn";

	// for running main()
	private static final String INPUT_TABLE = "mlab-oti:bocoup.jim_test";
	private static final String OUTPUT_TABLE = "mlab-oti:bocoup.jim_test_out";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";
	
	private String maxmindIspsTable = MAXMIND_ISPS_TABLE;
	/**
	 * Create a new pipeline.
	 * @param p The Dataflow pipeline to build on
	 */
	public AddISPsPipeline(Pipeline p) {
		super(p);
	}

	/**
	 * Adds in ISP information for the client.
	 *
	 * @param p The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addServerISPs(PCollection<TableRow> byIpData, PCollectionView<NavigableMap<String, TableRow>> maxMindAsnView) {

		// Use the side-loaded MaxMind ISP data to get server ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply(
				ParDo
				.named("Add Server ISPs")
				.withSideInputs(maxMindAsnView)
				.of(new AddISPsFn(maxMindAsnView, "server_ip_family", "server_ip_base64", "min_ip_hex",
						"max_ip_hex", "min_ip_hex", "max_ip_hex", "server_asn_name", "server_asn_number",
						"asn_name", "asn_number")));
		return byIpDataWithISPs;
	}
	
	/**
	 * Adds in ISP information for the client.
	 *
	 * @param p The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addClientISPs(PCollection<TableRow> byIpData, PCollectionView<NavigableMap<String, TableRow>> maxMindAsnView) {
		
		// Use the side-loaded MaxMind ISP data to get client ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply(
				ParDo
				.named("Add Client ISPs")
				.withSideInputs(maxMindAsnView)
				.of(new AddISPsFn(maxMindAsnView, "client_ip_family", "client_ip_base64", "min_ip_hex",
						"max_ip_hex", "min_ip_hex", "max_ip_hex", "client_asn_name", "client_asn_number",
						"asn_name", "asn_number")));
		return byIpDataWithISPs;
	}


	/**
	 * Adds necessary configuration to a Pipeline for adding ISPs to work.
	 *
	 * @param p The pipeline being used
	 * @return The pipeline being used (for convenience)
	 */
	public void preparePipeline() {
		// needed to use NavigableMap as an output/accumulator for adding ISPs efficiently
		this.pipeline.getCoderRegistry().registerCoder(NavigableMap.class, NavigableMapCoder.class);
	}

	/**
	 * Add in the steps to add ISP information to a given pipeline. Writes the
	 * data to a table if writeData field is true.
	 *
	 * @param p A pipeline to add the steps to. If null, a new pipeline is created.
	 * @param byIpData If provided, ISPs are added to this collection, otherwise they are read
	 * in from a BigQuery table.
	 *
	 * @return The PCollection with ISPs added
	 */
	@Override
	protected PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		// Read in the MaxMind ISP data
		PCollection<TableRow> maxMindAsn = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.maxmindIspsTable)
				.from(this.maxmindIspsTable));

		//Make the MaxMind ISP data ready for side input
		PCollectionView<NavigableMap<String, TableRow>> maxMindAsnView =
				maxMindAsn
				.apply(Combine.globally(new CombineAsNavigableMapHex())
						.named("Combine ISPs as NavMap")
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
		this.maxmindIspsTable = clientIspsTable;
		return this;
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

		AddISPsPipeline addISPs = new AddISPsPipeline(p);
		addISPs
			.setWriteData(true)
			.setInputTable(INPUT_TABLE)
			.setOutputTable(OUTPUT_TABLE)
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addISPs.apply();

		p.run();
	}
}
