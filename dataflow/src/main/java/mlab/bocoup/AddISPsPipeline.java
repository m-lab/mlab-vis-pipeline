package mlab.bocoup;

import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
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
public class AddISPsPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(AddISPsPipeline.class);

	private static final String INPUT_TABLE = "mlab-oti:bocoup.tmp_peter_hex_input";
	private static final String OUTPUT_TABLE = "mlab-oti:bocoup.tmp_peter_output";

	private static final String CLIENT_ISPS_TABLE = "mlab-oti:bocoup.maxmind_asn";
	private static final String SERVER_ISPS_TABLE = "mlab-oti:bocoup.mlab_sites";

	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private Pipeline pipeline;
	private String inputTable = INPUT_TABLE;
	private String outputTable = OUTPUT_TABLE;
	private String clientIspsTable = CLIENT_ISPS_TABLE;
	private String serverIspsTable = SERVER_ISPS_TABLE;
	private boolean writeData = false;
	private TableSchema outputSchema;
	private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
	private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;

	/**
	 * Create a new pipeline.
	 * @param p The Dataflow pipeline to build on
	 */
	public AddISPsPipeline(Pipeline p) {
		this.pipeline = p;
	}

	/**
	 * Initial read of the source data table from BigQuery.
	 *
	 * @param p The Dataflow Pipeline
	 * @return The PCollection representing the data from BigQuery
	 */
	public PCollection<TableRow> loadByIpData() {
		// read in the by IP data from `by_ip_day_base`
		PCollection<TableRow> byIpData = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.inputTable)
				.from(this.inputTable));

		return byIpData;
	}

	/**
	 * Adds in ISP information for the server.
	 *
	 * @param p The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addServerISPs(PCollection<TableRow> byIpData) {
		// Read in the MaxMind ISP data
		PCollection<TableRow> mlabSites = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.serverIspsTable)
				.from(this.serverIspsTable));

		// Make the MaxMind ISP data ready for side input
		PCollectionView<NavigableMap<String, TableRow>> mlabSitesView =
				mlabSites
				.apply(Combine.globally(new CombineAsNavigableMapHex())
						.named("Combine MLab Sites as NavMap")
						// make a view so it can be used as side input
						.asSingletonView());

		// Use the side-loaded MaxMind ISP data to get client ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply(
				ParDo
				.named("Add Server ISPs (side input)")
				.withSideInputs(mlabSitesView)
				.of(new AddISPsFn(mlabSitesView, "server_ip_family", "server_ip_base64", "server_asn_name",
						"server_asn_number", "min_ip_hex", "max_ip_hex", "min_ipv6_hex", "max_ipv6_hex",
						"transit_provider", null)));

		return byIpDataWithISPs;
	}

	/**
	 * Adds in ISP information for the client.
	 *
	 * @param p The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addClientISPs(PCollection<TableRow> byIpData) {
		// Read in the MaxMind ISP data
		PCollection<TableRow> maxMindAsn = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.clientIspsTable)
				.from(this.clientIspsTable));

		// Make the MaxMind ISP data ready for side input
		PCollectionView<NavigableMap<String, TableRow>> maxMindAsnView =
				maxMindAsn
				.apply(Combine.globally(new CombineAsNavigableMapHex())
						.named("Combine ISPs as NavMap")
						// make a view so it can be used as side input
						.asSingletonView());


		// Use the side-loaded MaxMind ISP data to get client ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply(
				ParDo
				.named("Add Client ISPs (side input)")
				.withSideInputs(maxMindAsnView)
				.of(new AddISPsFn(maxMindAsnView, "client_ip_family", "client_ip_base64", "client_asn_name",
						"client_asn_number", "min_ip_hex", "max_ip_hex", "min_ip_hex", 
						"max_ip_hex", "asn_name", "asn_number")));
		return byIpDataWithISPs;
	}


	/**
	 * Write the modified rows to a destination table in BigQuery
	 *
	 * @param byIpData The PCollection of rows with ISP information added to them already
	 * @param outputTable The identifier for the table to write to (e.g. `bocoup.my_table`)
	 * @param outputTableSchema The schema describing the output table
	 */
	public void writeByIpData(PCollection<TableRow> byIpData) {
		// Write the changes to `by_ip_day`


		byIpData.apply(
				BigQueryIO.Write
				.named("Write to " + this.outputTable)
				.to(this.outputTable)
				.withSchema(this.outputSchema)
				.withWriteDisposition(this.writeDisposition)
				.withCreateDisposition(this.createDisposition));
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
	public PCollection<TableRow> apply(PCollection<TableRow> byIpData) {
		this.preparePipeline();

		// read in the data from the table unless it has been provided
		PCollection<TableRow> data;
		if (byIpData != null) {
			data = byIpData;
		} else {
			data = this.loadByIpData();
		}

		// add in ISPs
		data = this.addServerISPs(data);
		data = this.addClientISPs(data);

		if (this.writeData) {
			// write the processed data to a table
			this.writeByIpData(data);
		}

		return data;
	}

	/**
	 * Add in the steps to add ISP information to a given pipeline. Reads data from a
	 * BigQuery table to begin with. Writes the data to a table if writeData field is
	 * true.
	 *
	 * @return The PCollection with ISPs added
	 */
	public PCollection<TableRow> apply() {
		return this.apply(null);
	}

	public Pipeline getPipeline() {
		return pipeline;
	}


	public AddISPsPipeline setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
		return this;
	}


	public String getInputTable() {
		return inputTable;
	}


	public AddISPsPipeline setInputTable(String inputTable) {
		this.inputTable = inputTable;
		return this;
	}


	public String getOutputTable() {
		return outputTable;
	}


	public AddISPsPipeline setOutputTable(String outputTable) {
		this.outputTable = outputTable;
		return this;
	}


	public String getClientIspsTable() {
		return clientIspsTable;
	}


	public AddISPsPipeline setClientIspsTable(String clientIspsTable) {
		this.clientIspsTable = clientIspsTable;
		return this;
	}


	public String getServerIspsTable() {
		return serverIspsTable;
	}


	public AddISPsPipeline setServerIspsTable(String serverIspsTable) {
		this.serverIspsTable = serverIspsTable;
		return this;
	}


	public TableSchema getOutputSchema() {
		return outputSchema;
	}


	public AddISPsPipeline setOutputSchema(TableSchema outputSchema) {
		this.outputSchema = outputSchema;
		return this;
	}


	public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
		return writeDisposition;
	}


	public AddISPsPipeline setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}


	public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
		return createDisposition;
	}


	public AddISPsPipeline setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}

	public boolean getWriteData() {
		return writeData;
	}


	public AddISPsPipeline setWriteData(boolean writeData) {
		this.writeData = writeData;
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
			.setOutputTable(OUTPUT_TABLE)
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addISPs.apply();

		p.run();
	}
}
