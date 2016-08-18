package mlab.bocoup;

import java.util.Map;
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
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import mlab.bocoup.dofn.AddISPsFn;
import mlab.bocoup.dofn.AddLocationNamesFn;
import mlab.bocoup.dofn.ExtractAsnNumFn;
import mlab.bocoup.dofn.ExtractRegionCodeFn;
import mlab.bocoup.dofn.MergeAsnsFn;
import mlab.bocoup.transform.CombineAsNavigableMapHex;
import mlab.bocoup.util.Schema;

public class MergeASNsPipeline {
	
	
	private static final String INPUT_TABLE = "mlab-oti:bocoup.tmp_jim_input";
	private static final String OUTPUT_TABLE = "mlab-oti:bocoup.tmp_jim_input";
	
	
	private static final Logger LOG = LoggerFactory.getLogger(AddISPsPipeline.class);
	private static final String MERGE_ASN_TABLE = "mlab-oti:bocoup.asn_merge";
	
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";
	
	private Pipeline pipeline;
	private String inputTable = INPUT_TABLE;
	private String outputTable = OUTPUT_TABLE;
	
	private String mergeAsnTable = MERGE_ASN_TABLE;
	
	private boolean writeData = false;
	private TableSchema outputSchema;
	private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
	private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
	
	/**
	 * Create a new pipeline.
	 * @param p The Dataflow pipeline to build on
	 */
	public MergeASNsPipeline(Pipeline p) {
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
		data = this.mergeASNs(data);
		

		if (this.writeData) {
			// write the processed data to a table
			this.writeByIpData(data);
		}

		return data;
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
				.named("Read " + this.mergeAsnTable)
				.from(this.mergeAsnTable));
		
		PCollection<KV<String, TableRow>> mergeAsnKeys = 
				mergeAsn.apply(ParDo.named("find region keys")
						.of(new ExtractAsnNumFn()));
		
		PCollectionView<Map<String, TableRow>> mergeAsnMap = mergeAsnKeys.apply(View.asMap());


		// Use the side-loaded MaxMind ISP data to get client ISPs
		PCollection<TableRow> byIpDataWithISPs = data.apply(
				ParDo
				.named("Add Client ISPs (side input)")
				.withSideInputs(mergeAsnMap)
				.of(new MergeAsnsFn(mergeAsnMap)));
				
		return byIpDataWithISPs;
	}


	private void preparePipeline() {
		// TODO Auto-generated method stub
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


	public MergeASNsPipeline setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
		return this;
	}


	public String getInputTable() {
		return inputTable;
	}


	public MergeASNsPipeline setInputTable(String inputTable) {
		this.inputTable = inputTable;
		return this;
	}


	public String getOutputTable() {
		return outputTable;
	}


	public MergeASNsPipeline setOutputTable(String outputTable) {
		this.outputTable = outputTable;
		return this;
	}



	public TableSchema getOutputSchema() {
		return outputSchema;
	}


	public MergeASNsPipeline setOutputSchema(TableSchema outputSchema) {
		this.outputSchema = outputSchema;
		return this;
	}


	public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
		return writeDisposition;
	}


	public MergeASNsPipeline setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}


	public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
		return createDisposition;
	}


	public MergeASNsPipeline setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}

	public boolean getWriteData() {
		return writeData;
	}


	public MergeASNsPipeline setWriteData(boolean writeData) {
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

		MergeASNsPipeline mergeASNs = new MergeASNsPipeline(p);
		mergeASNs
			.setWriteData(true)
			.setOutputTable(OUTPUT_TABLE)
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		mergeASNs.apply();

		p.run();
	}


}
