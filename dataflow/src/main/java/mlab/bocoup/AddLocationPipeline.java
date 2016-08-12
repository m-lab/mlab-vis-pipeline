package mlab.bocoup;

import java.util.Map;

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
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import mlab.bocoup.dofn.AddLocalTimeFn;
import mlab.bocoup.dofn.AddLocationNamesFn;
import mlab.bocoup.dofn.ExtractCountryCodeFn;
import mlab.bocoup.dofn.ExtractRegionCodeFn;
import mlab.bocoup.dofn.ExtractZoneKeynameFn;
import mlab.bocoup.util.Schema;

public class AddLocationPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(AddLocationPipeline.class);
	
	private static final String COUNTRY_CODE_TABLE = "bocoup.location_country_codes";
	private static final String REGION_CODE_TABLE = "bocoup.location_region_codes";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";
	
	private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
	private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
	
	private String inputTable;
	private String outputTable;
	private TableSchema outputSchema = Schema.fromJSONFile(OUTPUT_SCHEMA);
	private boolean writeData = false;
	
	private Pipeline pipeline;
	
	public AddLocationPipeline(Pipeline p) {
		this.pipeline = p;
	}

	public String getInputTable() {
		return inputTable;
	}

	public AddLocationPipeline setInputTable(String inputTable) {
		this.inputTable = inputTable;
		return this;
	}

	public String getOutputTable() {
		return outputTable;
	}

	public AddLocationPipeline setOutputTable(String outputTable) {
		this.outputTable = outputTable;
		return this;
	}

	public TableSchema getOutputSchema() {
		return outputSchema;
	}

	public AddLocationPipeline setOutputSchema(TableSchema outputSchema) {
		this.outputSchema = outputSchema;
		return this;
	}

	public boolean isWriteData() {
		return writeData;
	}

	public AddLocationPipeline setWriteData(boolean writeData) {
		this.writeData = writeData;
		return this;
	}

	public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
		return writeDisposition;
	}

	public AddLocationPipeline setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
		return createDisposition;
	}

	public AddLocationPipeline setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}
	
	public PCollection<TableRow> loadData() {
		// read in the by IP data from `by_ip_day_base`
		PCollection<TableRow> rows = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.inputTable)
				.from(this.inputTable));

		return rows;
	}
	
	public PCollection<TableRow> apply() {
		return this.apply(null);
	}
	
	public PCollection<TableRow> apply(PCollection<TableRow> rows) {
		
		PCollection<TableRow> data;
		if (rows != null) {
			data = rows; 
		} else {
			data = this.loadData();
		}

		PCollection<TableRow> countryCodes = this.pipeline.apply(
			BigQueryIO.Read
				.named("Read " + COUNTRY_CODE_TABLE)
				.from(COUNTRY_CODE_TABLE));
		
		PCollection<TableRow> regionCodes = this.pipeline.apply(
				BigQueryIO.Read
					.named("Read " + REGION_CODE_TABLE)
					.from(REGION_CODE_TABLE));
		
		PCollection<KV<String, TableRow>> countryKeys = 
				countryCodes.apply(ParDo.named("find country keys")
						.of(new ExtractCountryCodeFn()));
		
		PCollection<KV<String, TableRow>> regionKeys = 
				regionCodes.apply(ParDo.named("find region keys")
						.of(new ExtractRegionCodeFn()));
		
		
		PCollectionView<Map<String, TableRow>> countryMap = countryKeys.apply(View.asMap());
		PCollectionView<Map<String, TableRow>> regionMap = regionKeys.apply(View.asMap());
		
		// resolve locations
		PCollection<TableRow> withLocations = data.apply(
			ParDo
				.named("Add country and region names")
				.withSideInputs(countryMap, regionMap)
				.of(new AddLocationNamesFn(countryMap, regionMap)));
		
		if (this.writeData) {
			withLocations.apply(
				BigQueryIO.Write
					.named("Write to " + this.outputTable)
					.to(this.outputTable)
					.withSchema(this.outputSchema)
					.withWriteDisposition(this.writeDisposition)
					.withCreateDisposition(this.createDisposition));
		}
		
		return withLocations;
	}
	
	public static void main(String [] args) throws ClassNotFoundException {
		
		String outputTable = "mlab-oti:bocoup.zz4_all_ip_by_day_with_locations";
		String inputTable = "mlab-oti:bocoup.zz4_all_isp_by_day";
		
		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigQueryOptions.class);
		options.setAppName("AddLocations");
		
		// pipeline object
		Pipeline p = Pipeline.create(options);
		
		AddLocationPipeline addLocations = new AddLocationPipeline(p);
		addLocations
			.setWriteData(true)
			.setOutputTable(outputTable)
		    .setInputTable(inputTable)
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
		
		addLocations.apply();
		
		p.run();
	}
}
