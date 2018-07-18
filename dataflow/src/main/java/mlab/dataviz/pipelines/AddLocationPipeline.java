package mlab.dataviz.pipelines;

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

import mlab.dataviz.dofn.AddLocationNamesFn;
import mlab.dataviz.dofn.ExtractCountryCodeFn;
import mlab.dataviz.dofn.ExtractRegionCodeFn;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.Schema;

public class AddLocationPipeline extends BasePipeline {
	private static final Logger LOG = LoggerFactory.getLogger(AddLocationPipeline.class);

	private static final String COUNTRY_CODE_TABLE = "data_viz_helpers.location_country_codes";
	private static final String REGION_CODE_TABLE = "data_viz_helpers.location_region_codes";

	// for running main()
	private static final String INPUT_TABLE = "data_viz.all_isp_by_day";
	private static final String OUTPUT_TABLE = "data_viz.all_ip_by_day_with_locations";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private BQTableUtils bqUtils;
	private String countryCodeTable;
	private String regionCodeTable;
	

	public AddLocationPipeline(Pipeline p, BQTableUtils bqUtils) {
		super(p);
		this.bqUtils = bqUtils;
		this.setCountryCodeTable(COUNTRY_CODE_TABLE);
		this.setRegionCodeTable(REGION_CODE_TABLE);
	}

	public String getCountryCodeTable() {
		return countryCodeTable;
	}

	public void setCountryCodeTable(String countryCodeTable) {
		this.countryCodeTable = this.bqUtils.wrapTable(countryCodeTable);
	}

	public String getRegionCodeTable() {
		return regionCodeTable;
	}

	public void setRegionCodeTable(String regionCodeTable) {
		this.regionCodeTable = this.bqUtils.wrapTable(regionCodeTable);
	}
	
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		PCollection<TableRow> countryCodes = this.pipeline.apply(
			BigQueryIO.Read
				.named("Read " + this.getCountryCodeTable())
				.from(this.getCountryCodeTable()));

		PCollection<TableRow> regionCodes = this.pipeline.apply(
				BigQueryIO.Read
					.named("Read " + this.getRegionCodeTable())
					.from(this.getRegionCodeTable()));

		PCollection<KV<String, TableRow>> countryKeys =
				countryCodes.apply(ParDo.named("Extract Country Keys")
						.of(new ExtractCountryCodeFn()));

		PCollection<KV<String, TableRow>> regionKeys =
				regionCodes.apply(ParDo.named("Extract Region Keys")
						.of(new ExtractRegionCodeFn()));


		PCollectionView<Map<String, TableRow>> countryMap = countryKeys.apply(View.asMap());
		PCollectionView<Map<String, TableRow>> regionMap = regionKeys.apply(View.asMap());

		// resolve locations
		PCollection<TableRow> withLocations = data.apply(
			ParDo
				.named("Add country and region names")
				.withSideInputs(countryMap, regionMap)
				.of(new AddLocationNamesFn(countryMap, regionMap)));

		return withLocations;
	}

	public static void main(String [] args) throws ClassNotFoundException {


		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigQueryOptions.class);
		options.setAppName("AddLocations");

		// pipeline object
		Pipeline p = Pipeline.create(options);

		HistoricPipelineOptions optionsLocation = options.cloneAs(HistoricPipelineOptions.class);
		BQTableUtils bqUtils = new BQTableUtils(optionsLocation);
		
		AddLocationPipeline addLocations = new AddLocationPipeline(p, bqUtils);
		addLocations
			.setWriteData(true)
			.setOutputTable(bqUtils.wrapTable(OUTPUT_TABLE))
		    .setInputTable(bqUtils.wrapTable(INPUT_TABLE))
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addLocations.apply();

		p.run();
	}

	
}
