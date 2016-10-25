package mlab.bocoup;

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

import mlab.bocoup.dofn.AddLocationNamesFn;
import mlab.bocoup.dofn.ExtractCountryCodeFn;
import mlab.bocoup.dofn.ExtractRegionCodeFn;
import mlab.bocoup.util.Schema;

public class AddLocationPipeline extends BasePipeline {
	private static final Logger LOG = LoggerFactory.getLogger(AddLocationPipeline.class);
	
	private static final String COUNTRY_CODE_TABLE = "bocoup.location_country_codes";
	private static final String REGION_CODE_TABLE = "bocoup.location_region_codes";
	
	// for running main()
	private static final String INPUT_TABLE = "mlab-oti:bocoup.zz4_all_isp_by_day";
	private static final String OUTPUT_TABLE = "mlab-oti:bocoup.zz4_all_ip_by_day_with_locations";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";
	
	public AddLocationPipeline(Pipeline p) {
		super(p);
	}
	
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		PCollection<TableRow> countryCodes = this.pipeline.apply(
			BigQueryIO.Read
				.named("Read " + COUNTRY_CODE_TABLE)
				.from(COUNTRY_CODE_TABLE));
		
		PCollection<TableRow> regionCodes = this.pipeline.apply(
				BigQueryIO.Read
					.named("Read " + REGION_CODE_TABLE)
					.from(REGION_CODE_TABLE));
		
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
		
		AddLocationPipeline addLocations = new AddLocationPipeline(p);
		addLocations
			.setWriteData(true)
			.setOutputTable(OUTPUT_TABLE)
		    .setInputTable(INPUT_TABLE)
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
		
		addLocations.apply();
		
		p.run();
	}
}
