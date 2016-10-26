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

import mlab.bocoup.dofn.CleanLocationFn;
import mlab.bocoup.dofn.ExtractLocationKeyFn;
import mlab.bocoup.util.Schema;

public class LocationCleaningPipeline extends BasePipeline {
	private static final Logger LOG = LoggerFactory.getLogger(LocationCleaningPipeline.class);
	
	private static final String LOCATION_CLEANING_TABLE = "bocoup.location_cleaning";
	
	// for running main()
	private static final String INPUT_TABLE = "mlab-oti:bocoup.jim_test";
	private static final String OUTPUT_TABLE = "mlab-oti:bocoup.peter_test_out";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";
	
	public LocationCleaningPipeline(Pipeline p) {
		super(p);
	}
	
	/**
	 * Reads in the location_cleaning table as a side input and runs CleanLocationFn with it to replace
	 * cities and region codes for locations that match.
	 */
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		PCollection<TableRow> locationCleaning = this.pipeline.apply(
			BigQueryIO.Read
				.named("Read " + LOCATION_CLEANING_TABLE)
				.from(LOCATION_CLEANING_TABLE));
		
		PCollection<KV<String, TableRow>> locationKeys = 
				locationCleaning.apply(ParDo.named("Extract Location Keys")
						.of(new ExtractLocationKeyFn()));
		
		PCollectionView<Map<String, TableRow>> locationMap = locationKeys.apply(View.asMap());
		
		// resolve locations
		PCollection<TableRow> withLocations = data.apply(
			ParDo
				.named("Clean locations")
				.withSideInputs(locationMap)
				.of(new CleanLocationFn(locationMap)));
		
		return withLocations;
	}
	
	public static void main(String [] args) throws ClassNotFoundException {
		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigQueryOptions.class);
		options.setAppName("LocationCleaning");
		
		// pipeline object
		Pipeline p = Pipeline.create(options);
		
		LocationCleaningPipeline addLocations = new LocationCleaningPipeline(p);
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
