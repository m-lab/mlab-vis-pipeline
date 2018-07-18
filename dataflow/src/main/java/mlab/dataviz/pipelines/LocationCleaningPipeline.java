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

import mlab.dataviz.dofn.CleanLocationFn;
import mlab.dataviz.dofn.ExtractLocationKeyFn;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.Schema;

public class LocationCleaningPipeline extends BasePipeline {
	private static final Logger LOG = LoggerFactory.getLogger(LocationCleaningPipeline.class);

	// default table name
	private static final String LOCATION_CLEANING_TABLE = "data_viz_helpers.location_cleaning";

	// for running main()
	private static final String INPUT_TABLE = "data_viz_testing.test_location_clean_input";
	private static final String OUTPUT_TABLE = "data_viz_testing.test_location_clean_output";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private BQTableUtils bqUtils;
	private String locationCleaningTable;

	public LocationCleaningPipeline(Pipeline p, BQTableUtils bqUtils) {
		super(p);
		this.bqUtils = bqUtils;
		this.setLocationCleaningTable(LOCATION_CLEANING_TABLE);
	}

	/**
	 * Get the table name to write to
	 * @return String
	 */
	public String getLocationCleaningTable() {
		return this.locationCleaningTable;
	}
	
	/**
	 * Set the location table name
	 * @param locationCleaningTable
	 */
	public void setLocationCleaningTable(String locationCleaningTable) {
		this.locationCleaningTable = this.bqUtils.wrapTable(locationCleaningTable);
	}
	
	/**
	 * Reads in the location_cleaning table as a side input and runs CleanLocationFn with it to replace
	 * cities and region codes for locations that match.
	 */
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		PCollection<TableRow> locationCleaning = this.pipeline.apply(
			BigQueryIO.Read
				.named("Read " + this.getLocationCleaningTable())
				.from(this.getLocationCleaningTable()));

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
		
		HistoricPipelineOptions optionsLocationClean = options.cloneAs(HistoricPipelineOptions.class);
		BQTableUtils bqUtils = new BQTableUtils(optionsLocationClean);

		LocationCleaningPipeline addLocations = new LocationCleaningPipeline(p, bqUtils);
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
