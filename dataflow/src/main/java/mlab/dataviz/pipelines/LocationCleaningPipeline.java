package mlab.dataviz.pipelines;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.dofn.CleanLocationFn;
import mlab.dataviz.dofn.ExtractLocationKeyFn;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.Schema;

public class LocationCleaningPipeline extends BasePipeline {

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
	 * 
	 * @return String
	 */
	public String getLocationCleaningTable() {
		return this.locationCleaningTable;
	}

	/**
	 * Set the location table name
	 * 
	 * @param locationCleaningTable
	 */
	public void setLocationCleaningTable(String locationCleaningTable) {
		this.locationCleaningTable = this.bqUtils.wrapTable(locationCleaningTable);
	}

	/**
	 * Reads in the location_cleaning table as a side input and runs CleanLocationFn
	 * with it to replace cities and region codes for locations that match.
	 */
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		PCollection<TableRow> locationCleaning = this.pipeline.apply("Read " + this.getLocationCleaningTable(),
				BigQueryIO.readTableRows().from(this.getLocationCleaningTable()));

		PCollection<KV<String, TableRow>> locationKeys = locationCleaning.apply("Extract Location Keys",
				ParDo.of(new ExtractLocationKeyFn()));

		PCollectionView<Map<String, TableRow>> locationMap = locationKeys.apply(View.asMap());

		// resolve locations
		PCollection<TableRow> withLocations = data.apply("Clean locations",
				ParDo.of(new CleanLocationFn(locationMap)).withSideInputs(locationMap));

		return withLocations;
	}

	public static void main(String[] args) throws ClassNotFoundException {
		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryOptions.class);
		options.setAppName("LocationCleaning");

		// pipeline object
		Pipeline p = Pipeline.create(options);

		HistoricPipelineOptions optionsLocationClean = options.as(HistoricPipelineOptions.class);
		BQTableUtils bqUtils = new BQTableUtils(optionsLocationClean);

		LocationCleaningPipeline addLocations = new LocationCleaningPipeline(p, bqUtils);
		addLocations.setWriteData(true).setOutputTable(bqUtils.wrapTable(OUTPUT_TABLE))
				.setInputTable(bqUtils.wrapTable(INPUT_TABLE)).setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
				.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
				.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addLocations.apply();

		p.run();
	}
}
