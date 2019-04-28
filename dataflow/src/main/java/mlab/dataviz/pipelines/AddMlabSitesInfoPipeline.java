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

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.coder.NavigableMapCoder;
import mlab.dataviz.dofn.AddMlabSitesInfoFn;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.transform.CombineAsNavigableMapHex;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.Schema;

/**
 * Pipeline for overriding server locations based on the data provided from
 * https://mlab-ns.appspot.com/admin/sites
 *
 * Adds in mlab site ID, lat, long, city, region, country, continent
 *
 * @author pbeshai
 *
 */
public class AddMlabSitesInfoPipeline extends BasePipeline {
	private static final String MLAB_SITES_TABLE = "data_viz_helpers.mlab_sites";

	// for running main()
	private static final String INPUT_TABLE = "data_viz_testing.mlab_sites_test";
	private static final String OUTPUT_TABLE = "data_viz_testing.mlab_sites_test_output";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private BQTableUtils bqUtils;
	private String mlabSitesTable;

	/**
	 * Create a new pipeline.
	 * 
	 * @param p The Dataflow pipeline to build on
	 */
	public AddMlabSitesInfoPipeline(Pipeline p, BQTableUtils bqUtils) {
		super(p);
		this.bqUtils = bqUtils;
		this.setMlabSitesTable(MLAB_SITES_TABLE);
	}

	/**
	 * Adds in ISP information for the client.
	 *
	 * @param p        The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information
	 *                 added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addMlabSiteInfo(PCollection<TableRow> byIpData,
			PCollectionView<NavigableMap<String, TableRow>> mlabSitesView) {

		// Use the side-loaded MaxMind ISP data to get server ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply("Add MLab Sites Info",
				ParDo.of(new AddMlabSitesInfoFn(mlabSitesView)).withSideInputs(mlabSitesView));
		return byIpDataWithISPs;
	}

	public void setMlabSitesTable(String mlabSitesTable) {
		this.mlabSitesTable = this.bqUtils.wrapTable(mlabSitesTable);
	}

	public String getMlabSitesTable() {
		return this.mlabSitesTable;
	}

	/**
	 * Adds necessary configuration to a Pipeline for adding ISPs to work.
	 *
	 * @param p The pipeline being used
	 * @return The pipeline being used (for convenience)
	 */
	@Override
	public void preparePipeline() {
		// needed to use NavigableMap as an output/accumulator for adding ISPs
		// efficiently
		pipeline.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(NavigableMap.class, NavigableMapCoder.class));
	}

	/**
	 * Add in the steps to add MLab site info to a given pipeline. Writes the data
	 * to a table if writeData field is true.
	 *
	 * @param p        A pipeline to add the steps to. If null, a new pipeline is
	 *                 created.
	 * @param byIpData If provided, ISPs are added to this collection, otherwise
	 *                 they are read in from a BigQuery table.
	 *
	 * @return The PCollection with MLab site info added
	 */
	@Override
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		// Read in the MaxMind ISP data
		PCollection<TableRow> mlabSites = this.pipeline.apply("Read " + this.getMlabSitesTable(),
				BigQueryIO.readTableRows().from(this.getMlabSitesTable()));

		// Make the MaxMind ISP data ready for side input
		PCollectionView<NavigableMap<String, TableRow>> mlabSitesView = mlabSites.apply("Combine ISPs as NavMap",
				Combine.globally(new CombineAsNavigableMapHex())
						// make a view so it can be used as side input
						.asSingletonView());

		// add in ISPs
		data = this.addMlabSiteInfo(data, mlabSitesView);

		return data;
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

		HistoricPipelineOptions optionsMlabSites = options.as(HistoricPipelineOptions.class);
		BQTableUtils bqTableUtils = new BQTableUtils(optionsMlabSites);

		AddMlabSitesInfoPipeline addMlabSiteInfo = new AddMlabSitesInfoPipeline(p, bqTableUtils);
		addMlabSiteInfo.setWriteData(true).setInputTable(bqTableUtils.wrapTable(INPUT_TABLE))
				.setOutputTable(bqTableUtils.wrapTable(OUTPUT_TABLE))
				.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
				.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
				.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addMlabSiteInfo.apply();

		p.run();
	}
}
