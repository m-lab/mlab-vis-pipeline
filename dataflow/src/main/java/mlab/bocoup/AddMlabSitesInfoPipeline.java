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
import mlab.bocoup.dofn.AddMlabSitesInfoFn;
import mlab.bocoup.transform.CombineAsNavigableMapHex;
import mlab.bocoup.util.Schema;


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
	private static final Logger LOG = LoggerFactory.getLogger(AddMlabSitesInfoPipeline.class);
	private static final String MLAB_SITES_TABLE = "mlab-oti:bocoup.mlab_sites";

	//for running main()
	private static final String INPUT_TABLE = "mlab-oti:bocoup.peter_test";
	private static final String OUTPUT_TABLE = "mlab-oti:bocoup.peter_test_out";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	private String mlabSitesTable = MLAB_SITES_TABLE;

	/**
	 * Create a new pipeline.
	 * @param p The Dataflow pipeline to build on
	 */
	public AddMlabSitesInfoPipeline(Pipeline p) {
		super(p);
	}

	/**
	 * Adds in ISP information for the client.
	 *
	 * @param p The Dataflow Pipeline
	 * @param byIpData The PCollection representing the rows to have ISP information added to them
	 * @return A PCollection of rows with ISP information added to them
	 */
	public PCollection<TableRow> addMlabSiteInfo(PCollection<TableRow> byIpData, PCollectionView<NavigableMap<String, TableRow>> mlabSitesView) {

		// Use the side-loaded MaxMind ISP data to get server ISPs
		PCollection<TableRow> byIpDataWithISPs = byIpData.apply(
				ParDo
				.named("Add MLab Sites Info")
				.withSideInputs(mlabSitesView)
				.of(new AddMlabSitesInfoFn(mlabSitesView)));
		return byIpDataWithISPs;
	}

	/**
	 * Adds necessary configuration to a Pipeline for adding ISPs to work.
	 *
	 * @param p The pipeline being used
	 * @return The pipeline being used (for convenience)
	 */
	@Override
	public void preparePipeline() {
		// needed to use NavigableMap as an output/accumulator for adding ISPs efficiently
		this.pipeline.getCoderRegistry().registerCoder(NavigableMap.class, NavigableMapCoder.class);
	}

	/**
	 * Add in the steps to add MLab site info to a given pipeline. Writes the
	 * data to a table if writeData field is true.
	 *
	 * @param p A pipeline to add the steps to. If null, a new pipeline is created.
	 * @param byIpData If provided, ISPs are added to this collection, otherwise they are read
	 * in from a BigQuery table.
	 *
	 * @return The PCollection with MLab site info added
	 */
	@Override
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {   
		// Read in the MaxMind ISP data
		PCollection<TableRow> mlabSites = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.mlabSitesTable)
				.from(this.mlabSitesTable));

		//Make the MaxMind ISP data ready for side input
		PCollectionView<NavigableMap<String, TableRow>> mlabSitesView =
				mlabSites
				.apply(Combine.globally(new CombineAsNavigableMapHex())
						.named("Combine ISPs as NavMap")
						// make a view so it can be used as side input
						.asSingletonView());


		// add in ISPs
		data = this.addMlabSiteInfo(data, mlabSitesView);

		return data;
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

		AddMlabSitesInfoPipeline addMlabSiteInfo = new AddMlabSitesInfoPipeline(p);
		addMlabSiteInfo
		.setWriteData(true)
		.setInputTable(INPUT_TABLE)
		.setOutputTable(OUTPUT_TABLE)
		.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
		.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
		.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addMlabSiteInfo.apply();

		p.run();
	}
}
