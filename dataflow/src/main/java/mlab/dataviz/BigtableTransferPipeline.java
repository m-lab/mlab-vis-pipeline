package mlab.dataviz;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import mlab.dataviz.dofn.TableRowToHBase;
import mlab.dataviz.pipelineopts.BigtableTransferPipelineOptions;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.query.QueryBuilder;
import mlab.dataviz.util.bigtable.BigtableConfig;



public class BigtableTransferPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(BigtableTransferPipeline.class);

	// This file is used when the class is run as a program (in the main method)
	private static final String DEFAULT_BIGTABLE_CONFIG_FILE = "./data/bigtable/client_loc_search.json";
	private static final String BIGTABLE_CONFIG_DIR = "./data/bigtable/";

	private static final String DEFAULT_PROJECT_ID = "mlab-oti";
	private static final String DEFAULT_INSTANCE_ID = "mlab-data-viz-prod";


	private Pipeline pipeline;
	private BigtableConfig btConfig;
	private String projectId = DEFAULT_PROJECT_ID;
	private String instanceId = DEFAULT_INSTANCE_ID;
	private Boolean pipelinePrepared = false;

	/**
	 * Create the pipeline with a Dataflow pieline and a BigtableConfig object
	 * @param pipeline The Dataflow pipeline to run on
	 * @param btConfig The configuration for the Bigtable input and output
	 */
	public BigtableTransferPipeline(Pipeline pipeline, BigtableConfig btConfig) {
		this.pipeline = pipeline;
		this.btConfig = btConfig;
	}

	/**
	 * Create the pipeline with a Dataflow pieline and a BigtableConfig object
	 * @param pipeline The Dataflow pipeline to run on
	 * @param btConfig The configuration for the Bigtable input and output
	 */
	public BigtableTransferPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
	}

	/**
	 * Create the pipeline with a Dataflow pieline and a filename to a BigtableConfig json
	 * @param pipeline The Dataflow pipeline to run on
	 * @param btConfigFile The JSON filename of the configuration for the Bigtable input and output
	 */
	public BigtableTransferPipeline(Pipeline pipeline, String btConfigFile) {
		this.pipeline = pipeline;
		this.btConfig = BigtableConfig.fromJSONFile(btConfigFile);
	}

	/**
	 * Prepare the pipeline before running anything on it. Enable settings for
	 * Bigtable writing.
	 */
	public void preparePipeline() {
		// Before you write to Cloud Bigtable, you must create and initialize your
		// Cloud Dataflow pipeline so that puts and deletes can be serialized over
		// the network:
		if(!pipelinePrepared) {
			CloudBigtableIO.initializeForWrite(this.pipeline);
			pipelinePrepared = true;
		}
	}

	/**
	 * Transfer the data provided in byIpData to Bigtable
	 * @param bigQueryCollection The BigQuery TableRows representing input data
	 * @return The PCollection of hbase mutations used
	 */
	public PCollection<Mutation> apply(PCollection<TableRow> bigQueryCollection, BigtableConfig btConfig) {
		this.preparePipeline();

		LOG.info("Transferring from BigQuery to Bigtable");
		LOG.info("Bigtable Project ID: " + this.projectId);
		LOG.info("Bigtable Instance ID: " + this.instanceId);
		LOG.info("Bigtable Table: " + btConfig.getBigtableTable());

		// create configuration for writing to the Bigtable
		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
			    .withProjectId(this.projectId)
			    .withInstanceId(this.instanceId)
			    .withTableId(btConfig.getBigtableTable())
			    .build();

		// convert from TableRow objects to hbase compatible mutations (Put)
		PCollection<Mutation> hbasePuts = bigQueryCollection
			.apply(ParDo
					.named(btConfig.getBigtableTable() + " BT Transform")
					.of(new TableRowToHBase(btConfig)));

		// write the mutations to Bigtable
		hbasePuts.apply(CloudBigtableIO.writeToTable(config));

		return hbasePuts;
	}

	private String getQueryString(BigtableConfig btConfig) throws IOException {
		String queryString = btConfig.getBigQueryQuery();

		// TODO: move this to the bigtable config class?
		if(queryString == null) {
			LOG.info("Using Queryfile");
			Object[] queryParams = {btConfig.getBigQueryTable()};
			QueryBuilder qb = new QueryBuilder(btConfig.getBigQueryQueryFile(), queryParams);
			queryString = qb.getQuery();
		}

		return queryString;
	}

	/**
	 * Transfer the data provided in byIpData to Bigtable. Reads in from the query
	 * specified in the BigtableConfig object.
	 *
	 * @return The PCollection of hbase mutations used
	 * @throws IOException
	 */
	public PCollection<Mutation> apply() throws IOException {

		String queryString = this.getQueryString(this.btConfig);

		// Read from BigQuery based on the query in BigtableConfig
		LOG.info("Reading data from BigQuery query");
		PCollection<TableRow> bigQueryCollection = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read from BigQuery")
				.fromQuery(queryString));

		return this.apply(bigQueryCollection, this.btConfig);
	}

	/**
	 * Transfer the data provided in byIpData to Bigtable. Reads in from the query
	 * specified in the BigtableConfig object.
	 *
	 * @return The PCollection of hbase mutations used
	 * @throws IOException
	 */
	public PCollection<Mutation> apply(BigtableConfig btConfig) throws IOException {

		String queryString = this.getQueryString(btConfig);

		LOG.info("Reading data from BigQuery query");
		PCollection<TableRow> bigQueryCollection = this.pipeline.apply(
				BigQueryIO.Read
				.named(btConfig.getBigtableTable() + " BQ Read")
				.fromQuery(queryString));

		return this.apply(bigQueryCollection, btConfig);
	}

	public void applyAll(String btConfigDir, String configPrefix, String configSuffix) throws IOException {
		this.preparePipeline();
		File folder = new File(btConfigDir);
		File[] listOfFiles = folder.listFiles();

		if(configSuffix.length() == 0) {
			configSuffix = ".json";
		}

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile() && listOfFiles[i].getName().endsWith(configSuffix)) {
				if(configPrefix.length() == 0 || listOfFiles[i].getName().startsWith(configPrefix)) {
					String configFilename = btConfigDir + listOfFiles[i].getName();
					LOG.debug("Setup - config file: " + configFilename);
					System.out.println("config file" +  configFilename);
					BigtableConfig btConfig = BigtableConfig.fromJSONFile(configFilename);



					try {
						PCollection out = this.apply(btConfig);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} // end prefix check


			}
		}
	}

	/**
	 *
	 * @return
	 */
	public Pipeline getPipeline() {
		return pipeline;
	}

	/**
	 *
	 * @param pipeline
	 * @return
	 */
	public BigtableTransferPipeline setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
		return this;
	}

	/**
	 *
	 * @return
	 */
	public BigtableConfig getBtConfig() {
		return btConfig;
	}

	/**
	 *
	 * @param btConfig
	 * @return
	 */
	public BigtableTransferPipeline setBtConfig(BigtableConfig btConfig) {
		this.btConfig = btConfig;
		return this;
	}

	/**
	 *
	 * @return
	 */
	public String getProjectId() {
		return projectId;
	}

	/**
	 *
	 * @param projectId
	 * @return
	 */
	public BigtableTransferPipeline setProjectId(String projectId) {
		this.projectId = projectId;
		return this;
	}

	/**
	 *
	 * @return
	 */
	public String getInstanceId() {
		return instanceId;
	}

	/**
	 *
	 * @param instanceId
	 * @return
	 */
	public BigtableTransferPipeline setInstanceId(String instanceId) {
		this.instanceId = instanceId;
		return this;
	}

	/**
	 *
	 * @param args
	 */
	public static void runAll(BigtableTransferPipelineOptions options) {

		Pipeline pipe = Pipeline.create(options);
		BigtableTransferPipeline transferPipeline = new BigtableTransferPipeline(pipe);

		int test = options.getTest();

		String configPrefix = options.getConfigPrefix();
		String configSuffix = options.getConfigSuffix();

		try {
			transferPipeline.applyAll(BIGTABLE_CONFIG_DIR, configPrefix, configSuffix);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (test != 1) {
			pipe.run();
		}
	}

	/**
	 *
	 * @param args
	 */
	public static void runOne(String[] args, String configFile) {

		BigtableConfig btConfig = BigtableConfig.fromJSONFile(configFile);

		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigQueryOptions.class);

		Pipeline pipe = Pipeline.create(options);
		BigtableTransferPipeline transferPipeline = new BigtableTransferPipeline(pipe);

		try {
			transferPipeline.apply(btConfig);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		pipe.run();
	}


	/**
	 *
	 * @param args
	 */
	public static void main(String[] args) {

		PipelineOptionsFactory.register(BigtableTransferPipelineOptions.class);
		BigtableTransferPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(BigtableTransferPipelineOptions.class);

		BigtableTransferPipeline.runAll(options);

		//BigtableTransferPipeline.runOne(args, DEFAULT_BIGTABLE_CONFIG_FILE);

	}
}