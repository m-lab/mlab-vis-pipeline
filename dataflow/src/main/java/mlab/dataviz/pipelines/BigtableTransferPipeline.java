package mlab.dataviz.pipelines;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PCollection;

import io.prometheus.client.Gauge;
import mlab.dataviz.dofn.TableRowToHBase;
import mlab.dataviz.entities.BTPipelineRun;
import mlab.dataviz.entities.BTPipelineRunDatastore;
import mlab.dataviz.pipelineopts.BigtableTransferPipelineOptions;
import mlab.dataviz.query.QueryBuilder;
import mlab.dataviz.util.Formatters;
import mlab.dataviz.util.bigtable.BigtableConfig;



public class BigtableTransferPipeline implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(BigtableTransferPipeline.class);
	private static final String BIGTABLE_CONFIG_DIR = "./data/bigtable/";

	// defaults
	private static final String DEFAULT_PROJECT_ID = "mlab-sandbox";
	private static final String DEFAULT_INSTANCE_ID = "mlab-data-viz";

	private BTPipelineRun status;
	private BTPipelineRunDatastore datastore = null;
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP);
	
	private Pipeline pipeline;
	private BigtableConfig btConfig;
	private String projectId = DEFAULT_PROJECT_ID;
	private String instanceId = DEFAULT_INSTANCE_ID;
	private Boolean pipelinePrepared = false;
	private String[] args;
	
	/**
	 * Create a container for the pipeline that will then be run from a metrics server.
	 * It's a bit of a nested doll situation. This is just the container that will
	 * then spawn the actual pipelines.
	 * @param args
	 */
	public BigtableTransferPipeline(String []args) {
		this.args = args;
		
		try {
			this.datastore = new BTPipelineRunDatastore();
		 } catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		 }
	}
	
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
	 * Creates a datastore record of this pipeline run.
	 * Initializes its start date and run status and write it to the store.
	 * @return VizPipelineRun record.
	 * @throws SQLException
	 */
	private BTPipelineRun createRunRecord() throws SQLException {
		Calendar c = Calendar.getInstance();
		Date time = c.getTime();
		String runDate = dateFormatter.format(time);

		BTPipelineRun record = new BTPipelineRun.Builder()
				.run_start_date(runDate)
				.status(BTPipelineRun.STATUS_RUNNING)
				.build();

		// write it to the datastore
		long id = this.datastore.createBTPipelineRunEntity(record);
		record.setId(id);
		return record;
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
		LOG.info("Bigtable Project ID: " + this.getProjectId());
		LOG.info("Bigtable Instance ID: " + this.getInstanceId());
		LOG.info("Bigtable Table: " + btConfig.getBigtableTable());

		// create configuration for writing to the Bigtable
		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
			    .withProjectId(this.getProjectId())
			    .withInstanceId(this.getInstanceId())
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
			Object[] queryParams = {"[" + this.getProjectId() + ":" + btConfig.getBigQueryTable() + "]"};
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
						this.apply(btConfig);
					} catch (IOException e) {
						LOG.error(e.getMessage());
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
	public static DataflowPipelineJob runAll(BigtableTransferPipelineOptions options) {

		Pipeline pipe = Pipeline.create(options);
		BigtableTransferPipeline transferPipeline = new BigtableTransferPipeline(pipe);

		transferPipeline.setProjectId(options.getProject());
		transferPipeline.setInstanceId(options.getInstance());

		int test = options.getTest();

		String configPrefix = options.getConfigPrefix();
		String configSuffix = options.getConfigSuffix();

		try {
			transferPipeline.applyAll(BIGTABLE_CONFIG_DIR, configPrefix, configSuffix);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace().toString());
		}

		if (test != 1) {
			DataflowPipelineJob result = (DataflowPipelineJob) pipe.run();
			return result;
		}
		return null;
	}


	/**
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		BigtableTransferPipeline btp = new BigtableTransferPipeline(args);
		btp.run();
	}

	@Override
	public void run() {
		
		// Prometheus, duration tracking
		Gauge duration = Gauge.build().name("mlab_vis_pipeline_historic_bigtable_duration")
				.help("Historic pipeline duration - Bigtable").register();

		Gauge.Timer durationTimer = duration.startTimer();

		PipelineOptionsFactory.register(BigtableTransferPipelineOptions.class);
		BigtableTransferPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigtableTransferPipelineOptions.class);
		
		try {
			this.status = createRunRecord();
			DataflowPipelineJob result = BigtableTransferPipeline.runAll(options);
	
			if (result != null) {
				try {
					// Wait for Bigtable pipeline to finish
					result.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));
					durationTimer.setDuration();
					this.status.setStatus(BTPipelineRun.STATUS_DONE);
					this.datastore.updateBTPipelineRunEntity(this.status);
				} catch (IOException | InterruptedException e) {
					LOG.error(e.getMessage());
					e.printStackTrace();
					this.status.setStatus(BTPipelineRun.STATUS_FAILED);
					this.datastore.updateBTPipelineRunEntity(this.status);
				}
			}
		} catch (SQLException e1) {
			LOG.error(e1.getMessage());
			e1.printStackTrace();			
		}

	}
}