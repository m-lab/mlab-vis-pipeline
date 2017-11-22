package mlab.dataviz.pipelines;

import java.io.IOException;
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

import mlab.dataviz.dofn.TableRowToHBase;
import mlab.dataviz.pipelineopts.BigtableTransferPipelineOptions;
import mlab.dataviz.query.BigQueryJob;
import mlab.dataviz.query.QueryBuilder;
import mlab.dataviz.util.bigtable.BigtableConfig;

/**
 * Executes a single big table read and write based on a configuration file
 * and schema.
 */
public class BigtablePipeline implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BigtablePipeline.class);

    private BigtableConfig btConfig;
    private BigtableTransferPipelineOptions options;
    private Pipeline pipe;

    /**
     * @constructor
     * Creates a new bigtable pipeline
     * @param args command line arguments
     * @param configFileName the name of the table config rul
     */
    public BigtablePipeline(String[] args, String configFileName) {

        // setup options
        PipelineOptionsFactory.register(BigtableTransferPipelineOptions.class);
        this.options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(BigtableTransferPipelineOptions.class);

        // read config file
        this.btConfig = BigtableConfig.fromJSONFile(configFileName);

        // update application name
        this.options.setAppName("viz-bigtable-build-" + btConfig.getBigtableTable());

        // create pipeline
        this.pipe = Pipeline.create(this.options);
    }

    /**
     * @constructor
     *
     * Creates a new bigtable pipeline, reusing a global pipeline.
     * @param p
     * @param configFileName
     */
    public BigtablePipeline(Pipeline p, String configFileName) {
    		this.pipe = p;

    		// read config file
        this.btConfig = BigtableConfig.fromJSONFile(configFileName);

        this.options = (BigtableTransferPipelineOptions) this.pipe.getOptions();
    }

    /**
     * Return the query to be run based on the configuration file.
     * It replaces any parameters as needed, such as the table to be read.
     */
    private String getQueryString() throws IOException {
        String queryString = btConfig.getBigQueryQuery();

        // TODO: move this to the bigtable config class?
        if (queryString == null) {
            LOG.info("Using Queryfile");
            Object[] queryParams = { "[" + this.options.getProject() + ":" + btConfig.getBigQueryTable() + "]" };
            QueryBuilder qb = new QueryBuilder(btConfig.getBigQueryQueryFile(), queryParams);
            queryString = qb.getQuery();
        }

        return queryString;
    }

    public boolean doesSourceTableExist() throws IOException {
    		// check to see if table exists. If it doesn't, don't run.
        // only relevant the first time.
        String query = "SELECT size_bytes FROM data_viz_helpers.__TABLES__ WHERE table_id='" + this.btConfig.getBigQueryTable() + "'";
        
        BigQueryJob bqj = new BigQueryJob(this.options.getProject());
        java.util.List<TableRow> results = bqj.executeQuery(query);
        
        if (results == null) {
        		return false;
        } else {
        		return true;
        }
    }
    
    @Override
    public void run() {
        try {
            String queryString = this.getQueryString();
            
            if (!doesSourceTableExist()) {
            		LOG.info("Source table not created yet. Skipping " + this.btConfig.getBigtableTable());
            } else {
	            LOG.info("Reading data from BigQuery query");
	            // Formally called this.pipeline.apply.
	            PCollection<TableRow> bigQueryCollection = pipe
	                    .apply(BigQueryIO.Read.named(btConfig.getBigtableTable() + " BQ Read").fromQuery(queryString));
	
	            CloudBigtableIO.initializeForWrite(pipe);
	
	            LOG.info("Transferring from BigQuery to Bigtable");
	            LOG.info("Bigtable Project ID: " + this.options.getProject());
	            LOG.info("Bigtable Instance ID: " + this.options.getInstance());
	            LOG.info("Bigtable Table: " + btConfig.getBigtableTable());
	
	            // create configuration for writing to the Bigtable
	            CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
	                    .withProjectId(this.options.getProject())
	                    .withInstanceId(this.options.getInstance())
	                    .withTableId(btConfig.getBigtableTable()).build();
	
	            // convert from TableRow objects to hbase compatible mutations (Put)
	            PCollection<Mutation> hbasePuts = bigQueryCollection.apply(
	                    ParDo.named(btConfig.getBigtableTable() + " BT Transform")
	                    .of(new TableRowToHBase(btConfig)));
	
	            // write the mutations to Bigtable
	            hbasePuts.apply(CloudBigtableIO.writeToTable(config));
	
	            DataflowPipelineJob result = (DataflowPipelineJob) this.pipe.run();
	            try {
	                result.waitToFinish(-1, TimeUnit.MINUTES, new MonitoringUtil.PrintHandler(System.out));
	            } catch (InterruptedException e) {
	                LOG.error(e.getMessage());
	                e.printStackTrace();
	            }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }

    }
}