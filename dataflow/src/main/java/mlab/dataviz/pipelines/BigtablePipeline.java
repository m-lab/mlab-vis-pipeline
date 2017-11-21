package mlab.dataviz.pipelines;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Mutation;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

import com.google.protobuf.ByteString;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableOptions.Builder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import mlab.dataviz.dofn.TableRowToHBase;
import mlab.dataviz.pipelineopts.BigtableTransferPipelineOptions;
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

    @Override
    public void run() {
        try {
            String queryString = this.getQueryString();

            LOG.info("Reading data from BigQuery query");
            
            // Formally called this.pipeline.apply.
            PCollection<TableRow> bigQueryCollection = this.pipe
                    .apply(BigQueryIO.read().fromQuery(queryString))
                    .setName(btConfig.getBigtableTable() + " BQ Read");

            BigtableOptions config =  new Builder()
                    .setProjectId(this.options.getProject())
                    .setInstanceId(this.options.getInstance())
                    .setUserAgent("viz-pipeline")
                    .build();
            
            LOG.info("Transferring from BigQuery to Bigtable");
            LOG.info("Bigtable Project ID: " + this.options.getProject());
            LOG.info("Bigtable Instance ID: " + this.options.getInstance());
            LOG.info("Bigtable Table: " + btConfig.getBigtableTable());

            // convert from TableRow objects to hbase compatible mutations
            PCollection<KV<ByteString, Iterable<com.google.bigtable.v2.Mutation>>> hbasePuts = bigQueryCollection
            		.apply(btConfig.getBigtableTable() + " BT Transform", 
                    ParDo
                    .of(new TableRowToHBase(btConfig)));
           
            // write the mutations to Bigtable
            hbasePuts.apply(BigtableIO.write()
            		.withBigtableOptions(config)
            		.withTableId(btConfig.getBigtableTable()));
            
            DataflowPipelineJob result = (DataflowPipelineJob) this.pipe.run();
            try {
            		Duration d = new Duration(-1);
            		result.waitUntilFinish(d, new MonitoringUtil.LoggingHandler());
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }

    }
}