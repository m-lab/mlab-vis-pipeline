package mlab.dataviz.pipelines;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import mlab.dataviz.entities.BTPipelineRun;
import mlab.dataviz.entities.BTPipelineRunDatastore;
import mlab.dataviz.pipelineopts.BigtableTransferPipelineOptions;
import mlab.dataviz.util.Formatters;

/**
 * Runs in parallel all the big table transfers based on the json files
 * that live inside the data/bigtable folder.
 */
public class BigtableTransferPipeline implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(BigtableTransferPipeline.class);
    private static final String BIGTABLE_CONFIG_DIR = "./data/bigtable/";

    private String[] args;
    private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP);
    private BTPipelineRun status;
    private BTPipelineRunDatastore datastore = null;

    /**
     * @constructor
     * Creates a new big table transfer pipeline
     * @param args Command line arguments
     */
    public BigtableTransferPipeline(String[] args) {
        this.args = args;
        try {
            this.datastore = new BTPipelineRunDatastore();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
    * Returns a timestamp of the current time.
    * @return String of the timestamp.
    */
    private String getNowTimestamp() {
        Calendar c = Calendar.getInstance();
        Date time = c.getTime();
        return dateFormatter.format(time);
    }


    /**
    * Creates a datastore record of this pipeline run.
    * Initializes its start date and run status and write it to the store.
    * @return VizPipelineRun record.
    * @throws SQLException
    */
    private BTPipelineRun createRunRecord() throws SQLException {
        BTPipelineRun record = new BTPipelineRun.Builder().datastore(this.datastore)
                .run_start_date(this.getNowTimestamp()).status(BTPipelineRun.STATUS_RUNNING).build();

        // write it to the datastore
        long id = this.datastore.createBTPipelineRunEntity(record);
        record.setId(id);
        return record;
    }

    @Override
    public void run() {
        try {
            this.status = createRunRecord();

            PipelineOptionsFactory.register(BigtableTransferPipelineOptions.class);
            BigtableTransferPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation()
                .as(BigtableTransferPipelineOptions.class);

            int test = options.getTest();
            String configPrefix = options.getConfigPrefix();
            String configSuffix = options.getConfigSuffix();

            File folder = new File(BIGTABLE_CONFIG_DIR);
            File[] listOfFiles = folder.listFiles();

            if (configSuffix.length() == 0) {
                configSuffix = ".json";
            }

            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (File f : listOfFiles) {
                if (f.isFile() && f.getName().endsWith(configSuffix)) {
                    if(configPrefix.length() == 0 || f.getName().startsWith(configPrefix)) {
                        String configFilename = BIGTABLE_CONFIG_DIR + f.getName();
                        LOG.debug("Running bigtable transfer for file: " + configFilename);
                        BigtablePipeline btPipeline = new BigtablePipeline(this.args, configFilename);
                        if (test == 0) {
                            Thread btPipeThread = new Thread(btPipeline);
                            btPipeThread.start();
                            threads.add(btPipeThread);
                        } else {
                            LOG.info("Test mode, not running pipeline for " + configFilename);
                        }
                    }
                }
            }

            // once all jobs have been queued up, wait for them all
            if (test == 0) {
                for (Thread t : threads) {
                    try {
                        t.join();
                    } catch (InterruptedException ex) {
                        LOG.error(ex.getMessage());
                        ex.printStackTrace();
                    }
                }
            }

            // once we get here, we are complete with all runs. Write status and exit.
            this.status.setRunEndDate(this.getNowTimestamp());
            this.status.save();
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}