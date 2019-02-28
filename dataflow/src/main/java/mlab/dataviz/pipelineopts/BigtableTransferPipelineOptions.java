package mlab.dataviz.pipelineopts;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface BigtableTransferPipelineOptions extends DataflowPipelineOptions, BigQueryOptions {
    
    @Description("Which Bigtable instance to write to?")
    @Default.String("mlab-data-viz")
    String getInstance();
    
    void setInstance(String instance);
    
    @Description("Which Prometheus Instance")
    @Default.String("prometheus")
    String getPrometheus();
    
    void setPrometheus(String prometheus);
    
    @Description("Do not actually execute the pipeline")
    @Default.Integer(0)
    int getTest();
    
    void setTest(int test);
    
    @Description("Prefix for config scripts to read")
    @Default.String("")
    String getConfigPrefix();
    
    void setConfigPrefix(String configPrefix);
    
    @Description("Suffix for config scripts to read")
    @Default.String(".json")
    String getConfigSuffix();
    
    void setConfigSuffix(String configSuffix);
    
}
