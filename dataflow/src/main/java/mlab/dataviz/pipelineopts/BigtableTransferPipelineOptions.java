package mlab.dataviz.pipelineopts;

import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface BigtableTransferPipelineOptions extends PipelineOptions, BigQueryOptions {

	@Description("Which M-Lab Project")
	@Default.String("mlab-sandbox")
	String getProject();
	void setProject(String project);
	
	@Description("Which Bigtable instance to write to?")
	@Default.String("mlab-data-viz")
	String getInstance();
	void setInstance(String instance);
	
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
