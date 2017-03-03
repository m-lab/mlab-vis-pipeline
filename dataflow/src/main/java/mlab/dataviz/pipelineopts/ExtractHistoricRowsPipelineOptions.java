package mlab.dataviz.pipelineopts;

import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface ExtractHistoricRowsPipelineOptions extends PipelineOptions, BigQueryOptions {

	@Description("Path to configuration file specifying the extraction routine")
	String getConfigfile();
	void setConfigfile(String configfile);
}
