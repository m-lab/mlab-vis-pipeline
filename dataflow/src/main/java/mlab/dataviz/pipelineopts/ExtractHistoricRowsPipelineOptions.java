package mlab.dataviz.pipelineopts;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ExtractHistoricRowsPipelineOptions extends PipelineOptions, BigQueryOptions {

	@Description("Path to configuration file specifying the extraction routine")
	String getConfigfile();
	void setConfigfile(String configfile);
}
