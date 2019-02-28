package mlab.dataviz.pipelineopts;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface UpdatePipelineOptions extends PipelineOptions, BigQueryOptions {
	@Description("Time period, options are: 'day' or 'hour'")
	@Default.String("sample")
	String getTimePeriod();
	void setTimePeriod(String timePeriod);
	
	@Description("Which M-Lab Project")
	@Default.String("mlab-sandbox")
	String getProject();
	void setProject(String project);
	
}
