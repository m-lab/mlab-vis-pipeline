package mlab.dataviz.pipelineopts;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ExtractUpdateRowsPipelineOptions extends PipelineOptions, BigQueryOptions {

	@Description("Time period, options are: 'day' or 'hour'")
	@Default.String("sample")
	String getTimePeriod();
	void setTimePeriod(String timePeriod);
}
