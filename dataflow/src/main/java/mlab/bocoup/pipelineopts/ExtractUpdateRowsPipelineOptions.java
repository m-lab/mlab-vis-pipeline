package mlab.bocoup.pipelineopts;

import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface ExtractUpdateRowsPipelineOptions extends PipelineOptions, BigQueryOptions {

	@Description("Time period, options are: 'day' or 'hour'")
	@Default.String("sample")
	String getTimePeriod();
	void setTimePeriod(String timePeriod);
}
