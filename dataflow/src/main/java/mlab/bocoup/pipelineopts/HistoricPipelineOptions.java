package mlab.bocoup.pipelineopts;

import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface HistoricPipelineOptions extends PipelineOptions, BigQueryOptions {

	@Description("Time period, options are: 'sample', 'day' or 'hour'")
	@Default.String("sample")
	String getTimePeriod();
	void setTimePeriod(String timePeriod);
	
	@Description("Skip NDT Read: 0 to read, 1 to skip")
	@Default.Integer(1)
	int getSkipNDTRead();
	void setSkipNDTRead(int skipNDTRead);
}
