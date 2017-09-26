package mlab.dataviz.pipelineopts;

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
	
	@Description("Do not actually execute the pipeline")
	@Default.Integer(0)
	int getTest();
	void setTest(int test);
	
	@Description("Which M-Lab Project")
	@Default.String("mlab-sandbox")
	String getProject();
	void setProject(String project);
	
	@Description("Start date of historic pipeline. format: YYYY-MM-DD")
	@Default.String("2009-01-01")
	String getStartDate();
	void setStartDate(String startDate);
	
	@Description("End date of historic pipeline. format: YYYY-MM-DD")
	@Default.String("2016-10-01")
	String getEndDate();
	void setEndDate(String endDate);
	
}
