package mlab.dataviz.pipelineopts;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface AlivePingOptions extends PipelineOptions {

	@Description("Which M-Lab Project")
	@Default.String("mlab-sandbox")
	String getProject();
	void setProject(String project);
	
	@Description("Which Prometheus Instance")
	@Default.String("prometheus")
	String getPrometheus();
	void setPrometheus(String prometheus);
}
