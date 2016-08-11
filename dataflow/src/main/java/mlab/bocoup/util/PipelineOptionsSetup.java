package mlab.bocoup.util;

import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class PipelineOptionsSetup {
	
	public static OptionParser setupOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "runner" ).withRequiredArg();
		parser.accepts( "project" ).withRequiredArg();
		parser.accepts( "stagingLocation" ).withRequiredArg();
		parser.accepts( "numWorkers").withRequiredArg();
		return parser;
	}
	public static OptionSet getOptions(OptionParser parser, String[] args) {
	    OptionSet cmdOpts = parser.parse(args);
	    return cmdOpts;
	}
	
	/**
	 * Sets up BigQueryOptions for a pipeline run from commandline
	 * arguments. This is required because `fromArgs` does not otherwise
	 * allow for other parameters.
	 * 
	 * @param cmdOpts  an OptionSet of commandline arguments.
	 * @return options  BigQueryOptions for a pipeline run.
	 * @throws ClassNotFoundException
	 */
	public static BigQueryOptions setupBQOptions(OptionSet cmdOpts) throws ClassNotFoundException {

	    String project = (String) cmdOpts.valueOf("project");
	    String stagingLocation = (String) cmdOpts.valueOf("stagingLocation");
	    String numberOfWorkers = (String) cmdOpts.valueOf("numWorkers");
	    Class<? extends PipelineRunner<?>> runner = (Class<? extends PipelineRunner<?>>) (Class.forName((String) cmdOpts.valueOf("runner")));
	    	    
		// === Set up pipeline options
		DataflowPipelineOptions pipelineOpts = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		
		pipelineOpts.setProject(project);
		pipelineOpts.setRunner(runner);
		pipelineOpts.setStagingLocation(stagingLocation);
		
		if (cmdOpts.has("numWorkers")) {
			pipelineOpts.setNumWorkers(Integer.valueOf(numberOfWorkers));
		}
			
		BigQueryOptions options = (BigQueryOptions) pipelineOpts.as(BigQueryOptions.class);		
		
		return options;
	}
}
