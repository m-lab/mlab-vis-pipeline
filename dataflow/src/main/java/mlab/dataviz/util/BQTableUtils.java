package mlab.dataviz.util;

import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.pipelineopts.UpdatePipelineOptions;

public class BQTableUtils {
	private String project;
	
	/**
	 * Initializes bq table utils.
	 * @param opts - Must contain project
	 */
	public BQTableUtils(HistoricPipelineOptions opts) {
		this.project = opts.getProject();
	}
	
	/**
	 * Initializes bq table utils.
	 * @param opts - Must contain project
	 */
	public BQTableUtils(UpdatePipelineOptions opts) {
		this.project = opts.getProject();
	}
	
	/**
	 * Prepends a project name to a table
	 * @param tableName
	 * @return
	 */
	public String wrapTable(String tableName) {
		return this.project + ":" + tableName;
	}
	
	/**
	 * Like wrap, but with [*].
	 * @param tableName
	 * @return
	 */
	public String wrapTableWithBrakets(String tableName) {
		return "[" + this.project + ":" + tableName + "]";
	}
}
