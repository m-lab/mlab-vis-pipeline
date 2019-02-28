package mlab.dataviz.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import mlab.dataviz.util.BigQueryIOHelpers;

public class BasePipeline {
	protected Pipeline pipeline;
	protected String inputTable;
	protected String outputTable;
	protected boolean writeData = false;
	protected TableSchema outputSchema;
	protected WriteDisposition writeDisposition = WriteDisposition.WRITE_TRUNCATE;
	protected CreateDisposition createDisposition = CreateDisposition.CREATE_IF_NEEDED;

	public BasePipeline(Pipeline p) {
		super();
		this.pipeline = p;
	}

	/**
	 * Initial read of the source data table from BigQuery.
	 *
	 * @param p The Dataflow Pipeline
	 * @return The PCollection representing the data from BigQuery
	 */
	public PCollection<TableRow> loadData() {
		// read in the by IP data from `by_ip_day_base`
		return pipeline.apply("Read " + inputTable, BigQueryIO.readTableRows().from(inputTable));
	}

	/**
	 * Write the modified rows to a destination table in BigQuery
	 *
	 * @param data              The PCollection of rows to output
	 * @param outputTable       The identifier for the table to write to (e.g.
	 *                          `data_viz.my_table`)
	 * @param outputTableSchema The schema describing the output table
	 */
	public void writeDataToTable(PCollection<TableRow> data) {
		// Write the changes to the specified table
		BigQueryIOHelpers.writeTable(data, this.outputTable, this.outputSchema, this.writeDisposition,
				this.createDisposition);
	}

	public Pipeline getPipeline() {
		return pipeline;
	}

	public BasePipeline setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
		return this;
	}

	public String getInputTable() {
		return inputTable;
	}

	public BasePipeline setInputTable(String inputTable) {
		this.inputTable = inputTable;
		return this;
	}

	public String getOutputTable() {
		return outputTable;
	}

	public BasePipeline setOutputTable(String outputTable) {
		this.outputTable = outputTable;
		return this;
	}

	public TableSchema getOutputSchema() {
		return outputSchema;
	}

	public BasePipeline setOutputSchema(TableSchema outputSchema) {
		this.outputSchema = outputSchema;
		return this;
	}

	public WriteDisposition getWriteDisposition() {
		return writeDisposition;
	}

	public BasePipeline setWriteDisposition(WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	public CreateDisposition getCreateDisposition() {
		return createDisposition;
	}

	public BasePipeline setCreateDisposition(CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}

	public boolean getWriteData() {
		return writeData;
	}

	public BasePipeline setWriteData(boolean writeData) {
		this.writeData = writeData;
		return this;
	}

	/**
	 * Adds necessary configuration to a Pipeline before applying. Template method
	 * to be overridden.
	 *
	 * @param p The pipeline being used
	 */
	public void preparePipeline() {
		/* template method */
	}

	/**
	 * Template method for subclasses to just override the inner part of apply.
	 * 
	 * @param rows
	 * @return the modified rows
	 */
	protected PCollection<TableRow> applyInner(PCollection<TableRow> rows) {
		/* template method */
		return rows;
	}

	/**
	 * The main work of the pipeline goes here.
	 * 
	 * @param rows
	 * @return the modified rows
	 */
	public PCollection<TableRow> apply(PCollection<TableRow> rows) {
		this.preparePipeline();

		PCollection<TableRow> data;
		if (rows != null) {
			data = rows;
		} else {
			data = this.loadData();
		}

		// this should be overridden by subclasses
		data = this.applyInner(data);

		if (this.writeData) {
			// write the processed data to a table
			this.writeDataToTable(data);
		}

		return data;
	}

	/**
	 * Delegates to apply(null) to cause the pipeline to load data
	 * 
	 * @return the modified rows
	 */
	public PCollection<TableRow> apply() {
		return this.apply(null);
	}
}