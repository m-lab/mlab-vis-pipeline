package mlab.dataviz.util;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BigQueryIOHelpers {

	/**
	 * Helper to write to a BigQuery table
	 * @param rows
	 * @param outputTable
	 * @param outputSchema
	 * @param writeDisposition
	 * @param createDisposition
	 * @return
	 */
	public static WriteResult writeTable(PCollection<TableRow> rows, String outputTable, 
			TableSchema outputSchema, WriteDisposition writeDisposition, 
			CreateDisposition createDisposition) {
		Write<TableRow> transform = BigQueryIO.writeTableRows()
			.to(outputTable)
			.withSchema(outputSchema)
			.withWriteDisposition(writeDisposition)
			.withCreateDisposition(createDisposition);
		return rows.apply("Write " + outputTable, transform);
	}
}
