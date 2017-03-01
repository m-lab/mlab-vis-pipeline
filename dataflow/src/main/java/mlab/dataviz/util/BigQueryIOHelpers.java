package mlab.dataviz.util;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;

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
	public static PDone writeTable(PCollection<TableRow> rows, String outputTable, 
			TableSchema outputSchema, WriteDisposition writeDisposition, 
			CreateDisposition createDisposition) {
		return rows.apply(BigQueryIO.Write
			.named("Write " + outputTable)
			.to(outputTable)
			.withSchema(outputSchema)
			.withWriteDisposition(writeDisposition)
			.withCreateDisposition(createDisposition));
	}
	
}
