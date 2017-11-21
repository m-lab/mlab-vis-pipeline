package mlab.dataviz.util;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

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
		
		rows.apply(BigQueryIO.writeTableRows()
			.to(outputTable)
			.withSchema(outputSchema)
			.withWriteDisposition(writeDisposition)
			.withCreateDisposition(createDisposition));
		
		return PDone.in(rows.getPipeline());
		
	}
	
}
