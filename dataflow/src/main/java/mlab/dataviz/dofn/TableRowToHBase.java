package mlab.dataviz.dofn;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.util.bigtable.BigtableColumnSchema;
import mlab.dataviz.util.bigtable.BigtableConfig;
import mlab.dataviz.util.bigtable.BigtableRowKeySchema;

/**
 * DoFn to convert from a BigQuery TableRow download metrics row to a HBase (Bigtable)
 * compatible Mutation object
 * @author pbeshai
 *
 */
public class TableRowToHBase extends DoFn<TableRow, Mutation> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private BigtableConfig schema;

	public TableRowToHBase(BigtableConfig schema) {

		super();

		this.schema = schema;
	}

	private void addStringColumn(Put put, String colFamily, TableRow row, String field) {
		String value = (String) row.get(field);
		if(value != null && value.length() > 0) {
			put.addColumn(colFamily.getBytes(), field.getBytes(), value == null ? null : value.getBytes(StandardCharsets.UTF_8));
		}
	}

	private void addIntegerColumn(Put put, String colFamily, TableRow row, String field) {
		// Note: it presently seems that "INTEGER" columns are passed as Strings
		String value = (String) row.get(field);
		if(value != null && value.length() > 0) {
			put.addColumn(colFamily.getBytes(), field.getBytes(), value == null ? null : Bytes.toBytes(value));
		}
	}
	
	private void addDoubleColumn(Put put, String colFamily, TableRow row, String field) {
		Double value = (Double) row.get(field);
		if(value != null) {
            put.addColumn(colFamily.getBytes(), field.getBytes(), value == null ? null : Bytes.toBytes(value));
        }
	}

	private void addColumns(TableRow row, Put put, BigtableConfig schema) {

		Iterator<BigtableColumnSchema> columnIterator = schema.columns.iterator();
		while (columnIterator.hasNext()) {
			BigtableColumnSchema colSchema = columnIterator.next();

			switch (colSchema.getType()) {
			case "string":
				addStringColumn(put, colSchema.getFamily(), row, colSchema.getName());
				break;
			case "double":
				addDoubleColumn(put, colSchema.getFamily(), row, colSchema.getName());
				break;
			case "integer":
				addIntegerColumn(put, colSchema.getFamily(), row, colSchema.getName());
				break;
			case "integer_list":
				// encode as a string
				addStringColumn(put, colSchema.getFamily(), row, colSchema.getName());
				break;
			}
		}
	}

	/**
	 * Create a row key byte array based on a string
	 * Client ISP | Client Continent Code | Client Country Code | Client City | Server ISP | Day
	 * @param row
	 * @return
	 */
	private byte[] getRowKey(TableRow row, BigtableConfig schema) {
		Iterator<BigtableRowKeySchema> keyIterator = schema.rowKeys.iterator();

		ArrayList<String> rowKeys = new ArrayList<String>();

		while (keyIterator.hasNext()) {
			BigtableRowKeySchema keySchema = keyIterator.next();

			String keyValue = (String) row.get(keySchema.getName());
			String paddedKeyValue = stringWithLength(keyValue, keySchema.getSize());
			rowKeys.add(paddedKeyValue);
		}
		String rowKey = String.join("|", rowKeys);
		return rowKey.getBytes(StandardCharsets.UTF_8);
	}

	@ProcessElement
	public void processElement(ProcessContext c) throws Exception {
		TableRow row = c.element();

		byte[] rowKey = this.getRowKey(row, this.schema);

		Put put = new Put(rowKey);

		this.addColumns(row, put, this.schema);
		
		c.output(put);
	}

	/**
	 * Ensures that str is exactly length long by truncating or padding with trailing spaces
	 * @param str
	 * @param length
	 * @return str with set length
	 */
	private static String stringWithLength(String str, int length) {
		if (str == null) {
			str = "";
		}

		// truncate if too long
		if (str.length() > length) {
			return str.substring(0, length);
		}

		// add in spaces if not long enough
		int paddingToAdd = length - str.length();
		StringBuilder builder = new StringBuilder(str);
		for (int i = 0; i < paddingToAdd; i++) {
			builder.append(" ");
		}

		return builder.toString();
	}
}
