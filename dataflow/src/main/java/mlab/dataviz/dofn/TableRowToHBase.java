package mlab.dataviz.dofn;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.v2.Row;
import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.Builder;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import mlab.dataviz.util.bigtable.BigtableColumnSchema;
import mlab.dataviz.util.bigtable.BigtableConfig;
import mlab.dataviz.util.bigtable.BigtableRowKeySchema;

/**
 * DoFn to convert from a BigQuery TableRow download metrics row to a HBase (Bigtable)
 * compatible Mutation object
 * @author pbeshai
 *
 */
public class TableRowToHBase extends DoFn<TableRow, KV<ByteString, Iterable<Mutation>>> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(TableRowToHBase.class);
	private static final String COLUMN_FAMILY_DATA = "data";
	private static final String COLUMN_FAMILY_META = "meta";

	private BigtableConfig schema;

	public TableRowToHBase(BigtableConfig schema) {
		super();
		this.schema = schema;
	}

	/**
	 * Augments builder with the specific cells.
	 * @param row
	 * @param builder
	 * @return
	 */
	private Builder addCellsToBuilder(TableRow row, Builder builder) {
		Iterator<BigtableColumnSchema> columnIterator = this.schema.columns.iterator();
		
		while (columnIterator.hasNext()) {
			BigtableColumnSchema colSchema = columnIterator.next();
			String famName = colSchema.getFamily();
			
			switch (colSchema.getType()) {
			case "string":
				String strValue = (String) row.get(colSchema.getName());
				ByteString strByteStr = ByteString.copyFromUtf8(strValue);
				builder.setSetCell(
						Mutation.SetCell.newBuilder()
						.setValue(strByteStr)
						.setFamilyName(famName));
				break;
			case "double":
				Double doubleValue = (Double) row.get(colSchema.getName());
				ByteString doubleByteStr = ByteString.copyFromUtf8(doubleValue.toString());
				builder.setSetCell(
						Mutation.SetCell.newBuilder()
						.setValue(doubleByteStr)
						.setFamilyName(famName));
				break;
			case "integer":
				Integer intValue = (Integer) row.get(colSchema.getName());
				ByteString intByteStr = ByteString.copyFromUtf8(intValue.toString());
				builder.setSetCell(
						Mutation.SetCell.newBuilder()
						.setValue(intByteStr)
						.setFamilyName(famName));
				break;
			case "integer_list":
				String strIntListValue = (String) row.get(colSchema.getName());
				builder.setSetCell(
						Mutation.SetCell.newBuilder()
						.setValue(ByteString.copyFromUtf8(strIntListValue))
						.setFamilyName(famName));
				break;
			}
		}
		return builder;
	}

	/**
	 * Create a row key byte array based on a string
	 * Client ISP | Client Continent Code | Client Country Code | Client City | Server ISP | Day
	 * @param row
	 * @return
	 */
	private ByteString getRowKey(TableRow row, BigtableConfig schema) {
		Iterator<BigtableRowKeySchema> keyIterator = schema.rowKeys.iterator();

		ArrayList<String> rowKeys = new ArrayList<String>();

		while (keyIterator.hasNext()) {
			BigtableRowKeySchema keySchema = keyIterator.next();

			String keyValue = (String) row.get(keySchema.getName());
			String paddedKeyValue = stringWithLength(keyValue, keySchema.getSize());
			rowKeys.add(paddedKeyValue);
		}
		String rowKey = String.join("|", rowKeys);
		return ByteString.copyFromUtf8(rowKey);
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
	
	@ProcessElement
	public void processElement_(ProcessContext c) {
		TableRow row = c.element();
        ByteString rowKey = this.getRowKey(row, this.schema);

        	Builder builder = Mutation.newBuilder();
        	
        Iterable<Mutation> mutations =
            ImmutableList.of(addCellsToBuilder(row, builder).build());
        
        c.output(KV.of(rowKey, mutations));
    }
}
