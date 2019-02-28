package mlab.dataviz.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.google.api.services.bigquery.model.TableRow;

public class TransformToMutation extends DoFn<TableRow, Mutation> {
	private static final long serialVersionUID = 1L;

	protected String rowKey;
	protected String timestampKey;

	public TransformToMutation(String rowKey, String timestampKey) {
		super();

		this.rowKey = rowKey;
		this.timestampKey = timestampKey;
	}

	public void processElement(DoFn<TableRow, Mutation>.ProcessContext ctx) throws Exception {
		TableRow row = ctx.element();

		String rowKey = (String) row.get(this.rowKey);

		String colFamily = "cf1";
		String colQualifier = "qual";
		String value = "aaa";

		ctx.output(
				new Put(rowKey.getBytes()).addColumn(colFamily.getBytes(), colQualifier.getBytes(), value.getBytes()));
	}

}
