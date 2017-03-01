package mlab.dataviz.dofn;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class TransformToMutation extends DoFn<TableRow, Mutation> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(TransformToMutation.class);
	
	
	
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
		
		
		ctx.output(new Put(rowKey.getBytes()).addColumn(colFamily.getBytes(), colQualifier.getBytes(), value.getBytes()));
	}
	
}
