package mlab.bocoup.dofn;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

public class MergeAsnsFn extends DoFn<TableRow, TableRow> {
	private static final Logger LOG = LoggerFactory.getLogger(MergeAsnsFn.class);
	private PCollectionView<Map<String, TableRow>> mergeAsnMap;
		
	public MergeAsnsFn(PCollectionView<Map<String, TableRow>> mergeAsnMap) {
		this.mergeAsnMap = mergeAsnMap;
	}
	
	@Override
	public void processElement(DoFn<TableRow, TableRow>.ProcessContext c) throws Exception {
		
		TableRow dataRow = c.element().clone();
		
		Map<String, TableRow> mergeAsnMap = c.sideInput(this.mergeAsnMap);
		
		String clientAsnNumber = (String) dataRow.get("client_asn_number");
		String serverAsnNumber = (String) dataRow.get("server_asn_number");
		
		// client
		if (clientAsnNumber != null) {
			if (mergeAsnMap.containsKey(clientAsnNumber)) {
				dataRow.set("client_asn_number", mergeAsnMap.get(clientAsnNumber).get("new_asn_number"));
				dataRow.set("client_asn_name", mergeAsnMap.get(clientAsnNumber).get("new_asn_name"));
			}
		}
		
		// server
		if (serverAsnNumber != null) {
			if (mergeAsnMap.containsKey(serverAsnNumber)) {
				dataRow.set("server_asn_number", mergeAsnMap.get(serverAsnNumber).get("new_asn_number"));
				dataRow.set("server_asn_name", mergeAsnMap.get(serverAsnNumber).get("new_asn_name"));
			}
		}
		
		c.output(dataRow);	
	}
}
