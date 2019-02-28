package mlab.dataviz.dofn;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class MergeAsnsFn extends DoFn<TableRow, TableRow> {

	private static final long serialVersionUID = 1L;
	
	private PCollectionView<Map<String, TableRow>> mergeAsnMap;

	public MergeAsnsFn(PCollectionView<Map<String, TableRow>> mergeAsnMap) {
		this.mergeAsnMap = mergeAsnMap;
	}

	@ProcessElement
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
