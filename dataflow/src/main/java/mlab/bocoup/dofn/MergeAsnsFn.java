package mlab.bocoup.dofn;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

public class MergeAsnsFn extends DoFn<TableRow, TableRow> {
	private static final Logger LOG = LoggerFactory.getLogger(MergeAsnsFn.class);
	private PCollectionView<Map<String, TableRow>> mergeAsnMap;
	

	 private static final Map<String, String> continents;
	 static {
		 continents = new HashMap<String, String>();
		 continents.put("EU", "Europe");
		 continents.put("AS", "Asia");
		 continents.put("AF", "Africa");
		 continents.put("NA", "North America");
		 continents.put("AN", "Antarctica");
		 continents.put("SA", "South America");
		 continents.put("OC", "Oceania");
	}
	
	public MergeAsnsFn(PCollectionView<Map<String, TableRow>> mergeAsnMap) {
		this.mergeAsnMap = mergeAsnMap;
		
	}
	
	@Override
	public void processElement(DoFn<TableRow, TableRow>.ProcessContext c) throws Exception {
		
		TableRow dataRow = c.element().clone();
		
		Map<String, TableRow> mergeAsnMap = c.sideInput(this.mergeAsnMap);
		
		String clientAsnNumber = (String) dataRow.get("client_asn_number");
		//String clientAsnName = (String) dataRow.get("client_asn_name");
		
		// client
		if (clientAsnNumber != null) {
			if (mergeAsnMap.containsKey(clientAsnNumber)) {
				dataRow.set("client_asn_number", mergeAsnMap.get(clientAsnNumber).get("new_asn_number"));
				dataRow.set("client_asn_name", mergeAsnMap.get(clientAsnNumber).get("new_asn_name"));
			}
		}
		
		c.output(dataRow);	
	}
}
