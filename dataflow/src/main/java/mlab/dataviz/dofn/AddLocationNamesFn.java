package mlab.dataviz.dofn;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class AddLocationNamesFn extends DoFn<TableRow, TableRow> {
	
	private static final long serialVersionUID = 1L;
	
	private PCollectionView<Map<String, TableRow>> countryMap;
	private PCollectionView<Map<String, TableRow>> regionMap;

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
	
	public AddLocationNamesFn(PCollectionView<Map<String, TableRow>> countryMap, PCollectionView<Map<String, TableRow>> regionMap) {
		this.countryMap = countryMap;
		this.regionMap = regionMap;
	}
	
	@ProcessElement
	public void processElement(DoFn<TableRow, TableRow>.ProcessContext c) throws Exception {
		
		TableRow dataRow = c.element().clone();
		
		Map<String, TableRow> countryLookupMap = c.sideInput(this.countryMap);
		Map<String, TableRow> regionLookupMap = c.sideInput(this.regionMap);
		
		String clientCountryCode = (String) dataRow.get("client_country_code");
		String clientRegionCode = (String) dataRow.get("client_region_code");
		String clientContinentCode = (String) dataRow.get("client_continent_code");
		String clientRegionLookupCode = clientCountryCode + "_" + clientRegionCode;
		
		String serverCountryCode = (String) dataRow.get("server_country_code");
		String serverRegionCode = (String) dataRow.get("server_region_code");
		String serverContinentCode = (String) dataRow.get("server_continent_code");
		String serverRegionLookupCode = serverCountryCode + "_" + serverRegionCode;
		
		// client
		if (clientCountryCode != null) {
			if (countryLookupMap.containsKey(clientCountryCode)) {
				dataRow.set("client_country", countryLookupMap.get(clientCountryCode).get("country_name"));
			}
		}
		
		if (clientRegionCode != null) {
			if (regionLookupMap.containsKey(clientRegionLookupCode)) {
				dataRow.set("client_region", regionLookupMap.get(clientRegionLookupCode).get("region_name"));
			}
		}
		
		if (clientContinentCode != null) {
			dataRow.set("client_continent", continents.get(clientContinentCode));
		}
		
		// server
		if (serverCountryCode != null) {
			if (countryLookupMap.containsKey(serverCountryCode)) {
				dataRow.set("server_country", countryLookupMap.get(serverCountryCode).get("country_name"));
			}
		}
		
		if (serverRegionCode != null) {
			if (regionLookupMap.containsKey(serverRegionLookupCode)) {
				dataRow.set("server_region", regionLookupMap.get(serverRegionLookupCode).get("region_name"));
			}
		}
		
		if (serverContinentCode != null) {
			dataRow.set("server_continent", continents.get(serverContinentCode));
		}
		
		c.output(dataRow);	
	}
}
