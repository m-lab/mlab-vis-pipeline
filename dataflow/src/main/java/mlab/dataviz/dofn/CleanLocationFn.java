package mlab.dataviz.dofn;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class CleanLocationFn extends DoFn<TableRow, TableRow> {
	
	private static final long serialVersionUID = 1L;
	
	private PCollectionView<Map<String, TableRow>> locationMap;
		
	public CleanLocationFn(PCollectionView<Map<String, TableRow>> locationMap) {
		this.locationMap = locationMap;
	}
	
	@ProcessElement
	public void processElement(DoFn<TableRow, TableRow>.ProcessContext c) throws Exception {
		TableRow dataRow = c.element();
		
		Map<String, TableRow> locationMap = c.sideInput(this.locationMap);
		
		// read in the client location properties
		String clientContinentCode = (String) dataRow.get("client_continent_code");
		String clientCountryCode = (String) dataRow.get("client_country_code");
		String clientRegionCode = (String) dataRow.get("client_region_code");
		String clientCity = (String) dataRow.get("client_city");
		
		// generate the location key to look up in the map
		String locationKey = makeLocationKey(clientContinentCode, clientCountryCode, clientRegionCode, clientCity);
		
		// if we have an override for this, update the city and region code accordingly
		if (locationMap.containsKey(locationKey)) {
			dataRow = dataRow.clone();
			TableRow location = locationMap.get(locationKey);
			
			// note we cannot have null cities or regions, so enforce that here.
			String newCity = (String) location.get("new_city");
			if (newCity != null) {
				dataRow.set("client_city", newCity);
			}
			
			String newRegionCode = (String) location.get("new_region_code");
			if (newRegionCode != null) {
				dataRow.set("client_region_code", newRegionCode);
			}
		}
		
		c.output(dataRow);	
	}

	/**
	 * Helper to generate a location key used by location_cleaning map.
	 * Concatenates all values, removes whitespace and certain characters and 
	 * makes the string lowercase.
	 * 
	 * @param clientContinentCode
	 * @param clientCountryCode
	 * @param clientRegionCode
	 * @param clientCity
	 * @return
	 */
	private String makeLocationKey(String clientContinentCode, String clientCountryCode, String clientRegionCode,
			String clientCity) {
		StringBuilder str = new StringBuilder();
		if (clientContinentCode != null) {
			str.append(clientContinentCode);
		}
		if (clientCountryCode != null) {
			str.append(clientCountryCode);
		}
		if (clientRegionCode != null) {
			str.append(clientRegionCode);
		}
		if (clientCity != null) {
			str.append(clientCity);
		}
		
		return str.toString().toLowerCase().replaceAll("[\\W|_]", "");
	}
}
