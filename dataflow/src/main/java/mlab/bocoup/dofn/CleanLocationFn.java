package mlab.bocoup.dofn;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

public class CleanLocationFn extends DoFn<TableRow, TableRow> {
	private static final Logger LOG = LoggerFactory.getLogger(CleanLocationFn.class);
	private PCollectionView<Map<String, TableRow>> locationMap;
		
	public CleanLocationFn(PCollectionView<Map<String, TableRow>> locationMap) {
		this.locationMap = locationMap;
	}
	
	@Override
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
			
			dataRow.set("client_city", location.get("new_city"));
			dataRow.set("client_region_code", location.get("new_region_code"));
			System.out.println(location.get("new_region_code"));
			System.out.println(location.get("new_region_code") == null);
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
