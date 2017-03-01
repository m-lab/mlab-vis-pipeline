package mlab.dataviz.dofn;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import mlab.dataviz.dofn.CleanLocationFn;
import mlab.dataviz.dofn.ExtractLocationKeyFn;

/**
 * Tests the CleanLocationFn DoFn for replacing client city and region_code if 
 * in the replacement map
 * @author pbeshai
 *
 */
public class CleanLocationFnTest {
	// constants for building the ASN side data
	private static final String[] LOCATION_CLEANING_COLUMNS = { "location_key", "new_city", "new_region_code" };
	private static final String[] LOCATION_CLEANING_DATA = {
			"nausnynewyorkcity	New York	NY",
			"eugbh9london	London	EN"
	};

	// For side loading the location cleaning table
	PCollectionView<Map<String, TableRow>> locationCleaningView;
	List<KV<String, TableRow>> locationMapIterable;
	
	
	/**
	 * Generate a row of test data for a given client location
	 */
	private static TableRow makeTestData(String continentCode, String countryCode, String regionCode, String city) {
		TableRow row = new TableRow();
		row.set("client_city", city);
		row.set("client_region_code", regionCode);
		row.set("client_country_code", countryCode);
		row.set("client_continent_code", continentCode);
		
		return row;
	}
	
	/**
	 * Generate the location cleaning table data
	 * @return
	 */
	private static List<TableRow> getLocationCleaningData() {
		List<TableRow> rows = new ArrayList<TableRow>();
		
		// add a row for each of the data provided in LOCATION_CLEANING_DATA
		for (String locationCleaningRow : LOCATION_CLEANING_DATA) {
			String[] tokens = locationCleaningRow.split("\t");
			TableRow row = new TableRow();
			
			// tokens are split by tab and match the columns specified in LOCATION_CLEANING_COLUMNS
			for (int i = 0; i < LOCATION_CLEANING_COLUMNS.length; i++) {
				String value = tokens[i].equals("null") ? null : tokens[i];
				row.set(LOCATION_CLEANING_COLUMNS[i], value);
			}	
			
			rows.add(row);
		}
		return rows;
	}
	
	/**
	 * Recreate the Map for side input 
	 * @return
	 */
	private static List<KV<String, TableRow>> getLocationCleaningMap(List<TableRow> locationCleaningRows) {
		List<KV<String, TableRow>> rows = new ArrayList<KV<String, TableRow>>();
		
		for (TableRow input : locationCleaningRows) {
			rows.add(KV.of((String) input.get("location_key"), input));
		}
		
		return rows;
	}
	
	@Before
	public void setUp() {
		// initialize the test pipeline -- we do not even run it, but we 
		// need it to create PCollections it seems.
		Pipeline p = TestPipeline.create();
		p.getCoderRegistry().registerCoder(TableRow.class, TableRowJsonCoder.class);	
		
		// create our initial PCollection
		List<TableRow> locationCleaningList = getLocationCleaningData();
		PCollection<TableRow> locationCleaningRows = p.apply(Create.of(locationCleaningList));
		
		// get as Map (and as a view to be used as side input)
		PCollection<KV<String, TableRow>> locationKeys = 
				locationCleaningRows.apply(ParDo.named("Extract Location Keys")
						.of(new ExtractLocationKeyFn()));
		
		locationCleaningView = locationKeys.apply(View.asMap());
		
		// for some reason we have to recreate this navigable map here since it
		// cannot be extracted from the PCollectionView and `setSideInputInGlobalWindow
		// requires the values.
		locationMapIterable = getLocationCleaningMap(locationCleaningList);
	}
	
	@Test
	public void testLocationCleaning() {		
		// create the DoFn to test
		CleanLocationFn cleanLocationFn = new CleanLocationFn(locationCleaningView);
		
		// create the tester
		DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(cleanLocationFn);
		
		// set side inputs 
		fnTester.setSideInputInGlobalWindow(locationCleaningView, locationMapIterable);
		
		// prepare the test data
		List<TableRow> inputData = new ArrayList<TableRow>();
		inputData.add(makeTestData("NA", "US", "NY", "New York City")); // city replacement
		inputData.add(makeTestData("EU", "GB", "H9", "London")); // region replacement
		inputData.add(makeTestData("NA", "US", "WA", "New York City")); // no replacement
		
		// run the tester
		List<TableRow> output = fnTester.processBatch(inputData);
		
		// verify the output is what is expected
		assertEquals("New York", (String) output.get(0).get("client_city"));
		assertEquals("NY", (String) output.get(0).get("client_region_code"));
		assertEquals("US", (String) output.get(0).get("client_country_code"));
		assertEquals("NA", (String) output.get(0).get("client_continent_code"));
		
		assertEquals("London", (String) output.get(1).get("client_city"));
		assertEquals("EN", (String) output.get(1).get("client_region_code"));
		assertEquals("GB", (String) output.get(1).get("client_country_code"));
		assertEquals("EU", (String) output.get(1).get("client_continent_code"));
		
		assertEquals("New York City", (String) output.get(2).get("client_city"));
		assertEquals("WA", (String) output.get(2).get("client_region_code"));
		assertEquals("US", (String) output.get(2).get("client_country_code"));
		assertEquals("NA", (String) output.get(2).get("client_continent_code"));
	}
}
