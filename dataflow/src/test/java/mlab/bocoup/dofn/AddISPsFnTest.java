package mlab.bocoup.dofn;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import mlab.bocoup.coder.NavigableMapCoder;
import mlab.bocoup.transform.CombineAsNavigableMapHex;

public class AddISPsFnTest {

	/**
	 * Generate the NDT test data to decorate with ASN info
	 * @return
	 */
	private static List<TableRow> getTestData() {
		List<TableRow> rows = new ArrayList<TableRow>();
		
		TableRow row = new TableRow();
		row.set("client_ip_family", "2");
		row.set("client_ip_base64", "Rkq2bQ==");
		row.set("client_ip", "70.74.182.109");
		
		rows.add(row);
		
		return rows;
	}
	
	/**
	 * Generate the ASN data 
	 * @return
	 */
	private static List<TableRow> getAsnData() {
		List<TableRow> rows = new ArrayList<TableRow>();
		
		TableRow row = new TableRow();
		row.set("min_ip_hex", "46400000");
		row.set("max_ip_hex", "464FFFFF");
		row.set("asn_name", "Shaw Communications Inc.");
		row.set("asn_number",  "AS6327");	
		
		rows.add(row);
		
		return rows;
	}
	
	/**
	 * Recreate the NavigableMap for side input 
	 * @param asns The ASN data to build the map from
	 * @return
	 */
	private static NavigableMap<String, TableRow> getNavMap(List<TableRow> asns) {
		CombineAsNavigableMapHex combiner = new CombineAsNavigableMapHex();
		
		NavigableMap<String, TableRow> accumulator = combiner.createAccumulator();
		for (TableRow input : asns) {
			combiner.addInput(accumulator, input);
		}
		
		return accumulator;
	}
	
	@Test
	public void testClientAsnIPv4() {
		// initialize the test pipeline -- we do not even run it, but we 
		// need it to create PCollections it seems.
		Pipeline p = TestPipeline.create();
		p.getCoderRegistry().registerCoder(NavigableMap.class, NavigableMapCoder.class);
		p.getCoderRegistry().registerCoder(TableRow.class, TableRowJsonCoder.class);
		
		// create our initial ASNs PCollection
		List<TableRow> asnList = getAsnData();
		PCollection<TableRow> asns = p.apply(Create.of(asnList));
		
		// get ASNs as NavigableMap (and as a view to be used as side input)
		PCollectionView<NavigableMap<String, TableRow>> asnsView = asns.apply(
				Combine.globally(new CombineAsNavigableMapHex()).asSingletonView());
		
		// for some reason we have to recreate this navigable map here since it
		// cannot be extracted from the PCollectionView
		List<NavigableMap<String, TableRow>> navMapIterable = new ArrayList<NavigableMap<String, TableRow>>();
		navMapIterable.add(getNavMap(asnList));
		
		
		// create the DoFn to test
		AddISPsFn addIspsFn = new AddISPsFn(asnsView, "client_ip_family", "client_ip_base64", "client_asn_name",
				"client_asn_number", "min_ip_hex", "max_ip_hex", "min_ip_hex", 
				"max_ip_hex", "asn_name", "asn_number");
		
		// create the tester
		DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(addIspsFn);
		
		// set side inputs 
		fnTester.setSideInputInGlobalWindow(asnsView, navMapIterable);
		
		// run the tester
		List<TableRow> output = fnTester.processBatch(getTestData());
		
		// verify the output is what is expected
		for (TableRow row : output) {
			assertEquals((String) row.get("client_asn_name"), "Shaw Communications Inc.");
			assertEquals((String) row.get("client_asn_number"), "AS6327");
		}
	}
}
