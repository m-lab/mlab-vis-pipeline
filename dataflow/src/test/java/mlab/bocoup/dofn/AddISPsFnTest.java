package mlab.bocoup.dofn;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import javax.xml.bind.DatatypeConverter;

import org.junit.Before;
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
	private static String IPV4 = "2";
	private static String IPV6 = "10";
	
	
	// For side loading ASNs
	PCollectionView<NavigableMap<String, TableRow>> asnsView;
	List<NavigableMap<String, TableRow>> asnMapIterable;
	
	private static final String[] ASN_TOKEN_KEYS = { "asn_string", "asn_number", "asn_name", "min_ip_hex", "max_ip_hex",
			"min_ip", "max_ip", "ip_family" };
	private static final String[] ASN_INFO = {
			"AS3215	AS3215	Orange S.A.	5A680000	5A69FFFF	90.104.0.0	90.105.255.255	2",
			"AS6327	AS6327	Shaw Communications Inc.	46400000	464FFFFF	70.64.0.0	70.79.182.109	2",
			"AS6900	AS6900	Hewlett Packard GmbH	2A0209F8900000000000000000000000	2A0209F8900FFFFFFFFFFFFFFFFFFFFF	2a02:9f8:9000::	2a02:9f8:900f:ffff:ffff:ffff:ffff:ffff	10",
			"AS9052	AS9052	Unidad Editorial S.A.	2001067C229400000000000000000000	2001067C2294FFFFFFFFFFFFFFFFFFFF	2001:67c:2294::	2001:67c:2294:ffff:ffff:ffff:ffff:ffff	10"
	};

	
	/**
	 * Get a base64 string of an IP address
	 * @param ip
	 * @return
	 */
	private static String ipToBase64(String ip) {
		String base64 = null;
		
		try {
			InetAddress inet = InetAddress.getByName(ip);
			byte[] ipBytes = inet.getAddress();
			base64 = DatatypeConverter.printBase64Binary(ipBytes);
		} catch (UnknownHostException e) {
			// fail the test if the IP doesn't work
			assertNull(e);
		}
		
		return base64;
	}
	
	/**
	 * Generate a row of test data for a given IP and IP family
	 * @param ip
	 * @param ipFamily
	 * @return
	 */
	private static TableRow makeTestData(String ip, String ipFamily) {
		TableRow row = new TableRow();
		row.set("client_ip_family", ipFamily);
		row.set("client_ip_base64", ipToBase64(ip));
		
		return row;
	}
	
	/**
	 * Generate the ASN data 
	 * @return
	 */
	private static List<TableRow> getAsnData() {
		List<TableRow> rows = new ArrayList<TableRow>();
		
		// add a row for each of the ASNs provided in the ASN_INFO key
		for (String asnInfo : ASN_INFO) {
			String[] tokens = asnInfo.split("\t");
			TableRow row = new TableRow();
			
			// tokens are split by tab and match the keys specified in ASN_TOKEN_KEYS
			for (int i = 0; i < ASN_TOKEN_KEYS.length; i++) {
				row.set(ASN_TOKEN_KEYS[i], tokens[i]);
			}	
			
			rows.add(row);
		}
		return rows;
	}
	
	/**
	 * Recreate the NavigableMap for side input 
	 * @param asns The ASN data to build the map from
	 * @return
	 */
	private static NavigableMap<String, TableRow> getAsnMap(List<TableRow> asns) {
		CombineAsNavigableMapHex combiner = new CombineAsNavigableMapHex();
		
		NavigableMap<String, TableRow> accumulator = combiner.createAccumulator();
		for (TableRow input : asns) {
			combiner.addInput(accumulator, input);
		}
		
		return accumulator;
	}
	
	@Before
	public void setUp() {
		// initialize the test pipeline -- we do not even run it, but we 
		// need it to create PCollections it seems.
		Pipeline p = TestPipeline.create();
		p.getCoderRegistry().registerCoder(NavigableMap.class, NavigableMapCoder.class);
		p.getCoderRegistry().registerCoder(TableRow.class, TableRowJsonCoder.class);	
		
		// create our initial ASNs PCollection
		List<TableRow> asnList = getAsnData();
		PCollection<TableRow> asns = p.apply(Create.of(asnList));
		
		// get ASNs as NavigableMap (and as a view to be used as side input)
		asnsView = asns.apply(
				Combine.globally(new CombineAsNavigableMapHex()).asSingletonView());
		
		// for some reason we have to recreate this navigable map here since it
		// cannot be extracted from the PCollectionView and `setSideInputInGlobalWindow
		// requires the values.
		asnMapIterable = new ArrayList<NavigableMap<String, TableRow>>();
		asnMapIterable.add(getAsnMap(asnList));
	}
	
	@Test
	public void testClientAsn() {		
		// create the DoFn to test
		AddISPsFn addIspsFn = new AddISPsFn(asnsView, "client_ip_family", "client_ip_base64", "client_asn_name",
				"client_asn_number", "min_ip_hex", "max_ip_hex", "min_ip_hex", "max_ip_hex", "asn_name", "asn_number");
		
		// create the tester
		DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(addIspsFn);
		
		// set side inputs 
		fnTester.setSideInputInGlobalWindow(asnsView, asnMapIterable);
		
		// prepare the test data
		List<TableRow> inputData = new ArrayList<TableRow>();
		inputData.add(makeTestData("70.74.182.109", IPV4)); // IPv4 match
		inputData.add(makeTestData("70.54.182.109", IPV4)); // IPv4 not in range
		inputData.add(makeTestData("2a02:9f8:9000::1", IPV6)); // IPv6 match
		inputData.add(makeTestData("2a02:9f8:8000::1", IPV6)); // IPv6 not in range
		
		
		// run the tester
		List<TableRow> output = fnTester.processBatch(inputData);
		
		// verify the output is what is expected
		assertEquals((String) output.get(0).get("client_asn_name"), "Shaw Communications Inc.");
		assertEquals((String) output.get(0).get("client_asn_number"), "AS6327");
		assertNull((String) output.get(1).get("client_asn_name"));
		assertNull((String) output.get(1).get("client_asn_number"));
		assertEquals((String) output.get(2).get("client_asn_name"), "Hewlett Packard GmbH");
		assertEquals((String) output.get(2).get("client_asn_number"), "AS6900");
		assertNull((String) output.get(3).get("client_asn_name"));
		assertNull((String) output.get(3).get("client_asn_number"));
	}
}
