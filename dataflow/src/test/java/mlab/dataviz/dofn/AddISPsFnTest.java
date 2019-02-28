package mlab.dataviz.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import javax.xml.bind.DatatypeConverter;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.coder.NavigableMapCoder;
import mlab.dataviz.transform.CombineAsNavigableMapHex;

/**
 * Tests the AddISPsFn DoFn for adding ISP information to TableRows
 * @author pbeshai
 *
 */
public class AddISPsFnTest {
	// values for the IP family field
	private static final int IP_FAMILY_IPV4 = 0;
	private static final int IP_FAMILY_IPV6 = 1;

	// constants for building the ASN side data
	private static final String[] ASN_TOKEN_KEYS = { "asn_string", "asn_number", "asn_name", "min_ip_hex", "max_ip_hex",
			"min_ip", "max_ip", "ip_family", "min_ipv6_hex", "max_ipv6_hex" };
	private static final String[] ASN_INFO = {
			"AS3215	AS3215	Orange S.A.	0000000000000000000000005A680000	0000000000000000000000005A69FFFF	90.104.0.0	90.105.255.255	2	x	x",
			"AS6327	AS6327	Shaw Communications Inc.	00000000000000000000000046400000	000000000000000000000000464FFFFF	70.64.0.0	70.79.182.109	2	x	x",
			"AS6900	AS6900	Hewlett Packard GmbH	2A0209F8900000000000000000000000	2A0209F8900FFFFFFFFFFFFFFFFFFFFF	2a02:9f8:9000::	2a02:9f8:900f:ffff:ffff:ffff:ffff:ffff	10	2A0209F8900000000000000000000000	2A0209F8900FFFFFFFFFFFFFFFFFFFFF",
			"AS9052	AS9052	Unidad Editorial S.A.	2001067C229400000000000000000000	2001067C2294FFFFFFFFFFFFFFFFFFFF	2001:67c:2294::	2001:67c:2294:ffff:ffff:ffff:ffff:ffff	10	2001067C229400000000000000000000	2001067C2294FFFFFFFFFFFFFFFFFFFF",
			"AS12782	AS12782	Uppsala Lans Landsting	x	x	2a01:58:20::	2a01:58:21:ffff:ffff:ffff:ffff:ffff	10	2A010058002000000000000000000000	2A0100580021FFFFFFFFFFFFFFFFFFFF",
	};

	// For side loading ASNs
	PCollectionView<NavigableMap<String, TableRow>> asnsView;
	List<NavigableMap<String, TableRow>> asnMapIterable;


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
	private static TableRow makeTestData(String ip, int ipFamily) {
		TableRow row = new TableRow();
		row.set("client_ip_family", String.valueOf(ipFamily));
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
		p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(NavigableMap.class, NavigableMapCoder.class));
		p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(TableRow.class, TableRowJsonCoder.class));

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
//		// create the DoFn to test
//		AddISPsFn addIspsFn = new AddISPsFn(asnsView, "client_ip_family", "client_ip_base64", "min_ip_hex",
//				"max_ip_hex", "min_ip_hex", "max_ip_hex", "client_asn_name", "client_asn_number", "asn_name",
//				"asn_number");
//
//		// create the tester
//		DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(addIspsFn);
//
////		TestPipeline testPipeline = TestPipeline.create();
////
////		testPipeline.apply(ParDo.of(addIspsFn).withSideInputs(asnsView));
//
//
//		// set side inputs
//		fnTester.setSideInputs(asnsView, asnMapIterable);
//
//		// prepare the test data
//		List<TableRow> inputData = new ArrayList<TableRow>();
//		inputData.add(makeTestData("70.74.182.109", IP_FAMILY_IPV4)); // IPv4 match
//		inputData.add(makeTestData("70.54.182.109", IP_FAMILY_IPV4)); // IPv4 not in range
//		inputData.add(makeTestData("2a02:9f8:9000::1", IP_FAMILY_IPV6)); // IPv6 match
//		inputData.add(makeTestData("2a02:9f8:8000::1", IP_FAMILY_IPV6)); // IPv6 not in range
//
//
//		// run the tester
//		List<TableRow> output = fnTester.processBatch(inputData);
//
//		// verify the output is what is expected
//		assertEquals("Shaw Communications Inc.", (String) output.get(0).get("client_asn_name"));
//		assertEquals("AS6327", (String) output.get(0).get("client_asn_number"));
//		assertNull((String) output.get(1).get("client_asn_name"));
//		assertNull((String) output.get(1).get("client_asn_number"));
//		assertEquals("Hewlett Packard GmbH", (String) output.get(2).get("client_asn_name"));
//		assertEquals("AS6900", (String) output.get(2).get("client_asn_number"));
//		assertNull((String) output.get(3).get("client_asn_name"));
//		assertNull((String) output.get(3).get("client_asn_number"));
	}

	@Test
	public void testNoAsnNumber() {
//		// create the DoFn to test
//		AddISPsFn addIspsFn = new AddISPsFn(asnsView, "client_ip_family", "client_ip_base64", "min_ip_hex",
//				"max_ip_hex", "min_ip_hex", "max_ip_hex", "client_asn_name", "client_asn_number", "asn_name", null);
//
//		// create the tester
//		DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(addIspsFn);
//
//		// set side inputs
//		fnTester.setSideInputInGlobalWindow(asnsView, asnMapIterable);
//
//		// prepare the test data
//		List<TableRow> inputData = new ArrayList<TableRow>();
//		inputData.add(makeTestData("70.74.182.109", IP_FAMILY_IPV4)); // IPv4 match
//		inputData.add(makeTestData("70.54.182.109", IP_FAMILY_IPV4)); // IPv4 not in range
//
//		// run the tester
//		List<TableRow> output = fnTester.processBatch(inputData);
//
//		// verify the output is what is expected
//		assertEquals("Shaw Communications Inc.", (String) output.get(0).get("client_asn_name"));
//		assertNull((String) output.get(0).get("client_asn_number"));
	}

	@Test
	public void testDifferentIPv6Columns() {
//		// create the DoFn to test
//		AddISPsFn addIspsFn = new AddISPsFn(asnsView, "client_ip_family", "client_ip_base64", "min_ip_hex",
//				"max_ip_hex", "min_ipv6_hex", "max_ipv6_hex", "client_asn_name", "client_asn_number", "asn_name",
//				"asn_number");
//
//		// create the tester
//		DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(addIspsFn);
//
//		// set side inputs
//		fnTester.setSideInputInGlobalWindow(asnsView, asnMapIterable);
//
//		// prepare the test data
//		List<TableRow> inputData = new ArrayList<>();
//		inputData.add(makeTestData("2a01:58:20::1", IP_FAMILY_IPV6)); // IPv6 match in min/max_ipv6_hex columns
//		inputData.add(makeTestData("2a01:58:10::1", IP_FAMILY_IPV6)); // IPv6 not in range in min/max_ipv6 columns
//		inputData.add(makeTestData("2a02:9f8:9000::1", IP_FAMILY_IPV6)); // IPv6 match both min_ip_hex and min_ipv6_hex
//		inputData.add(makeTestData("2a02:9f8:8000::1", IP_FAMILY_IPV6)); // IPv6 not in range
//
//		// run the tester
//		List<TableRow> output = fnTester.processBatch(inputData);
//
//		// verify the output is what is expected
//		assertEquals("Uppsala Lans Landsting", (String) output.get(0).get("client_asn_name"));
//		assertEquals("AS12782", (String) output.get(0).get("client_asn_number"));
//		assertNull((String) output.get(1).get("client_asn_name"));
//		assertNull((String) output.get(1).get("client_asn_number"));
//		assertEquals("Hewlett Packard GmbH", (String) output.get(2).get("client_asn_name"));
//		assertEquals("AS6900", (String) output.get(2).get("client_asn_number"));
//		assertNull((String) output.get(3).get("client_asn_name"));
//		assertNull((String) output.get(3).get("client_asn_number"));
	}

	@Test
	public void testHexFromBase64() {
		// We need the hex to be 32 characters in length so string sorting works.
		assertEquals("26020306CD9E1490ED3C8B7F85EE70B6", AddISPsFn.hexFromBase64("JgIDBs2eFJDtPIt/he5wtg=="));
		assertEquals("000000000000000000000000D4A2334B", AddISPsFn.hexFromBase64("1KIzSw=="));

	}
}
