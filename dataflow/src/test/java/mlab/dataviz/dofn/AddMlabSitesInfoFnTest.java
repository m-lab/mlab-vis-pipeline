package mlab.dataviz.dofn;

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

import mlab.dataviz.coder.NavigableMapCoder;
import mlab.dataviz.dofn.AddMlabSitesInfoFn;
import mlab.dataviz.transform.CombineAsNavigableMapHex;

/**
 * Tests the AddISPsFn DoFn for adding ISP information to TableRows
 * @author pbeshai
 *
 */
public class AddMlabSitesInfoFnTest {
	// values for the IP family field
	private static final int IP_FAMILY_IPV4 = 0;
	private static final int IP_FAMILY_IPV6 = 1;

	// constants for building the ASN side data
	private static final String[] ASN_TOKEN_KEYS = { "site", "latitude", "longitude", "city", "region_code",
			"country_code", "continent_code", "min_ip_hex", "max_ip_hex", "transit_provider", "min_ip", "max_ip",
			"ip_prefix", "min_ipv6_hex", "max_ipv6_hex", "min_ipv6", "max_ipv6", "ipv6_prefix" };
	private static final String[] ASN_INFO = {
			"acc01,5.606,-0.1681,Accra, ,GH,AF,000000000000000000000000C4C902C0,000000000000000000000000C4C902FF,Ghana IXP,196.201.2.192,196.201.2.255,196.201.2.192/26, , , , , ",
			"yyz01,43.6767,-79.6306,Toronto,ON,CA,NA,000000000000000000000000A2DB3000,000000000000000000000000A2DB303F,Hurricane Electric,162.219.48.0,162.219.48.63,162.219.48.0/26,2620010A80FD00000000000000000000,2620010A80FDFFFFFFFFFFFFFFFFFFFF,2620:10a:80fd::,2620:10a:80fd:ffff:ffff:ffff:ffff:ffff,2620:10a:80fd::/48"
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
		row.set("server_ip_family", String.valueOf(ipFamily));
		row.set("server_ip_base64", ipToBase64(ip));

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
			String[] tokens = asnInfo.split(",");
			TableRow row = new TableRow();

			// tokens are split by tab and match the keys specified in ASN_TOKEN_KEYS
			for (int i = 0; i < ASN_TOKEN_KEYS.length; i++) {
				row.set(ASN_TOKEN_KEYS[i], tokens[i].trim());
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
	public void testAddsInfo() {
		// create the DoFn to test
		AddMlabSitesInfoFn addInfoFn = new AddMlabSitesInfoFn(asnsView);

		// create the tester
		DoFnTester<TableRow, TableRow> fnTester = DoFnTester.of(addInfoFn);

		// set side inputs
		fnTester.setSideInputInGlobalWindow(asnsView, asnMapIterable);


		// prepare the test data
		List<TableRow> inputData = new ArrayList<TableRow>();
//		inputData.add(makeTestData("196.201.2.192", IP_FAMILY_IPV4)); // IPv4 match
//		inputData.add(makeTestData("196.201.2.190", IP_FAMILY_IPV4)); // IPv4 not in range
		inputData.add(makeTestData("2620:10a:80fd::1", IP_FAMILY_IPV6)); // IPv6 match
//		inputData.add(makeTestData("2622:10a:80fd::1", IP_FAMILY_IPV6)); // IPv6 not in range

		/*
		 *
		 * "acc01,5.606,-0.1681,Accra,,GH,AF
			"yyz01,43.6767,-79.6306,Toronto,ON,CA,NA,00000
		 *
		 */
		// run the tester
		List<TableRow> output = fnTester.processBatch(inputData);
//
//		// verify the output is what is expected
//		assertEquals("acc01", (String) output.get(0).get("mlab_site_id"));
//		assertEquals("5.606", (String) output.get(0).get("server_latitude"));
//		assertEquals("-0.1681", (String) output.get(0).get("server_longitude"));
//		assertEquals("Accra", (String) output.get(0).get("server_city"));
//		assertNull((String) output.get(0).get("server_region_code"));
//		assertEquals("GH", (String) output.get(0).get("server_country_code"));
//		assertEquals("AF", (String) output.get(0).get("server_continent_code"));
//
//		assertNull((String) output.get(1).get("mlab_site_id"));

		assertEquals("yyz01", (String) output.get(0).get("mlab_site_id"));
		assertEquals("43.6767", (String) output.get(0).get("server_latitude"));
		assertEquals("-79.6306", (String) output.get(0).get("server_longitude"));
		assertEquals("Toronto", (String) output.get(0).get("server_city"));
		assertEquals("ON", (String) output.get(0).get("server_region_code"));
		assertEquals("CA", (String) output.get(0).get("server_country_code"));
		assertEquals("NA", (String) output.get(0).get("server_continent_code"));

//		assertNull((String) output.get(3).get("mlab_site_id"));
	}
}
