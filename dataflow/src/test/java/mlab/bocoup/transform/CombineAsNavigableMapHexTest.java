package mlab.bocoup.transform;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;

/**
 * Test combining ASNs into a navigable map based on the min IP hex value
 * @author pbeshai
 *
 */
public class CombineAsNavigableMapHexTest {

	@Test
	public void test() {
		// create the combiner to test manually
		CombineAsNavigableMapHex combiner = new CombineAsNavigableMapHex();
		
		// create a couple different accumulators to ensure merge works
		NavigableMap<String, TableRow> accumulator1 = combiner.createAccumulator();
		NavigableMap<String, TableRow> accumulator2 = combiner.createAccumulator();
		List<NavigableMap<String, TableRow>> accumulatorList = new ArrayList<NavigableMap<String, TableRow>>();
		accumulatorList.add(accumulator1);
		accumulatorList.add(accumulator2);
		
		//
		// NOTE: All hex codes must be the same length.
		// See https://github.com/bocoup/mlab-vis-pipeline/issues/1
		//
		
		// test having only ipv4 hex
		TableRow ipv4Row = new TableRow();
		ipv4Row.set("min_ip_hex", "0000000000000000000000004056C8C0");
		ipv4Row.set("max_ip_hex", "0000000000000000000000004056C8FF");
		combiner.addInput(accumulator1, ipv4Row);
		
		// test having only ipv6 hex
		TableRow ipv6Row = new TableRow();
		ipv6Row.set("min_ipv6_hex",  "24040138400900000000000000000000");
		ipv6Row.set("max_ipv6_hex",  "2404013840090000FFFFFFFFFFFFFFFF");
		combiner.addInput(accumulator1, ipv6Row);
		
		// test having both hexes
		TableRow bothRow = new TableRow();
		bothRow.set("min_ip_hex", "000000000000000000000000C4C902C0");
		bothRow.set("max_ip_hex", "000000000000000000000000C4C902FF");
		bothRow.set("min_ipv6_hex",  "200105A03B0200000000000000000000");
		bothRow.set("max_ipv6_hex",  "200105A03B020000FFFFFFFFFFFFFFFF");
		combiner.addInput(accumulator2, bothRow);
		
		// merge the accumulators
		NavigableMap<String, TableRow> mergedAccumulator = combiner.mergeAccumulators(accumulatorList);
		
		// get the output from the combiner and do our assertions on it
		NavigableMap<String, TableRow> output = combiner.extractOutput(mergedAccumulator);
		
		// ensure nothing matches if out of range. 
		assertNull(output.floorEntry("0000000000000000000000003056C8C0"));
		
		// ensure ipv4Row matches for its IPv4 hex
		assertEquals(ipv4Row, output.floorEntry("0000000000000000000000004056C8C0").getValue());
		assertEquals(ipv4Row, output.floorEntry("0000000000000000000000004056C8C1").getValue());
		assertEquals(ipv4Row, output.floorEntry("0000000000000000000000004056C8FF").getValue());
		
		// ensure ipv6Row matches for its IPv6 hex
		assertEquals(ipv6Row, output.floorEntry("24040138400900000000000000000000").getValue());
		assertEquals(ipv6Row, output.floorEntry("2404013840090000E000000000000000").getValue());
		assertEquals(ipv6Row, output.floorEntry("2404013840090000FFFFFFFFFFFFFFFF").getValue());
		
		// ensure bothRow matches for its IPv4 hex
		assertEquals(bothRow, output.floorEntry("000000000000000000000000C4C902C0").getValue());
		assertEquals(bothRow, output.floorEntry("000000000000000000000000C4C902D0").getValue());
		assertEquals(bothRow, output.floorEntry("000000000000000000000000C4C902FF").getValue());
		
		// enusre bothRow matches for its IPv6 hex
		assertEquals(bothRow, output.floorEntry("200105A03B0200000000000000000000").getValue());
		assertEquals(bothRow, output.floorEntry("200105A03B0200000000000000000001").getValue());
		assertEquals(bothRow, output.floorEntry("200105A03B020000FFFFFFFFFFFFFFFF").getValue());
	}
}
