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
		
		// test having only ipv4 hex
		TableRow ipv4Row = new TableRow();
		ipv4Row.set("min_ip_hex", "1000");
		ipv4Row.set("max_ip_hex", "2000");
		combiner.addInput(accumulator1, ipv4Row);
		
		// test having only ipv6 hex
		TableRow ipv6Row = new TableRow();
		ipv6Row.set("min_ipv6_hex",  "100000000000000000");
		ipv6Row.set("max_ipv6_hex",  "200000000000000000");
		combiner.addInput(accumulator1, ipv6Row);
		
		// test having both hexes
		TableRow bothRow = new TableRow();
		bothRow.set("min_ip_hex", "2001");
		bothRow.set("max_ip_hex", "3000");
		bothRow.set("min_ipv6_hex",  "500000000000000000");
		bothRow.set("max_ipv6_hex",  "800000000000000000");
		combiner.addInput(accumulator2, bothRow);
		
		// merge the accumulators
		NavigableMap<String, TableRow> mergedAccumulator = combiner.mergeAccumulators(accumulatorList);
		
		// get the output from the combiner and do our assertions on it
		NavigableMap<String, TableRow> output = combiner.extractOutput(mergedAccumulator);
		
		// ensure nothing matches if out of range. 
		assertNull(output.floorEntry("0999"));
		
		// ensure ipv4Row matches for its IPv4 hex
		assertEquals(ipv4Row, output.floorEntry("1000").getValue());
		assertEquals(ipv4Row, output.floorEntry("1200").getValue());
		assertEquals(ipv4Row, output.floorEntry("2000").getValue());
		
		// ensure ipv6Row matches for its IPv6 hex
		assertEquals(ipv6Row, output.floorEntry("100000000000000000").getValue());
		assertEquals(ipv6Row, output.floorEntry("110000000000000000").getValue());
		assertEquals(ipv6Row, output.floorEntry("200000000000000000").getValue());
		
		// ensure bothRow matches for its IPv4 hex
		assertEquals(bothRow, output.floorEntry("2001").getValue());
		assertEquals(bothRow, output.floorEntry("2150").getValue());
		assertEquals(bothRow, output.floorEntry("3000").getValue());
		
		// enusre bothRow matches for its IPv6 hex
		assertEquals(bothRow, output.floorEntry("500000000000000000").getValue());
		assertEquals(bothRow, output.floorEntry("600000000000000000").getValue());
		assertEquals(bothRow, output.floorEntry("800000000000000000").getValue());
	}
}
