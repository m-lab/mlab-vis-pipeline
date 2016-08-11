package mlab.bocoup.transform;

import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;

/**
 * Combines IPv4 and IPv6 address ranges in a single map based on their hex encoding
 * Maps the IP range to AS table row (e.g. from mlab_sites or maxmind_asn)
 * @author pbeshai
 *
 */
public class CombineAsNavigableMapHex extends CombineFn<TableRow, NavigableMap<String, TableRow>, NavigableMap<String, TableRow>>  {
	String name = "CombineAsNavigableMapHex";
	
	@Override
	public NavigableMap<String, TableRow> createAccumulator() {
		return new TreeMap<String, TableRow>();
	}

	@Override
	public NavigableMap<String, TableRow> addInput(NavigableMap<String, TableRow> accumulator, TableRow input) {
		// add in IPv6 to the map if available
		String ipMin = (String) input.get("min_ipv6_hex");
		if (ipMin != null && ipMin.length() > 0) {
			accumulator.put(ipMin, input);
		}
		
		// add in IPv4 to the map if available (this can also be IPv6 as in the case of maxmind)
		ipMin = (String) input.get("min_ip_hex");
		if (ipMin != null && ipMin.length() > 0) {
			accumulator.put(ipMin, input);
		}
		
		return accumulator;
	}

	@Override
	public NavigableMap<String, TableRow> mergeAccumulators(Iterable<NavigableMap<String, TableRow>> accumulators) {
		NavigableMap<String, TableRow> finalMap = this.createAccumulator();
		
		for (NavigableMap<String, TableRow> accumulator : accumulators) {
			finalMap.putAll(accumulator);
		}
		
		return finalMap;
	}

	@Override
	public NavigableMap<String, TableRow> extractOutput(NavigableMap<String, TableRow> accumulator) {
		return accumulator;
	}
}
