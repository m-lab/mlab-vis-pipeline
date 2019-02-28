package mlab.dataviz.transform;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.beam.sdk.transforms.Combine.CombineFn;

import com.google.api.services.bigquery.model.TableRow;

/**
 * Combines IPv4 and IPv6 address ranges in a single map based on their hex encoding
 * Maps the IP range to AS table row (e.g. from mlab_sites or maxmind_asn)
 *
 * Note - for this to work correctly, the hex values must have the same length.
 * See https://github.com/m-lab/mlab-vis-pipeline/issues/1
 *
 * @author pbeshai
 *
 */
public class CombineAsNavigableMapHex extends CombineFn<TableRow, NavigableMap<String, TableRow>, NavigableMap<String, TableRow>>  {

	private static final long serialVersionUID = 1L;
	
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
		// combine everything into a single map
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
