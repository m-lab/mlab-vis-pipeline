package mlab.bocoup.dofn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;


/**
 * Extracts the zone name key 
 * @author iros
 *
 */
public class ExtractZoneKeynameFn extends DoFn<TableRow, KV<String, TableRow>> {
	private static final Logger LOG = LoggerFactory.getLogger(ExtractZoneKeynameFn.class);
	
	@Override
	public void processElement(ProcessContext c) throws Exception {
		TableRow row = c.element();
		String key = (String) row.get("zone_name");
		if (key != null) {
			c.output(KV.of(key, row));
		} else {
			LOG.error("Key not found for row " + row);
		}
	}
}