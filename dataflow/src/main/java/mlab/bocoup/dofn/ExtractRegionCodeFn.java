package mlab.bocoup.dofn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

public class ExtractRegionCodeFn extends DoFn<TableRow, KV<String, TableRow>> {

	private static final Logger LOG = LoggerFactory.getLogger(ExtractRegionCodeFn.class);

	@Override
	public void processElement(DoFn<TableRow, KV<String, TableRow>>.ProcessContext c) throws Exception {
		TableRow row = c.element();
		String country = (String) row.get("country_code");
		String region = (String) row.get("region_code");

		if ((country != null) && (region != null)) {
			c.output(KV.of(country + "_" + region, row));
		} else {
			LOG.error("Key not found for row " + row);
		}
	}
}
