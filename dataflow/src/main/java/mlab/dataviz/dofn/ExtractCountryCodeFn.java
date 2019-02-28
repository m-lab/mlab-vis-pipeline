package mlab.dataviz.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class ExtractCountryCodeFn extends DoFn<TableRow, KV<String, TableRow>> {
	
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(ExtractCountryCodeFn.class);
	
	@ProcessElement
	public void processElement(ProcessContext c) throws Exception {
		TableRow row = c.element();
		String key = (String) row.get("country_code");
		if (key != null) {
			c.output(KV.of(key, row));
		} else {
			LOG.error("Key not found for row " + row);
		}
	}
}
