package mlab.dataviz.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class ExtractAsnNumFn extends DoFn<TableRow, KV<String, TableRow>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ExtractAsnNumFn.class);

	@ProcessElement
	public void processElement(DoFn<TableRow, KV<String, TableRow>>.ProcessContext c) throws Exception {
		TableRow row = c.element();
		String asnNum = (String) row.get("asn_number");

		if (asnNum != null) {
			c.output(KV.of(asnNum, row));
		} else {
			LOG.error("Key not found for row " + row);
		}
	}
	
}
