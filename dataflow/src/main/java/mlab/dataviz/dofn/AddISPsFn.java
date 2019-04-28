package mlab.dataviz.dofn;

import java.util.NavigableMap;

import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

/**
 * DoFn that adds AS information to a row based on a PCollectionView passed in
 * as side input. It uses as NavigableMap to efficiently match IPs in a range.
 *
 * @author pbeshai
 *
 */
public class AddISPsFn extends BaseIPMatchingFn {
	
	private static final long serialVersionUID = 1L;
	
	/** Key in the data row to save the AS name to */
	protected String dataASNNameKey;
	/** Key in the data row to save the AS number to (optional) */
	protected String dataASNNumberKey;
	
	/** Key in the AS row to look up the AS name from */
	protected String asnNameKey;
	/** Key in the AS row to look up the AS number from (optional) */
	protected String asnNumberKey;

	/**
	 * Main constructor for the DoFn
	 *
	 * @param asnView
	 * @param dataIPFamilyKey
	 * @param dataIPBase64Key
	 * @param asnMinIPv4Key
	 * @param asnMaxIPv4Key
	 * @param asnMinIPv6Key
	 * @param asnMaxIPv6Key
	 * @param dataASNNameKey
	 * @param dataASNNumberKey
	 * @param asnNameKey
	 * @param asnNumberKey
	 */
	public AddISPsFn(PCollectionView<NavigableMap<String, TableRow>> asnView, String dataIPFamilyKey,
			String dataIPBase64Key, String asnMinIPv4Key, String asnMaxIPv4Key, String asnMinIPv6Key,
			String asnMaxIPv6Key, String dataASNNameKey, String dataASNNumberKey, String asnNameKey,
			String asnNumberKey) {
		super(asnView, dataIPFamilyKey, dataIPBase64Key, asnMinIPv4Key, asnMaxIPv4Key, asnMinIPv6Key, asnMaxIPv6Key);
		this.dataASNNameKey = dataASNNameKey;
		this.dataASNNumberKey = dataASNNumberKey;
		this.asnNameKey = asnNameKey;
		this.asnNumberKey = asnNumberKey;
	}
	
	/**
	 * Helper function to add AS information to the data row if a match is
	 * found.
	 *
	 * @param dataRow
	 *            The row to add AS info to
	 * @param asn
	 *            The AS row
	 */
	@Override
	protected void addInfoToRow(TableRow dataRow, TableRow asn) {
		// add in the AS name
		dataRow.set(this.dataASNNameKey, asn.get(this.asnNameKey));
		
		// add in the AS number if configured
		if (this.asnNumberKey != null && this.dataASNNumberKey != null) {
			dataRow.set(this.dataASNNumberKey, asn.get(this.asnNumberKey));
		}
	}
}
