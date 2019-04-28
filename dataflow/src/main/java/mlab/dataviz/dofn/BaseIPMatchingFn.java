package mlab.dataviz.dofn;

import java.util.Map;
import java.util.NavigableMap;

import javax.xml.bind.DatatypeConverter;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

public class BaseIPMatchingFn extends DoFn<TableRow, TableRow> {

	private static final long serialVersionUID = 1L;
	
	private static final int IP_FAMILY_IPV6 = 1;
	
	/** Side input view NavigableMap from min IP to AS data */
	protected PCollectionView<NavigableMap<String, TableRow>> asnView;
	/**
	 * Key in the data row to look up the IP family (IPV4 or IPV6 as int)
	 */
	protected String dataIPFamilyKey;
	/**
	 * The base64 encoded IP key
	 */
	protected String dataIPBase64Key;
	/** Key in the AS row to look up the min IP hex from */
	protected String asnMinIPv4Key;
	/** Key in the AS row to look up the max IP hex from */
	protected String asnMaxIPv4Key;
	/** Key in the AS row to look up the min IPv6 hex from */
	protected String asnMinIPv6Key;
	/** Key in the AS row to look up the max IPv6 hex from */
	protected String asnMaxIPv6Key;
	
	/**
	 * Converts a packed IP in base64 to base16. Needed to maintain sort order
	 *
	 * @param base64Ip
	 *            The packed base64 string (result of PARSE_PACKED_IP in SQL)
	 * @return the IP as a base16 string
	 */
	public static String hexFromBase64(String base64Ip) {
		byte[] decoded = DatatypeConverter.parseBase64Binary(base64Ip);
		String hex = DatatypeConverter.printHexBinary(decoded);
		
		// ensure that the hex is the full 32 characters in length
		int hexLength = hex.length();
		if (hexLength < 32) {
			String zeros = "00000000000000000000000000000000";
			hex = zeros.substring(0,  32 - hexLength) + hex;
		}
		
		return hex;
	}

	/**
	 * Main constructor 
	 * @param asnView
	 * @param dataIPFamilyKey
	 * @param dataIPBase64Key
	 * @param asnMinIPv4Key
	 * @param asnMaxIPv4Key
	 * @param asnMinIPv6Key
	 * @param asnMaxIPv6Key
	 */
	public BaseIPMatchingFn(PCollectionView<NavigableMap<String, TableRow>> asnView, String dataIPFamilyKey,
			String dataIPBase64Key, String asnMinIPv4Key, String asnMaxIPv4Key, String asnMinIPv6Key,
			String asnMaxIPv6Key) {
		super();
		this.asnView = asnView;
		this.dataIPFamilyKey = dataIPFamilyKey;
		this.dataIPBase64Key = dataIPBase64Key;
		this.asnMinIPv4Key = asnMinIPv4Key;
		this.asnMaxIPv4Key = asnMaxIPv4Key;
		this.asnMinIPv6Key = asnMinIPv6Key;
		this.asnMaxIPv6Key = asnMaxIPv6Key;
	}
	
	/**
	 * The main function for matching AS based on data row IP and the IP range
	 * the AS matches.
	 *
	 * This adds the AS to the data row in the columns specified by
	 * dataASNNameKey and dataASNNumberKey.
	 */
	@ProcessElement
	public void processElement(DoFn<TableRow, TableRow>.ProcessContext c) throws Exception {
		// get the data row to modify
		TableRow dataRow = c.element().clone();
	
		// get the IP of the row
		String ipBase64 = (String) dataRow.get(this.dataIPBase64Key);
	
		// no IP configured, return the row as it is
		if (ipBase64 == null) {
			c.output(dataRow);
			return;
		}
	
		String asnMinKey, asnMaxKey;
		// check if it is IPv6
		if (Integer.valueOf((String) dataRow.get(this.dataIPFamilyKey)) == IP_FAMILY_IPV6) {
			// it is IPv6
			asnMinKey = this.asnMinIPv6Key;
			asnMaxKey = this.asnMaxIPv6Key;
		} else {
			// it is IPv4
			asnMinKey = this.asnMinIPv4Key;
			asnMaxKey = this.asnMaxIPv4Key;
		}
	
	
		// read in the navigable map from side input
		NavigableMap<String, TableRow> asnMap = c.sideInput(this.asnView);
	
		// we do have an IP, so look up the AS
		String ipHex = hexFromBase64(ipBase64);
	
		// find in map looking to match the min IP in the range
		Map.Entry<String, TableRow> asnEntry = asnMap.floorEntry(ipHex);
		if (asnEntry != null) {
			TableRow asn = asnEntry.getValue();
			// if an AS matches this IP, double check the IP is in range and
			// set value
			if (asn != null) {
				String asnMinIp = (String) asn.get(asnMinKey);
				String asnMaxIp = (String) asn.get(asnMaxKey);
	
				// if it matches the IP range, add the AS to the data row
				if (asnMinIp.compareTo(ipHex) <= 0 && asnMaxIp.compareTo(ipHex) >= 0) {
					addInfoToRow(dataRow, asn);
				} 
			}
		}
	
		// output the row with the new data added to it.
		c.output(dataRow);
	}

	protected void addIfAvailable(TableRow dataRow, TableRow mlabSite, String setKey, String readKey) {
		Object value = mlabSite.get(readKey);
		if (value != null && !(value instanceof String && ((String) value).length() == 0)) {
			dataRow.set(setKey,  value);
		}
	}
	
	/**
	 * Helper function to add AS information to the data row if a match is
	 * found. Should be overridden by subclasses
	 *
	 * @param dataRow
	 *            The row to add AS info to
	 * @param asn
	 *            The AS row
	 */
	protected void addInfoToRow(TableRow dataRow, TableRow asn) {
		/* template method */
	};

}