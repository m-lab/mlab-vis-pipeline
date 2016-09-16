package mlab.bocoup.dofn;

import java.util.Map;
import java.util.NavigableMap;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

/**
 * DoFn that adds AS information to a row based on a PCollectionView passed in
 * as side input. It uses as NavigableMap to efficiently match IPs in a range.
 *
 * @author pbeshai
 *
 */
public class AddISPsFn extends DoFn<TableRow, TableRow> {
	private static final Logger LOG = LoggerFactory.getLogger(AddISPsFn.class);

	// values for the IP family field
	private static final int IP_FAMILY_IPV4 = 2;
	private static final int IP_FAMILY_IPV6 = 10;

	/** Side input view NavigableMap from min IP to AS data */
	private PCollectionView<NavigableMap<String, TableRow>> asnView;

	/**
	 * Key in the data row to look up the IP family (IPV4 or IPV6 as int)
	 */
	private String dataIPFamilyKey;

	/**
	 * The base64 encoded IP key
	 */
	private String dataIPBase64Key;

	/** Key in the data row to save the AS name to */
	private String dataASNNameKey;

	/** Key in the data row to save the AS number to (optional) */
	private String dataASNNumberKey;

	/** Key in the AS row to look up the min IP hex from */
	private String asnMinIPv4Key;

	/** Key in the AS row to look up the max IP hex from */
	private String asnMaxIPv4Key;

	/** Key in the AS row to look up the min IPv6 hex from */
	private String asnMinIPv6Key;

	/** Key in the AS row to look up the max IPv6 hex from */
	private String asnMaxIPv6Key;

	/** Key in the AS row to look up the AS name from */
	private String asnNameKey;

	/** Key in the AS row to look up the AS number from (optional) */
	private String asnNumberKey;

	/**
	 * Main constructor for the DoFn
	 *
	 * @param asnView
	 * @param dataIPFamilyKey
	 * @param dataASNNameKey
	 * @param dataASNNumberKey
	 * @param asnMinIPv4Key
	 * @param asnMaxIPv4Key
	 * @param asnNameKey
	 * @param asnNumberKey
	 */
	public AddISPsFn(PCollectionView<NavigableMap<String, TableRow>> asnView, String dataIPFamilyKey, String dataIPBase64Key,
			String dataASNNameKey, String dataASNNumberKey, String asnMinIPv4Key, String asnMaxIPv4Key,
			String asnMinIPv6Key, String asnMaxIPv6Key, String asnNameKey, String asnNumberKey) {
		super();
		this.asnView = asnView;
		this.dataIPFamilyKey = dataIPFamilyKey;
		this.dataIPBase64Key = dataIPBase64Key;
		this.dataASNNameKey = dataASNNameKey;
		this.dataASNNumberKey = dataASNNumberKey;
		this.asnMinIPv4Key = asnMinIPv4Key;
		this.asnMaxIPv4Key = asnMaxIPv4Key;
		this.asnMinIPv6Key = asnMinIPv6Key;
		this.asnMaxIPv6Key = asnMaxIPv6Key;
		this.asnNameKey = asnNameKey;
		this.asnNumberKey = asnNumberKey;
	}

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
	 * The main function for matching AS based on data row IP and the IP range
	 * the AS matches.
	 *
	 * This adds the AS to the data row in the columns specified by
	 * dataASNNameKey and dataASNNumberKey.
	 */
	@Override
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
					addASNInformation(dataRow, asn);
				} 
			}
		}

		// output the row with the new data added to it.
		c.output(dataRow);
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
	private void addASNInformation(TableRow dataRow, TableRow asn) {
		// add in the AS name
		dataRow.set(this.dataASNNameKey, asn.get(this.asnNameKey));
		
		// add in the AS number if configured
		if (this.asnNumberKey != null && this.dataASNNumberKey != null) {
			dataRow.set(this.dataASNNumberKey, asn.get(this.asnNumberKey));
		}
	}

}
