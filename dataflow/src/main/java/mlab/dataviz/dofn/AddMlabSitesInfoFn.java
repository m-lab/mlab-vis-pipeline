package mlab.dataviz.dofn;

import java.util.NavigableMap;

import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

/**
 * DoFn that adds MLab site info to a row based on a PCollectionView passed in
 * as side input. It uses as NavigableMap to efficiently match IPs in a range.
 * It adds in:
 * 
 * Adds in mlab site ID, lat, long, city, region, country, continent
 *
 * @author pbeshai
 *
 */
public class AddMlabSitesInfoFn extends BaseIPMatchingFn {
	
	private static final long serialVersionUID = 1L;
	
	private static final String IP_FAMILY_KEY = "server_ip_family";
	private static final String IP_BASE64_KEY = "server_ip_base64";
	private static final String MLAB_MIN_IPV4_KEY = "min_ip_hex";
	private static final String MLAB_MAX_IPV4_KEY = "max_ip_hex";
	private static final String MLAB_MIN_IPV6_KEY = "min_ipv6_hex";
	private static final String MLAB_MAX_IPV6_KEY = "max_ipv6_hex";
	
	private static final String MLAB_SITE_KEY = "site";
	private static final String MLAB_LATITUDE_KEY = "latitude";
	private static final String MLAB_LONGITUDE_KEY = "longitude";
	private static final String MLAB_CITY_KEY = "city";
	private static final String MLAB_REGION_CODE_KEY = "region_code";
	private static final String MLAB_COUNTRY_CODE_KEY = "country_code";
	private static final String MLAB_CONTINENT_CODE_KEY = "continent_code";

	/**
	 * Main constructor for the DoFn
	 *
	 * @param mlabSitesView
	 */
	public AddMlabSitesInfoFn(PCollectionView<NavigableMap<String, TableRow>> mlabSitesView) {
		super(mlabSitesView, 
			  IP_FAMILY_KEY,
			  IP_BASE64_KEY,
			  MLAB_MIN_IPV4_KEY,
			  MLAB_MAX_IPV4_KEY,
			  MLAB_MIN_IPV6_KEY,
			  MLAB_MAX_IPV6_KEY);
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
	protected void addInfoToRow(TableRow dataRow, TableRow mlabSite) {
		dataRow.set("mlab_site_id", mlabSite.get(MLAB_SITE_KEY));
		addIfAvailable(dataRow, mlabSite, "server_latitude", MLAB_LATITUDE_KEY);
		addIfAvailable(dataRow, mlabSite, "server_longitude", MLAB_LONGITUDE_KEY);
		addIfAvailable(dataRow, mlabSite, "server_city", MLAB_CITY_KEY);
		addIfAvailable(dataRow, mlabSite, "server_region_code", MLAB_REGION_CODE_KEY);
		addIfAvailable(dataRow, mlabSite, "server_country_code", MLAB_COUNTRY_CODE_KEY);
		addIfAvailable(dataRow, mlabSite, "server_continent_code", MLAB_CONTINENT_CODE_KEY);
	}
}
