package mlab.bocoup.util;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.opencsv.CSVReader;

import mlab.bocoup.query.BigQueryJob;

public class TimeLocalizer {
	private static final Logger LOG = LoggerFactory.getLogger(TimeLocalizer.class);

	// rows shape: "zone_id","abbreviation","time_start","gmt_offset","dst"
	private static final String TIMEZONE_FILE = "./data/bigquery/timezonedb/merged_timezone.csv";

	private static String PROJECT_ID = "mlab-oti";
	private static String BQ_TIMEZONE_TABLE = "bocoup.localtime_timezones";
	private static String[] TIMEZONE_FIELDS = {"zone_name", "timezone_name", "time_start", "gmt_offset_seconds", "dst_flag"};

	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP2);
	private static SimpleDateFormat dateOutputFormatter = new SimpleDateFormat(Formatters.TIMESTAMP_TIMEZONE);
	private static Calendar cal = Calendar.getInstance();

	private static Map<String, List<String[]>> timezoneMap = new HashMap<String, List<String[]>>();

	public static final boolean LOCAL_MODE = false;
	public static final boolean BQ_MODE = true;
	private static boolean mode = LOCAL_MODE;
	
	public TimeLocalizer setMode(boolean mode) {
		this.mode = mode;
		return this;
	}
	
	private void buildMaps(List<String[]> timezones) {
		
		// build a map from timezones, from ID to the rows relevant to it.
		Iterator<String[]> timezoneLinesIterator = timezones.iterator();

		while (timezoneLinesIterator.hasNext()) {
			String[] entries = timezoneLinesIterator.next();
			String zoneName = entries[0];
			if (timezoneMap.containsKey(zoneName)) {
				List<String[]> row = timezoneMap.get(entries[0]);
				row.add(entries);
				timezoneMap.put(zoneName, row);
			} else {
				List<String[]> rows = new ArrayList<String[]>();
				rows.add(entries);
				timezoneMap.put(zoneName, rows);
			}
		
		}
		
	}
	
	/**
	 * @private
	 * Instantiates zone and timezone maps.
	 */
	private void instantiateLocalMaps() {
		CSVReader timezoneReader;

		try {
			timezoneReader = new CSVReader(new FileReader(TIMEZONE_FILE));

			// build a map from timezones, key is timezone name, value is ID
			List<String[]> timezoneLines = timezoneReader.readAll();
			
			buildMaps(timezoneLines);

		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}
	
	/**
	 * Converts iterators into arrays of strings for postprocessing.
	 * @param zoneIterator
	 * @param timezoneIterator
	 */
	private void handleBQIterators(Iterator<TableRow> timezoneIterator) {
		List<String[]> timezonesStringRows = new ArrayList<String[]>();
		
		while (timezoneIterator.hasNext()) {
			TableRow row = timezoneIterator.next();
			
			String[] strRow = new String[TIMEZONE_FIELDS.length];
			int fieldIdx = 0;
			for (TableCell cell : row.getF()) {
		        strRow[fieldIdx++] = cell.getV().toString();
		     }
			timezonesStringRows.add(strRow);
		}
		
		LOG.info("Setup reference timezone tables. Timezone count: " + timezonesStringRows.size());
		buildMaps(timezonesStringRows);
	}
	
	/**
	 * Initializes zone information from big query tables.
	 * @throws IOException
	 */
	private void instantiateBqMaps() throws IOException {
		BigQueryJob bqj = new BigQueryJob(PROJECT_ID);
		String getTimezonesQuery = "select * from " + BQ_TIMEZONE_TABLE;
		List<TableRow> timezones = bqj.executePaginatedQuery(getTimezonesQuery);
		handleBQIterators(timezones.iterator());
	}
	
	/**
	 * @constructor
	 * Default constructor. Call to initialize, followed by setMode and setup.
	 */
	public TimeLocalizer() {}

	public TimeLocalizer setup() throws IOException {
		if (this.mode == LOCAL_MODE) {
			instantiateLocalMaps();
		} else {
			instantiateBqMaps();
		}
		return this;
	}

	/**
	 * Compact representation of a timestamp that includes the
	 * timezone, location
	 * @author iros
	 *
	 */
	public class LocalTimeDetails {
		private String timestamp;
		private String timezone;
		private String zone;
		private int offset;
		
		public LocalTimeDetails(String timestamp, String timezone, String location, int offset) {
			this.setTimestamp(timestamp);
			this.setTimezone(timezone);
			this.setZone(location);
			this.setOffset(offset);
		}

		public String getZone() {
			return zone;
		}

		public void setZone(String location) {
			this.zone = location;
		}

		public String getTimezone() {
			return timezone;
		}

		public void setTimezone(String timezone) {
			this.timezone = timezone;
		}

		public String getTimestamp() {
			return this.timestamp;
		}

		public void setTimestamp(String timestamp) {
			this.timestamp = timestamp;
		}
		
		public int getOffset() {
			return offset;
		}

		public void setOffset(int offset) {
			this.offset = offset;
		}
		
		public String toString() {
			return this.getTimestamp() + " " + this.getTimezone() + " " + this.getZone() + " ("+this.getOffset()+")";
		}
	}
	
	/**
	 * Returns the timezone associated with latitude and longitude
	 * @param lat
	 * @param lng
	 * @return
	 */
	public String getTimeZone(double lat, double lng) {
		return TimezoneMapper.latLngToTimezoneString(lat, lng);
	}

	/**
	 * Convert a timestamp and lat/lon coordinates into the local time
	 * for a location
	 * @param timestamp String  timestamp
	 * @param lat  double latitude
	 * @param lon  double longitude
	 * @return localTimestamp LocalTimeDetails
	 * @throws Exception if the timestamp can't be parsed
	 */
	public LocalTimeDetails utcToLocalTime(String timestamp, double lat, double lon) throws Exception {
		String tz = this.getTimeZone(lat, lon);
		Date date = dateFormatter.parse(timestamp);

		// find corresponding offset rows
		List<String[]> tzRows = timezoneMap.get(tz);
		Iterator<String[]> tzRowsIterator = tzRows.iterator();

		// Within the rows, find the appropriate row with the right
		// offset. Each row corresponds to a year. There are rows into the
		// future as well.
		int gmtOffset = 0;
		String[] row = null;
		long timeStart = 0;
		while (tzRowsIterator.hasNext()) {
			row = tzRowsIterator.next();
			
			if (Long.valueOf(row[2]) > (date.getTime() / 1000)) {
				System.out.println(timeStart + " " + (date.getTime() / 1000));
				break;
			}
			timeStart = Long.valueOf(row[2]);
			gmtOffset = Integer.valueOf(row[3]);
		}
		int hours = gmtOffset / 60 / 60;
		cal.setTime(date);
		cal.add(Calendar.HOUR_OF_DAY, hours);
		
		return new LocalTimeDetails(dateOutputFormatter.format(cal.getTime()), row[1], tz, hours);
	}

	public static void main(String[] args) throws NumberFormatException, Exception {

		TimeLocalizer tl = new TimeLocalizer().setMode(BQ_MODE).setup();

		List<List<String>> sampleTimes = new ArrayList<List<String>>();
		List<String> t1 = Arrays.asList("San Francisco", "2015-05-29 10:00:00 UTC", "37.791500091552734",
				"-122.40889739990234", "2015-05-29 3:00:00");
		sampleTimes.add(t1);
		List<String> t2 = Arrays.asList("Chicago", "2014-12-01 12:00:00 UTC", "41.92060089111328", "-87.70169830322266",
				"2014-12-01 6:00:00");
		sampleTimes.add(t2);
		// no dst
		List<String> t3 = Arrays.asList("Paris", "2013-02-27 23:12:00 UTC", "48.88169860839844", "2.382200002670288",
				"2013-02-28 00:12:00");
		sampleTimes.add(t3);
		// dst
		List<String> t4 = Arrays.asList("Paris", "2013-04-27 23:12:00 UTC", "48.88169860839844", "2.382200002670288",
				"2013-04-28 01:12:00");
		sampleTimes.add(t4);
		
		List<String> t5 = Arrays.asList("???", "2016-04-10 00:00:00 UTC", "33.003501892089844", "-96.9000015258789", "");
		sampleTimes.add(t5);
		
		Iterator<List<String>> i = sampleTimes.iterator();
		while (i.hasNext()) {
			List<String> row = i.next();
			LocalTimeDetails ltd = tl.utcToLocalTime(row.get(1), Double.valueOf(row.get(2)), Double.valueOf(row.get(3)));
			
			System.out.println(row.get(0) + " " + tl.getTimeZone(Double.valueOf(row.get(2)), Double.valueOf(row.get(3)))
					+ " starting: " + row.get(1) 
					+ " computed: " + ltd.getTimestamp() + "("+ltd.getOffset()+")"
					+ " expected: " + row.get(4));

		}
	}

}
