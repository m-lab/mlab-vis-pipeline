package mlab.dataviz.dofn;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.util.Formatters;
import mlab.dataviz.util.TimezoneMapper;

public class AddLocalTimeFn extends DoFn<TableRow, TableRow> {
	
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AddLocalTimeFn.class);
	
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP2);
	private static SimpleDateFormat dateOutputFormatter = new SimpleDateFormat(Formatters.TIMESTAMP_TIMEZONE);
	private static Calendar cal = Calendar.getInstance();
	
	private PCollectionView<Map<String, Iterable<TableRow>>> groupedZones;

	public AddLocalTimeFn() {}
	
	public AddLocalTimeFn(PCollectionView<Map<String, Iterable<TableRow>>> groupedZones) {
		this.groupedZones = groupedZones;
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
	
	public LocalTimeDetails utcToLocalTime(Iterable<TableRow> rows, String tz, String timestamp, double lat, double lon) throws Exception {

		Date date = dateFormatter.parse(timestamp);
		Iterator<TableRow> rowsIterator = rows.iterator();
		
		// Within the rows, find the appropriate row with the right
		// offset. Each row corresponds to a year. There are rows into the
		// future as well.
		int gmtOffset = 0;
		TableRow row = null;
		long dateLong = date.getTime();
		
		while (rowsIterator.hasNext()) {
			row = rowsIterator.next();
			long rowTimestart = Long.valueOf((String) row.get("time_start"));
			if (rowTimestart > (dateLong / 1000)) {
				break;
			}
			gmtOffset = Integer.valueOf((String) row.get("gmt_offset_seconds"));
		}
		int hours = gmtOffset / 60 / 60;
		cal.setTime(date);
		cal.add(Calendar.HOUR_OF_DAY, hours);
		
		return new LocalTimeDetails(dateOutputFormatter.format(cal.getTime()), (String) row.get("timezone_name"), tz, hours);
	}
	
	@ProcessElement
	public void processElement(DoFn<TableRow, TableRow>.ProcessContext c) throws Exception {
		
		TableRow dataRow = c.element().clone();
		
		if (dataRow.containsKey("client_latitude") && dataRow.containsKey("client_longitude") ) {
			String testDate = (String) dataRow.get("test_date");
			double lat = (Double) dataRow.get("client_latitude");
			double lng = (Double) dataRow.get("client_longitude");
			
			Map<String, Iterable<TableRow>> timezonesMap = c.sideInput(this.groupedZones);
			
			String timezoneName = TimezoneMapper.latLngToTimezoneString(lat, lng);
			Iterable<TableRow> rows = timezonesMap.get(timezoneName);
			
			if (timezonesMap.containsKey(timezoneName)) {
				LocalTimeDetails details = this.utcToLocalTime(rows, timezoneName, testDate, lat, lng); 
				
				dataRow.set("local_test_date", details.getTimestamp());
				dataRow.set("local_time_zone", details.getTimezone());
				dataRow.set("local_zone_name", details.getZone());
				dataRow.set("local_gmt_offset", details.getOffset());
			} else {
				LOG.error("No entries for " + timezoneName);
			}
			
		}
		
		
		
		c.output(dataRow);
	}
}
