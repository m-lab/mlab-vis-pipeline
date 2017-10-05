package mlab.dataviz.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import mlab.dataviz.util.Formatters;

public class DateRange {
	private long startDate;
	private long endDate;
	private SimpleDateFormat dtf = new SimpleDateFormat(Formatters.TIMESTAMP);
	
	public DateRange(long startDate, long endDate) throws ParseException {
		this.startDate = startDate;
		this.endDate = endDate;
	}
	
	public String getStartRangeLongStr() {
		return String.valueOf((this.startDate / 1000) - Instant.EPOCH.getEpochSecond());
	}
	
	public String getEndRangeLongStr() {
		return String.valueOf((this.endDate / 1000) - Instant.EPOCH.getEpochSecond());
	}
	
	public String getDisplayStartTimestampStr() {
		Date d = new Date((long) (this.startDate));
		return dtf.format(d);
	}
	
	public String getDisplayEndTimestampStr() {
		Date d = new Date((long) (this.endDate));
		return dtf.format(d);
	}
	
	public boolean equals(DateRange d) {
		return (d.startDate == this.startDate) &&
				(d.endDate == this.endDate);
	}
}
