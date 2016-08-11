package mlab.bocoup.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;

import mlab.bocoup.data.DateRange;

public class TimeChunk {
	
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP);
	private static Calendar cal = Calendar.getInstance();
	
	/**
	 * Creates a predetermined number ranges of unix dates that fall between two dates. They are
	 * equally sized.
	 * @param startDate String - start date of the format {@link mlab.bocoup.util.Formatters.TEST_DATE}
	 * @param endDate String - start date of the format {@link mlab.bocoup.util.Formatters.TEST_DATE}
	 * @param numberOfDays int - number of days between ranges
	 * @return dates ArrayList<mlab.bocoup.data.DateRange>
	 * @throws ParseException - if date cannot be parsed.
	 */
	public static ArrayList<DateRange> byNumberOfDays(String startDate, String endDate, int numberOfDays) throws ParseException {
		ArrayList<DateRange> dates = new ArrayList<DateRange>();
		
		
		// convert to unix
		long startDateUnix = dateFormatter.parse(startDate).getTime();
		long endDateUnix = dateFormatter.parse(endDate).getTime();
		
		while (startDateUnix < endDateUnix) {
			
			// compute next chunk end date
			long nextDate = _addDays(startDateUnix, numberOfDays);
			if (nextDate > endDateUnix) {
				nextDate = endDateUnix;
			}
			
			DateRange r = new DateRange(startDateUnix, nextDate);
			dates.add(r);
			
			// update startDateUnix to move it forward.
			startDateUnix = nextDate + 1;
		}
		
		return dates;
	}
	
	/**
	 * Creates a set of ranges of unix dates that are incremented by number of days between
	 * two end dates
	 * @param startDate String - start date of the format {@link mlab.bocoup.util.Formatters.TEST_DATE}
	 * @param endDate String - start date of the format {@link mlab.bocoup.util.Formatters.TEST_DATE}
	 * @param numberOfChunks int - number of chunks to create.
	 * @return dates ArrayList<mlab.bocoup.data.DateRange>
	 * @throws ParseException - if date cannot be parsed.
	 */
	public static ArrayList<DateRange> intoNumberOfChunks(String startDate, String endDate, int numberOfChunks) throws ParseException {
		
		ArrayList<DateRange> dates = new ArrayList<DateRange>();

		// convert to unix
		long startDateUnix = dateFormatter.parse(startDate).getTime();
		long endDateUnix = dateFormatter.parse(endDate).getTime();
		
		// figure out time chunk size
		long diff = (endDateUnix - startDateUnix) / numberOfChunks;
		
		while (startDateUnix < endDateUnix) {
			// compute next date
			long nextDate = startDateUnix + diff;
			
			DateRange r = new DateRange(startDateUnix, nextDate);
			dates.add(r);
			
			// update startDateUnix to move it forward.
			startDateUnix = nextDate + 1;
		}
		
		return dates;
	}
	
	private static long _addDays(long fromDateUnix, int numOfDays) {
		cal.setTimeInMillis(fromDateUnix);
		cal.add(Calendar.DATE, (int) numOfDays);
		return cal.getTimeInMillis();
	}
	
	// example usage:
	public static void main(String[]args) throws ParseException {
		
		String startDate = "2015-01-01T00:00:00Z";
		String finalDate = "2015-01-08T00:00:00Z";
		int numberOfDays = 40;
		
		ArrayList<DateRange> dates = TimeChunk.byNumberOfDays(startDate, finalDate, numberOfDays);
		Iterator<DateRange> i = dates.iterator();
		System.out.println("by number of days:");
		while (i.hasNext()) {
			DateRange dr = i.next();
			System.out.println(dr.getStartRangeLongStr() + " - " + dr.getEndRangeLongStr());
			System.out.println(dr.getDisplayStartTimestampStr() + " - " + dr.getDisplayEndTimestampStr());
		}
		
		dates = TimeChunk.intoNumberOfChunks(startDate, finalDate, 6);
		i = dates.iterator();
		System.out.println("Into number of chunks:");
		while (i.hasNext()) {
			DateRange dr = i.next();
			System.out.println(dr.getStartRangeLongStr() + " - " + dr.getEndRangeLongStr());
			System.out.println(dr.getDisplayStartTimestampStr() + " - " + dr.getDisplayEndTimestampStr());
		}
	}
}
