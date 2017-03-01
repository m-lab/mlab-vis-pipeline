package mlab.dataviz.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import mlab.dataviz.data.DateRange;

public class TimeChunk {

	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP);
	private static Calendar cal = Calendar.getInstance();

	/**
	 * Creates a predetermined number ranges of unix dates that fall between two dates. They are
	 * equally sized.
	 * @param startDate String - start date of the format {@link mlab.dataviz.util.Formatters.TEST_DATE}
	 * @param endDate String - start date of the format {@link mlab.dataviz.util.Formatters.TEST_DATE}
	 * @param numberOfDays int - number of days between ranges
	 * @return dates ArrayList<mlab.dataviz.data.DateRange>
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
	 * @param startDate String - start date of the format {@link mlab.dataviz.util.Formatters.TEST_DATE}
	 * @param endDate String - start date of the format {@link mlab.dataviz.util.Formatters.TEST_DATE}
	 * @param numberOfChunks int - number of chunks to create.
	 * @return dates ArrayList<mlab.dataviz.data.DateRange>
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
}
