package mlab.bocoup.util;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import mlab.bocoup.data.DateRange;

public class TimeChunkTest {

	private String startDate = "2015-01-01T00:00:00Z";
	private String finalDate = "2015-01-08T00:00:00Z";
	private int numOfDays = 2;
	private static ArrayList<DateRange> expectedDates;
	private static ArrayList<StringPair> expectedDatesDisplay;
	
	private static class StringPair {
		private String a;
		private String b;
		
		StringPair(String a, String b) {
			this.a = a;
			this.b = b;
		}
		
		String getA() { return this.a; }
		String getB() { return this.b; }
	}
	
	@Test
	public void testByNumberOfDays() throws ParseException {
		
		expectedDates = new ArrayList<DateRange>();
		expectedDatesDisplay = new ArrayList<StringPair>();
		expectedDates.add(new DateRange(1420088400000L, 1420261200000L));
		expectedDates.add(new DateRange(1420261200001L, 1420434000001L));
		expectedDates.add(new DateRange(1420434000002L, 1420606800002L));
		expectedDates.add(new DateRange(1420606800003L, 1420693200000L));
		
		expectedDatesDisplay.add(new StringPair("2015-01-01T00:00:00Z", "2015-01-03T00:00:00Z"));
		expectedDatesDisplay.add(new StringPair("2015-01-03T00:00:00Z", "2015-01-05T00:00:00Z"));
		expectedDatesDisplay.add(new StringPair("2015-01-05T00:00:00Z", "2015-01-07T00:00:00Z"));
		expectedDatesDisplay.add(new StringPair("2015-01-07T00:00:00Z", "2015-01-08T00:00:00Z"));
		
		
		try {
			ArrayList<DateRange> dates = TimeChunk.byNumberOfDays(startDate, finalDate, numOfDays);
			
			assertEquals(dates.size(), 4);
			Iterator<DateRange> i = dates.iterator();
			int idx = 0;
			while (i.hasNext()) {
				DateRange d = i.next();
				StringPair sp = expectedDatesDisplay.get(idx);
				checkPublic(d, sp, idx++);
			}
		} catch (ParseException e) {
			fail(e.getMessage());
		}
	}
	
	private void checkPublic(DateRange d, StringPair sp, int idx) {
		assertEquals(d.getRangeStart(), expectedDates.get(idx).getRangeStart());
		assertEquals(d.getRangeEnd(), expectedDates.get(idx).getRangeEnd());
		assertEquals(d.getDisplayStartTimestampStr(), sp.getA());
		assertEquals(d.getDisplayEndTimestampStr(), sp.getB());
	}

	@Test
	public void testIntoNumberOfChunks() throws ParseException {
		
		expectedDates = new ArrayList<DateRange>();
		expectedDatesDisplay = new ArrayList<StringPair>();
		
		expectedDates.add(new DateRange(1420088400000L, 1420390800000L));
		expectedDates.add(new DateRange(1420390800001L, 1420693200001L));
		
		expectedDatesDisplay.add(new StringPair("2015-01-01T00:00:00Z", "2015-01-04T12:00:00Z"));
		expectedDatesDisplay.add(new StringPair("2015-01-04T12:00:00Z", "2015-01-08T00:00:00Z"));
		
		try {
			ArrayList<DateRange> dates = TimeChunk.intoNumberOfChunks(startDate, finalDate, 2);
			
			assertEquals(dates.size(), 2);
			
			Iterator<DateRange> i = dates.iterator();
			int idx = 0;
			while (i.hasNext()) {
				DateRange d = i.next();
				StringPair sp = expectedDatesDisplay.get(idx);
				checkPublic(d, sp, idx++);
			}
			
		} catch (ParseException e) {
			fail(e.getMessage());
		}
	}

}
