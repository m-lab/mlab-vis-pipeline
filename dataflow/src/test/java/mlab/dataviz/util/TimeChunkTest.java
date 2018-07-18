package mlab.dataviz.util;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import mlab.dataviz.util.TimeChunk;

public class TimeChunkTest {

	private String startDate = "2015-01-01T00:00:00Z";
	private String finalDate = "2015-01-08T00:00:00Z";
	private int numOfDays = 2;
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
	
	private void checkPublic(DateRange d, StringPair sp, int idx) {
		assertEquals(d.getDisplayStartTimestampStr(), sp.getA());
		assertEquals(d.getDisplayEndTimestampStr(), sp.getB());
	}
	
	@Test
	public void testByNumberOfDays() throws ParseException {
		
		expectedDatesDisplay = new ArrayList<StringPair>();
		
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

	@Test
	public void testIntoNumberOfChunks() throws ParseException {
		
		expectedDatesDisplay = new ArrayList<StringPair>();
		
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
