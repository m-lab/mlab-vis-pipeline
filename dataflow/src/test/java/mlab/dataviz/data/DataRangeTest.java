package mlab.dataviz.data;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.time.Instant;

import org.junit.Before;
import org.junit.Test;

import mlab.dataviz.data.DateRange;

public class DataRangeTest {
	
	private DateRange dr;
	long startdate;
	long enddate;
	
	@Before
	public void setUp() throws Exception {
		
		// "2014-12-31T19:00:00Z"
		startdate = 1420070400000L;
		
		// "2015-01-31T19:00:00Z"
		enddate = 1422748800000L;
		
		dr = new DateRange(startdate, enddate);
	}

	@Test
	public void testDateRange() {
		try {
			dr = new DateRange(startdate, enddate);
			assertNotNull(dr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetRangeStart() {
		assertEquals(dr.getRangeStart(), startdate);
	}

	@Test
	public void testGetRangeEnd() {
		assertEquals(dr.getRangeEnd(), enddate);
	}

	@Test
	public void testGetStartRangeLongStr() {
		assertEquals(dr.getStartRangeLongStr(), String.valueOf((startdate/1000) - Instant.EPOCH.getEpochSecond()));
	}

	@Test
	public void testGetEndRangeLongStr() {
		assertEquals(dr.getEndRangeLongStr(), String.valueOf((enddate/1000) - Instant.EPOCH.getEpochSecond()));
	}

	@Test
	public void testGetDisplayStartTimestampStr() {
		assertEquals(dr.getDisplayStartTimestampStr(), "2014-12-31T19:00:00Z");
	}

	@Test
	public void testGetDisplayEndTimestampStr() {
		assertEquals(dr.getDisplayEndTimestampStr(), "2015-01-31T19:00:00Z");
	}
	
	@Test
	public void testEquals() throws ParseException {
		DateRange good = new DateRange(startdate, enddate);
		DateRange bad = new DateRange(startdate+10, enddate);
		
		assertTrue(dr.equals(good));
		assertFalse(dr.equals(bad));
		
	}
}
