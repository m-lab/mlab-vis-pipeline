package mlab.dataviz.data;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.time.Instant;
import java.util.TimeZone;
import org.junit.Before;
import org.junit.Test;

import mlab.dataviz.data.DateRange;

public class DataRangeTest {

	private DateRange dr;
	long startdate;
	long enddate;

	@Before
	public void setUp() throws Exception {
		TimeZone tz = TimeZone.getDefault();
		
		// "2015-01-01T00:00:00Z"
		startdate = 1420070400000L - tz.getRawOffset();

		// "2015-02-01T00:00:00Z"
		enddate = 1422748800000L - tz.getRawOffset();

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
	public void testGetStartRangeLongStr() {
		assertEquals(dr.getStartRangeLongStr(), String.valueOf((startdate/1000) - Instant.EPOCH.getEpochSecond()));
	}

	@Test
	public void testGetEndRangeLongStr() {
		assertEquals(dr.getEndRangeLongStr(), String.valueOf((enddate/1000) - Instant.EPOCH.getEpochSecond()));
	}

	@Test
	public void testGetDisplayStartTimestampStr() {
		assertEquals(dr.getDisplayStartTimestampStr(), "2015-01-01T00:00:00Z");
	}

	@Test
	public void testGetDisplayEndTimestampStr() {
		assertEquals(dr.getDisplayEndTimestampStr(), "2015-02-01T00:00:00Z");
	}

	@Test
	public void testEquals() throws ParseException {
		DateRange good = new DateRange(startdate, enddate);
		DateRange bad = new DateRange(startdate+10, enddate);

		assertTrue(dr.equals(good));
		assertFalse(dr.equals(bad));

	}
}
