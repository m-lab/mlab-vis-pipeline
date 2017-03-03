package mlab.dataviz.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import mlab.dataviz.util.TimeLocalizer;
import mlab.dataviz.util.TimeLocalizer.LocalTimeDetails;

public class TimeLocalizerTest {

	private static TimeLocalizer tlLocal;
	private static TimeLocalizer tlBq;
	
	static List<List<String>> sampleTimes;
	
	
	@BeforeClass
	public static void setupClass() {
		try {
			tlLocal = new TimeLocalizer().setMode(TimeLocalizer.LOCAL_MODE).setup();
			tlBq = new TimeLocalizer().setMode(TimeLocalizer.BQ_MODE).setup();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		sampleTimes = new ArrayList<List<String>>();
		
		List<String> t1 = Arrays.asList("San Francisco", "2015-05-29 10:00:00 UTC", "37.791500091552734",
				"-122.40889739990234", "2015-05-29 03:00:00", "PST");
		sampleTimes.add(t1);
		
		List<String> t2 = Arrays.asList("Chicago", "2014-12-01 12:00:00 UTC", "41.92060089111328", 
				"-87.70169830322266", "2014-12-01 06:00:00", "CDT");
		sampleTimes.add(t2);
		
		// no dst
		List<String> t3 = Arrays.asList("Paris", "2013-02-27 23:12:00 UTC", "48.88169860839844", 
				"2.382200002670288", "2013-02-28 00:12:00", "CEST");
		sampleTimes.add(t3);
		
		// dst
		List<String> t4 = Arrays.asList("Paris", "2013-04-27 23:12:00 UTC", "48.88169860839844", 
				"2.382200002670288", "2013-04-28 01:12:00", "CET");
		sampleTimes.add(t4);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetTimeZoneLocal() {
		Iterator<List<String>> i = sampleTimes.iterator();
		while (i.hasNext()) {
			List<String> row = i.next();
			try {
				
				LocalTimeDetails ltd = tlLocal.utcToLocalTime(
						row.get(1), Double.valueOf(row.get(2)), Double.valueOf(row.get(3)));
				assertEquals(ltd.getTimezone(), row.get(5));
				
			} catch (Exception e) {
				fail(e.getMessage());
			}
		}
	}

	@Test
	public void testUtcToLocalTimeLocal() {
		Iterator<List<String>> i = sampleTimes.iterator();
		while (i.hasNext()) {
			List<String> row = i.next();
			try {
				
				LocalTimeDetails ltd = tlLocal.utcToLocalTime(
						row.get(1), Double.valueOf(row.get(2)), Double.valueOf(row.get(3)));
				assertEquals(ltd.getTimestamp(), row.get(4));
				
			} catch (Exception e) {
				fail(e.getMessage());
			}
		}
	}
	
	@Test
	public void testGetTimeZoneBq() {
		Iterator<List<String>> i = sampleTimes.iterator();
		while (i.hasNext()) {
			List<String> row = i.next();
			try {
				
				LocalTimeDetails ltd = tlBq.utcToLocalTime(
						row.get(1), Double.valueOf(row.get(2)), Double.valueOf(row.get(3)));
				assertEquals(ltd.getTimezone(), row.get(5));
				
			} catch (Exception e) {
				fail(e.getMessage());
			}
		}
	}

	@Test
	public void testUtcToLocalTimeBq() {
		Iterator<List<String>> i = sampleTimes.iterator();
		while (i.hasNext()) {
			List<String> row = i.next();
			try {
				LocalTimeDetails ltd = tlBq.utcToLocalTime(
						row.get(1), Double.valueOf(row.get(2)), Double.valueOf(row.get(3)));
				assertEquals(ltd.getTimestamp(), row.get(4));
				
			} catch (Exception e) {
				fail(e.getMessage());
			}
		}
	}


}
