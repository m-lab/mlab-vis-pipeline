package mlab.dataviz.pipelines;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import mlab.dataviz.ExtractHistoricRowsPipeline;
import mlab.dataviz.util.Formatters;

public class ExtractHistoricRowsPipelineTest {

	private JSONObject config;
	private String projectId = "mlab-oti";
	private ExtractHistoricRowsPipeline ehrp;
	private static SimpleDateFormat dateFormatter = new SimpleDateFormat(Formatters.TIMESTAMP2);

	@Before
	public void setUp() throws Exception {
		config = new JSONObject();
		config.put("lastDateFromTable", "[mlab-oti:ndtforbocoup.ndt]");
		config.put("projectId", "mlab-oti");
		ehrp = new ExtractHistoricRowsPipeline(null);
	}

	@After
	public void tearDown() throws Exception {
	}

	// this test is currently disabled since we are not using this feature currently
	// and the table it relies on is not permanent.
//	@Test
//	public void testGetDateRange() {
//		try {
//			String [] timestamps = ehrp.getDatesAuto(config);
//			String startTime = timestamps[0];
//			String endTime = timestamps[1];
//
//			// verify we can parse the dates
//			dateFormatter.parse(startTime);
//			dateFormatter.parse(endTime);
//			System.out.println(timestamps[0] + " - " + timestamps[1]);
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (ParseException e) {
//			System.out.println("Can't parse date: " + e.getMessage());
//			assertFalse(false);
//		}
//	}

}
