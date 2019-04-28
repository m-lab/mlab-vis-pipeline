package mlab.dataviz.entities;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import mlab.dataviz.util.Formatters;

public class DatastoreTest {
	
	private static SimpleDateFormat dtf = new SimpleDateFormat(Formatters.TIMESTAMP);
	
	private static BTPipelineRunDatastore dbt;
	
	public static void main(String[] args) throws IOException, GeneralSecurityException {
		
		dbt = new BTPipelineRunDatastore();
		
		try {
			// bt tests
			long id = createBT();
			updateBT(id);
			BTPipelineRun bt = getBT(id);
			System.out.println(bt.toString());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static long createBT() throws SQLException {
		Calendar c = Calendar.getInstance();
		
		c.set(2017, 01, 15);
		Date start = c.getTime();
		c.set(2017, 01, 20);
		Date end = c.getTime();
		
		BTPipelineRun bpr = new BTPipelineRun.Builder()
				.run_start_date(dtf.format(start))
				.run_end_date(dtf.format(end))
				.status(BTPipelineRun.STATUS_RUNNING)
				.build();
		return dbt.createBTPipelineRunEntity(bpr);
	}
	
	public static void updateBT(long id) throws SQLException {
		dbt.markBTPipelineRunComplete(id);
	}
	
	public static BTPipelineRun getBT(long id) throws SQLException {
		return dbt.getBTPipelineRunEntity(id);
	}

}
