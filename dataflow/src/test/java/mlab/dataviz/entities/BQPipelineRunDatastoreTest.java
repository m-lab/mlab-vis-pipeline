package mlab.dataviz.entities;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BQPipelineRunDatastoreTest {

	private final LocalDatastoreHelper helper = LocalDatastoreHelper.create();
	private BQPipelineRunDatastore ds;
	private BQPipelineRun record1 = new BQPipelineRun.Builder().run_end_date("2017-11-22T18:59:22Z")
			.run_start_date("2017-11-12T18:59:22Z").data_end_date("2017-05-11 00:00:00.000000")
			.data_start_date("2017-05-01 00:00:00.000000").status("running").type("day").build();

	private BQPipelineRun record2 = new BQPipelineRun.Builder().run_end_date("2017-09-22T18:59:22Z") // two months
																										// earlier
			.run_start_date("2017-09-12T18:59:22Z").data_end_date("2017-05-11 00:00:00.000000")
			.data_start_date("2017-05-01 00:00:00.000000").status(BQPipelineRun.STATUS_DONE) // complete
			.type("day").build();

	@Before
	public void setUp() throws IOException, InterruptedException, GeneralSecurityException, SQLException {
		helper.start();
		Datastore localDatastore = helper.getOptions().getService();
		ds = new BQPipelineRunDatastore(localDatastore);
	}

	@After
	public void tearDown() throws IOException, InterruptedException, TimeoutException {
		// helper.stop(Duration.ofMinutes(1));
	}

	// Run this test twice to prove we're not leaking any state across tests.
	@Test
	public void testCreate() throws IOException, GeneralSecurityException, SQLException {
		BQPipelineRun status = new BQPipelineRun.Builder().data_end_date("data_date_end")
				.data_start_date("data_date_start").build();

		long id = ds.createBQPipelineRunEntity(status);
		assert (id != 0);
	}

	@Test
	public void testFind() throws SQLException {
		ds.createBQPipelineRunEntity(record1);
		ds.createBQPipelineRunEntity(record2);
		
		BQPipelineRun found;
		try {
			found = ds.getBQPipelineRunEntity(record1.getId());
			assertEquals(found.getDataEndDate(), record1.getDataEndDate());
			assertEquals(found.getDataStartDate(), record1.getDataStartDate());
			assertEquals(found.getRunEndDate(), record1.getRunEndDate());
			assertEquals(found.getRunStartDate(), record1.getRunStartDate());
			assertEquals(found.getType(), record1.getType());
		} catch (Exception e) {
			assertFalse(e.getMessage(), false);
		}
	}

	@Test
	public void testUpdate() throws SQLException {
		long id = ds.createBQPipelineRunEntity(record1);
		record1.setType("TestType");
		ds.updateBQPipelineRunEntity(record1);

		BQPipelineRun readRecord1 = ds.getBQPipelineRunEntity(record1.getId());
		assertEquals(readRecord1.getType(), "TestType");
	}

	// enable these eventually when we figure out how to get indices
	// in a test env. @todo.
//	@Test
//	public void testGetLastRun() throws SQLException {
//		ds.createBQPipelineRunEntity(record1);
//		ds.createBQPipelineRunEntity(record2);
//
//		BQPipelineRun last = ds.getLastBQPipelineRun("day");
//		assertEquals(last.getId(), record2.getId());
//		assertEquals(last.getDataEndDate(), record2.getDataEndDate());
//		assertNotEquals(last.getDataEndDate(), record1.getDataEndDate());
//	}
//
//	@Test
//	public void testGetMissingLastRun() throws SQLException {
//		ds.createBQPipelineRunEntity(record1);
//		ds.createBQPipelineRunEntity(record2);
//		
//		BQPipelineRun last = ds.getLastBQPipelineRun("not_a_type");
//		assertEquals(last, null);
//	}
}
