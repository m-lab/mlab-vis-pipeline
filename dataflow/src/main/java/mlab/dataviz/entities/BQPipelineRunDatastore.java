package mlab.dataviz.entities;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.auth.oauth2.GoogleCredentials;


public class BQPipelineRunDatastore implements BQPipelineRunDao {

	private static final String KIND = "viz_pipeline_bq";
	private Datastore datastore;
	private KeyFactory keyFactory;

	public BQPipelineRunDatastore() throws IOException, GeneralSecurityException {

//		HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
//		JsonFactory jsonFactory = new JacksonFactory();
//		GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
		GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
		
		DatastoreOptions options =
				DatastoreOptions.newBuilder()
				.setCredentials(credentials)
		        .setNamespace("mlab-vis-pipeline").build();

		datastore = options.getService();
		keyFactory = datastore.newKeyFactory().setKind(KIND);
	}

	public BQPipelineRun entityToVizPipelineRun(Entity entity) {
		return new BQPipelineRun.Builder()
				.data_end_date(entity.getString(BQPipelineRun.DATA_END_DATE))
				.data_start_date(entity.getString(BQPipelineRun.DATA_START_DATE))
				.run_end_date(entity.getString(BQPipelineRun.RUN_END_DATE))
				.run_start_date(entity.getString(BQPipelineRun.RUN_START_DATE))
				.status(entity.getString(BQPipelineRun.STATUS))
				.id(entity.getKey().getId())
				.build();
	}

	public List<BQPipelineRun> entitiesToVizPipelineRuns(QueryResults<Entity> resultList) {
		List<BQPipelineRun> resultVizPipelineRuns = new ArrayList<>();
		while (resultList.hasNext()) { // We still have data
			resultVizPipelineRuns.add(entityToVizPipelineRun(resultList.next())); // Add the Book to the List
		}
		return resultVizPipelineRuns;
	}

	@Override
	public Long createBQPipelineRunEntity(BQPipelineRun vpr) throws SQLException {
		IncompleteKey incompleteKey = keyFactory.newKey();
		Key key = datastore.allocateId(incompleteKey);

		FullEntity<Key> incVizPipelineRun = Entity.newBuilder(key)
				.set(BQPipelineRun.DATA_START_DATE, vpr.getDataStartDate())
				.set(BQPipelineRun.DATA_END_DATE, vpr.getDataEndDate())
				.set(BQPipelineRun.RUN_START_DATE, vpr.getRunStartDate())
				.set(BQPipelineRun.RUN_END_DATE, vpr.getRunEndDate())
				.set(BQPipelineRun.STATUS, BQPipelineRun.STATUS_RUNNING)
				.build();


		Entity vprEntity = datastore.add(incVizPipelineRun);
		return vprEntity.getKey().getId();
	}


	@Override
	public BQPipelineRun getBQPipelineRunEntity(Long id) throws SQLException {
		Entity vprEntity = datastore.get(keyFactory.newKey(id));
		return entityToVizPipelineRun(vprEntity);
	}

	@Override
	public void markBQPipelineRunComplete(long id) throws SQLException {
		Key key = keyFactory.newKey(id);
		BQPipelineRun vpr = getBQPipelineRunEntity(id);
		Entity vprEntity = Entity.newBuilder(key)
				.set(BQPipelineRun.DATA_START_DATE, vpr.getDataStartDate())
				.set(BQPipelineRun.DATA_END_DATE, vpr.getDataEndDate())
				.set(BQPipelineRun.RUN_START_DATE, vpr.getRunStartDate())
				.set(BQPipelineRun.RUN_END_DATE, vpr.getRunEndDate())
				.set(BQPipelineRun.STATUS, BQPipelineRun.STATUS_DONE)
				.build();
		datastore.update(vprEntity);
	}

	public void markBQPipelineRunFailed(long id) throws SQLException {
		Key key = keyFactory.newKey(id);
		BQPipelineRun vpr = getBQPipelineRunEntity(id);
		Entity vprEntity = Entity.newBuilder(key)
				.set(BQPipelineRun.DATA_START_DATE, vpr.getDataStartDate())
				.set(BQPipelineRun.DATA_END_DATE, vpr.getDataEndDate())
				.set(BQPipelineRun.RUN_START_DATE, vpr.getRunStartDate())
				.set(BQPipelineRun.RUN_END_DATE, vpr.getRunEndDate())
				.set(BQPipelineRun.STATUS, BQPipelineRun.STATUS_FAILED)
				.build();
		datastore.update(vprEntity);
	}

	@Override
	public void updateBQPipelineRunEntity(BQPipelineRun vpr) throws SQLException {
		Key key = keyFactory.newKey(vpr.getId());
		Entity vprEntity = Entity.newBuilder(key)
				.set(BQPipelineRun.DATA_START_DATE, vpr.getDataStartDate())
				.set(BQPipelineRun.DATA_END_DATE, vpr.getDataEndDate())
				.set(BQPipelineRun.RUN_START_DATE, vpr.getRunStartDate())
				.set(BQPipelineRun.RUN_END_DATE, vpr.getRunEndDate())
				.set(BQPipelineRun.STATUS, vpr.getRunEndDate())
				.build();
		datastore.update(vprEntity);
	}

	@Override
	public BQPipelineRun getLastBQPipelineRun() throws SQLException {
		Query<Entity> query = Query.newEntityQueryBuilder()
				.setKind(KIND)
				.setLimit(1)
				.setOrderBy(OrderBy.desc(BQPipelineRun.DATA_END_DATE))
				.build();

		  QueryResults<Entity> resultList = datastore.run(query);
		  List<BQPipelineRun> resultVizPipelineRuns = entitiesToVizPipelineRuns(resultList);
		  if (resultVizPipelineRuns.size() == 1) {
			  return resultVizPipelineRuns.get(0);
		  } else {
			  return null;
		  }

	}
}
