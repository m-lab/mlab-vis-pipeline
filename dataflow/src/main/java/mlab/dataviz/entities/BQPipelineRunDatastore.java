package mlab.dataviz.entities;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.auth.oauth2.GoogleCredentials;

public class BQPipelineRunDatastore implements BQPipelineRunDao {

	private static final String KIND = "viz_pipeline_bq";
	public Datastore datastore;
	private KeyFactory keyFactory;

	public BQPipelineRunDatastore() throws IOException, GeneralSecurityException {
		GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

		DatastoreOptions options = DatastoreOptions.newBuilder().setCredentials(credentials)
				.setNamespace("mlab-vis-pipeline").build();

		datastore = options.getService();
		keyFactory = datastore.newKeyFactory().setKind(KIND);
	}
	
	public BQPipelineRunDatastore(Datastore d) {
		this.datastore = d;
		this.keyFactory = this.datastore.newKeyFactory().setKind(KIND);
	}

	private BQPipelineRun entityToVizPipelineRun(Entity entity) {
		return new BQPipelineRun.Builder().data_end_date(entity.getString(BQPipelineRun.DATA_END_DATE))
				.data_start_date(entity.getString(BQPipelineRun.DATA_START_DATE))
				.run_end_date(entity.getString(BQPipelineRun.RUN_END_DATE))
				.run_start_date(entity.getString(BQPipelineRun.RUN_START_DATE))
				.status(entity.getString(BQPipelineRun.STATUS)).type(entity.getString(BQPipelineRun.TYPE))
				.id(entity.getKey().getId()).build();
	}

	private List<BQPipelineRun> entitiesToVizPipelineRuns(QueryResults<Entity> resultList) {
		List<BQPipelineRun> resultVizPipelineRuns = new ArrayList<>();
		while (resultList.hasNext()) {
			resultVizPipelineRuns.add(entityToVizPipelineRun(resultList.next()));
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
				.set(BQPipelineRun.STATUS, BQPipelineRun.STATUS_RUNNING).set(BQPipelineRun.TYPE, vpr.getType()).build();

		Entity vprEntity = datastore.add(incVizPipelineRun);
		vpr.setId(vprEntity.getKey().getId());
		return vprEntity.getKey().getId();
	}

	@Override
	public BQPipelineRun getBQPipelineRunEntity(Long id) throws SQLException {
		Entity vprEntity = datastore.get(keyFactory.newKey(id));
		return entityToVizPipelineRun(vprEntity);
	}

	@Override
	public void updateBQPipelineRunEntity(BQPipelineRun vpr) throws SQLException {
		Key key = keyFactory.newKey(vpr.getId());
		Entity vprEntity = Entity.newBuilder(key).set(BQPipelineRun.DATA_START_DATE, vpr.getDataStartDate())
				.set(BQPipelineRun.DATA_END_DATE, vpr.getDataEndDate())
				.set(BQPipelineRun.RUN_START_DATE, vpr.getRunStartDate())
				.set(BQPipelineRun.RUN_END_DATE, vpr.getRunEndDate()).set(BQPipelineRun.STATUS, vpr.getStatus())
				.set(BQPipelineRun.TYPE, vpr.getType()).build();
		datastore.update(vprEntity);
	}

	@Override
	public BQPipelineRun getLastBQPipelineRun(String type) throws SQLException {
		Query<Entity> query = Query.newEntityQueryBuilder().setKind(KIND)
				.setFilter(CompositeFilter.and(PropertyFilter.eq(BQPipelineRun.STATUS, BQPipelineRun.STATUS_DONE),
						PropertyFilter.eq(BQPipelineRun.TYPE, type)))
				.setLimit(1).setOrderBy(OrderBy.desc(BQPipelineRun.DATA_END_DATE)).build();
		query.toString();
		try {
			QueryResults<Entity> resultList = datastore.run(query);
			List<BQPipelineRun> resultVizPipelineRuns = entitiesToVizPipelineRuns(resultList);
			if (resultVizPipelineRuns.size() == 1) {
				return resultVizPipelineRuns.get(0);
			} else {
				return null;
			}
		} catch (DatastoreException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			return null;
		}

	}
}
