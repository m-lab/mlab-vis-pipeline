package mlab.dataviz.entities;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;

public class BTPipelineRunDatastore implements BTPipelineRunDao {

    private static final String KIND = "viz_pipeline_bt";

    private Datastore datastore;
    private KeyFactory keyFactory;

    /**
     * Creates a new bigtable pipeline run object
     * @throws IOException
     */
    public BTPipelineRunDatastore() throws IOException {
		GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

		DatastoreOptions options =
				DatastoreOptions.newBuilder()
				.setCredentials(credentials)
		        .setNamespace("mlab-vis-pipeline").build();

        datastore = options.getService();
        keyFactory = datastore.newKeyFactory().setKind(KIND);
    }

    /**
     * Converts an entity to a bigtable pipeline run object.
     * @param entity DB entity
     * @return BTPipelineRun object
     */
    private BTPipelineRun entityToBTPipelineRun(Entity entity) {
        return new BTPipelineRun.Builder()
                .run_end_date(entity.getString(BTPipelineRun.RUN_END_DATE))
                .run_start_date(entity.getString(BTPipelineRun.RUN_START_DATE))
                .status(entity.getString(BTPipelineRun.STATUS))
                .id(entity.getKey().getId())
                .build();
    }

    private List<BTPipelineRun> entitiesToBTPipelineRuns(QueryResults<Entity> resultList) {
        List<BTPipelineRun> resultBTPipelineRuns = new ArrayList<>();
        while (resultList.hasNext()) {
                resultBTPipelineRuns.add(entityToBTPipelineRun(resultList.next()));
        }
        return resultBTPipelineRuns;
    }

    /**
     * Create a new database entry from a BTPipelineRun object
     * @param bpr A BTPipelineRun object
     * @return id for created object.
     */
	@Override
	public Long createBTPipelineRunEntity(BTPipelineRun bpr) throws SQLException {
		IncompleteKey incompleteKey = keyFactory.newKey();
        Key key = datastore.allocateId(incompleteKey);

        FullEntity<Key> btPipelineRunShellEntity = Entity.newBuilder(key)
                .set(BTPipelineRun.RUN_START_DATE, bpr.getRunStartDate())
                .set(BTPipelineRun.RUN_END_DATE, bpr.getRunEndDate())
                .set(BTPipelineRun.STATUS, BTPipelineRun.STATUS_RUNNING).build();

        Entity btPipelineRunEntity = datastore.add(btPipelineRunShellEntity);
        return btPipelineRunEntity.getKey().getId();
	}


	/**
	 * Fetches a bigtable pipeline run entity from datastore
	 * @param id
	 * @return bpr BTPipelineRun object
	 */
	@Override
	public BTPipelineRun getBTPipelineRunEntity(Long id) throws SQLException {
		Entity btPipelineRunEntity = datastore.get(keyFactory.newKey(id));
        return entityToBTPipelineRun(btPipelineRunEntity);
	}

	/**
	 * Update a run in datastore
	 * @param btPipelineRun BTPipelineRun object
	 */
	@Override
	public void updateBTPipelineRunEntity(BTPipelineRun btPipelineRun) throws SQLException {
        Key key = keyFactory.newKey(btPipelineRun.getId());
        Entity btPipelineEntity = Entity.newBuilder(key)
                .set(BTPipelineRun.RUN_START_DATE, btPipelineRun.getRunStartDate())
                .set(BTPipelineRun.RUN_END_DATE, btPipelineRun.getRunEndDate())
                .set(BTPipelineRun.STATUS, btPipelineRun.getStatus()).build();
        datastore.update(btPipelineEntity);
	}

	/**
	 * Mark a run complete
	 * @param id of a run
	 */
	@Override
	public void markBTPipelineRunComplete(long id) throws SQLException {
		Key key = keyFactory.newKey(id);
        BTPipelineRun vpr = getBTPipelineRunEntity(id);
        Entity btPipelineRunEntity = Entity.newBuilder(key)
                .set(BTPipelineRun.RUN_START_DATE, vpr.getRunStartDate())
                .set(BTPipelineRun.RUN_END_DATE, vpr.getRunEndDate())
                .set(BTPipelineRun.STATUS, BTPipelineRun.STATUS_DONE).build();
        datastore.update(btPipelineRunEntity);
        }

        /**
         * Returns the last bigtable pipeline run that is still running.
         * This way, we won't start another one if it is still running.
         */
        @Override
        public BTPipelineRun getLastBTPipelineRun() throws SQLException {
                Query<Entity> query = Query.newEntityQueryBuilder()
                        .setKind(KIND)
                        .setFilter(PropertyFilter.eq(BTPipelineRun.STATUS, BTPipelineRun.STATUS_RUNNING))
                        .setLimit(1)
                        .setOrderBy(OrderBy.desc(BTPipelineRun.RUN_END_DATE))
                        .build();

                QueryResults<Entity> resultList = datastore.run(query);
                List<BTPipelineRun> resultBTPipelineRuns = entitiesToBTPipelineRuns(resultList);
                if (resultBTPipelineRuns.size() == 1) {
                		BTPipelineRun run = resultBTPipelineRuns.get(0);
                		if (run.getRunEndDate().length() > 0) {
                			return run;
                		} else {
                			return null;
                		}
                } else {
                        return null;
                }
        }
}