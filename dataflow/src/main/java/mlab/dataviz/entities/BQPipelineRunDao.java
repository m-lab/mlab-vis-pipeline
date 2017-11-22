package mlab.dataviz.entities;
import java.sql.SQLException;

public interface BQPipelineRunDao {

	// add new entity for a run
	Long createBQPipelineRunEntity(BQPipelineRun vpr) throws SQLException;

	// fetch an entity
	BQPipelineRun getBQPipelineRunEntity(Long id) throws SQLException;

	// update an entity
	void updateBQPipelineRunEntity(BQPipelineRun vpr) throws SQLException;

	BQPipelineRun getLastBQPipelineRun(String type) throws SQLException;
}