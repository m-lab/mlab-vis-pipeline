package mlab.dataviz.entities;
import java.sql.SQLException;

public interface BQPipelineRunDao {

	// add new entity for a run
	Long createBQPipelineRunEntity(BQPipelineRun vpr) throws SQLException;

	// fetch an entity
	BQPipelineRun getBQPipelineRunEntity(Long id) throws SQLException;

	// update an entity
	void updateBQPipelineRunEntity(BQPipelineRun vpr) throws SQLException;

	// get last entry
	BQPipelineRun getLastBQPipelineRun() throws SQLException;

	// mark pipeline done
	void markBQPipelineRunComplete(long id) throws SQLException;
}