package mlab.dataviz.entities;

import java.sql.SQLException;

public interface BTPipelineRunDao {

    // add new entity for a run
    Long createBTPipelineRunEntity(BTPipelineRun bpr) throws SQLException;

    // fetch an entity
    BTPipelineRun getBTPipelineRunEntity(Long id) throws SQLException;

    // update an entity
    void updateBTPipelineRunEntity(BTPipelineRun bpr) throws SQLException;

    // mark pipeline entry done
    void markBTPipelineRunComplete(long id) throws SQLException;
}