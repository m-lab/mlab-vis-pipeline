package mlab.dataviz.entities;

import java.sql.SQLException;

/**
 * Represents a bigtable pipeline run.
 * Stores the run dates as well as status.
 *
 * @author iros
 *
 */
public class BTPipelineRun {

    BTPipelineRunDatastore datastore;

    // properties
    private long id;
    private String run_start_date;
    private String run_end_date;
    private String status;

    // property names
    public static final String ID = "id";
    public static final String RUN_START_DATE = "runstartdate";
    public static final String RUN_END_DATE = "runenddate";
    public static final String STATUS = "status";

    // statuses
    public static final String STATUS_RUNNING = "running";
    public static final String STATUS_DONE = "done";
    public static final String STATUS_FAILED = "failed";

    // constructor
    private BTPipelineRun(Builder builder, BTPipelineRunDatastore d) {
        this.datastore = d;
        this.id = builder.id;
		this.run_end_date = builder.run_end_date;
		this.run_start_date = builder.run_start_date;
		this.status = builder.status;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getRunStartDate() {
        return this.run_start_date;
    }

    public void setRunStartDate(String date) {
        this.run_start_date = date;
    }

    public String getRunEndDate() {
        return this.run_end_date;
    }

    public void setRunEndDate(String date) {
        this.run_end_date = date;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    
    public boolean isDone() {
    		return this.status == STATUS_DONE && this.run_end_date.length() > 0;
    }
    public String toString() {
        return "ID: " + this.id + " Run Start date: " + this.run_start_date + " Run End date: " + this.run_end_date
                + " Status: " + this.status;
    }

    public void save() throws SQLException {
        this.datastore.updateBTPipelineRunEntity(this);
    }

    public static class Builder {
        private BTPipelineRunDatastore datastore;
		private long id;
		private String run_start_date = "";
		private String run_end_date = "";
        private String status = "";

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public long getId() {
            return this.id;
        }

        public Builder datastore(BTPipelineRunDatastore d) {
            this.datastore = d;
            return this;
        }

        public BTPipelineRunDatastore getDatastore() {
            return this.datastore;
        }

        public Builder run_start_date(String run_date) {
            this.run_start_date = run_date;
            return this;
        }

        public Builder run_end_date(String run_date) {
            this.run_end_date = run_date;
            return this;
        }

        public String getRunStartDate() {
            return this.run_start_date;
        }

        public String getRunEndDate() {
            return this.run_end_date;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public String getStatus() {
            return this.status;
        }

        public BTPipelineRun build() {
            return new BTPipelineRun(this, this.datastore);
        }

        @Override
		public String toString() {
		    return "ID: " + this.id +
		 	      " Run Start date: " + this.run_start_date +
		 	      " Run End date: " + this.run_end_date +
		 	      " Status: " + this.status;
	    }
    }
}