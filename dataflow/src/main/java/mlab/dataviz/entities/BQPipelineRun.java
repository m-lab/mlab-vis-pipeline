package mlab.dataviz.entities;

/**
 * Represents a run of the bigquery portion of the 
 * viz pipeline. It is stored in Datastore, to track the 
 * dates we use to query for new data as well as the
 * dates of the run and its status.
 *
 * @author iros
 */
public class BQPipelineRun {

	// properties
	private long id;
	private String data_start_date = null;
	private String data_end_date = null;
	private String status = null;
	private String run_start_date = null;
	private String run_end_date = null;

	// property names
	public static final String ID = "id";
	public static final String DATA_START_DATE = "datastartdate";
	public static final String DATA_END_DATE = "dataenddate";
	public static final String RUN_START_DATE = "runstartdate";
	public static final String RUN_END_DATE = "runenddate";
	public static final String STATUS = "status";

	// statuses
	public static final String STATUS_RUNNING = "running";
	public static final String STATUS_DONE = "done";
	public static final String STATUS_FAILED = "failed";

	// constructor
	private BQPipelineRun(Builder builder) {
		this.id = builder.id;
		this.data_start_date = builder.data_start_date;
		this.data_end_date = builder.data_end_date;
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


	public String getDataStartDate() {
		return data_start_date;
	}


	public void setDataStartDate(String start_date) {
		this.data_start_date = start_date;
	}


	public String getDataEndDate() {
		return data_end_date;
	}


	public void setDataEndDate(String end_date) {
		this.data_end_date = end_date;
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

	public String[] getDates() {
		return new String[] {
			this.data_start_date, this.data_end_date
		};
	}
	
	public String toString() {
	    return
	        "ID: " + this.id +
	        " Data Start date: " + this.data_start_date +
	        " Data End date: " + this.data_end_date +
	        " Run Start date: " + this.run_start_date +
	        " Run End date: " + this.run_end_date +
	        " Status: " + this.status;
	  }

	public static class Builder {
		private long id;
		private String run_start_date = "";
		private String run_end_date = "";
		private String data_start_date = "";
		private String data_end_date = "";
		private String status = "";

		public Builder id(long id) {
			this.id = id;
			return this;
		}

		public long getId() {
			return this.id;
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

		public Builder data_start_date(String start_date) {
			this.data_start_date = start_date;
			return this;
		}

		public String getDataStartDate() {
			return this.data_start_date;
		}

		public Builder data_end_date(String end_date) {
			this.data_end_date = end_date;
			return this;
		}

		public String getDataEndDate() {
			return this.data_end_date;
		}

		public Builder status(String status) {
			this.status = status;
			return this;
		}

		public String getStatus() {
			return this.status;
		}

		public BQPipelineRun build() {
			return new BQPipelineRun(this);
		}

		 @Override
		  public String toString() {
		    return
		    		 "ID: " + this.id +
		 	      " Data Start date: " + this.data_start_date +
		 	      " Data End date: " + this.data_end_date +
		 	      " Run Start date: " + this.run_start_date +
		 	      " Run End date: " + this.run_end_date +
		 	      " Status: " + this.status;
		  }
	}
}
