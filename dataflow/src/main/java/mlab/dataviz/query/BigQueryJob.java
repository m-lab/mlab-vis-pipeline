package mlab.dataviz.query;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.dofn.AddISPsFn;

public class BigQueryJob {
	
	private static final Logger LOG = LoggerFactory.getLogger(BigQueryJob.class);
	
	private Bigquery bigquery;
	private String projectId;
	
	public BigQueryJob (String projectId) throws IOException {
		this.projectId = projectId;
		this.bigquery = createAuthorizedClient();
	}
	
	/**
	 * Creates authorized query client.
	 * @return
	 * @throws IOException
	 */
	public Bigquery createAuthorizedClient() throws IOException {
		// Create the credential
		HttpTransport transport = new NetHttpTransport();
		JsonFactory jsonFactory = new JacksonFactory();
		GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

		// Depending on the environment that provides the default credentials
		// (e.g. Compute Engine, App
		// Engine), the credentials may require us to specify the scopes we need
		// explicitly.
		// Check for this case, and inject the Bigquery scope if required.
		if (credential.createScopedRequired()) {
			credential = credential.createScoped(BigqueryScopes.all());
		}

		return new Bigquery.Builder(transport, jsonFactory, credential)
				.setApplicationName("Bocoup M-Lab NDT Pipeline")
				.build();
	}

	/**
	 * Executes a given query synchronously. Returns the rows
	 * that resulted from that query.
	 *
	 * @param querySql  the query to execute.
	 * @param bigquery  the Bigquery service object.
	 * @param projectId  the id of the project under which to run the query.
	 * @return a list of the results of the query.
	 * @throws IOException  if there's an error communicating with the API.
	 */
	public java.util.List<TableRow> executeQuery(String querySql)
			throws IOException {
		
		QueryResponse query = bigquery.jobs()
				.query(projectId, 
						new QueryRequest().setQuery(querySql))
				.execute();
		
		GetQueryResultsResponse queryResult = bigquery.jobs()
				.getQueryResults(
						query.getJobReference()
							.getProjectId(), 
						query.getJobReference()
							.getJobId())
				.execute();

		return queryResult.getRows();
	}
	
	/**
	 * @private
	 * Sets up a query job and returns a reference.
	 * @param bigquery
	 * @param projectId
	 * @param querySql
	 * @return
	 * @throws IOException
	 */
	private static JobReference startQuery(Bigquery bigquery, String projectId, String querySql) throws IOException {
		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		config.setQuery(queryConfig);
		job.setConfiguration(config);
		queryConfig.setQuery(querySql);

		Insert insert = bigquery.jobs().insert(projectId, job);
		insert.setProjectId(projectId);
		JobReference jobId = insert.execute().getJobReference();
		return jobId;
	}

	/**
	 * Polls the status of a BigQuery job, returns TableReference to results if "DONE"
	 */
	private static TableReference checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
	    throws IOException, InterruptedException {
	  // Variables to keep track of total query time
	  long startTime = System.currentTimeMillis();
	  long elapsedTime;

	  while (true) {
	    Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
	    elapsedTime = System.currentTimeMillis() - startTime;
	    LOG.info("Job status " + elapsedTime +
	        jobId.getJobId() + " " + pollJob.getStatus().getState());
	    if (pollJob.getStatus().getState().equals("DONE")) {
	      return pollJob.getConfiguration().getQuery().getDestinationTable();
	    }
	    // Pause execution for one second before polling job status again, to
	    // reduce unnecessary calls to the BigQUery API and lower overall
	    // application bandwidth.
	    Thread.sleep(1000);
	  }
	}

	/**
	 * Executes a query that is likely to have more rows than can fit in a single result.
	 * @param querySql String the query to run
	 * @return
	 * @throws IOException
	 */
	public java.util.List<TableRow> executePaginatedQuery(String querySql) throws IOException {

		JobReference jobId = startQuery(bigquery, this.projectId, querySql);
		TableReference completedJob = null;
		try {
			completedJob = checkQueryResults(bigquery, this.projectId, jobId);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace().toString());
		}

		// QueryResponse query = bigquery.jobs().query(projectId, new QueryRequest().setQuery(querySql)).execute();
		List<TableRow> allRows = null;

		int page = 1;
		String pageToken = null;

		// Default to not looping
		boolean moreResults = false;

		do {
			LOG.info("Fetching page " + page);
			TableDataList queryResult = bigquery.tabledata().list(
		            completedJob.getProjectId(),
		            completedJob.getDatasetId(),
		            completedJob.getTableId())
		                .setPageToken(pageToken)
		         .execute();
			 
			if (page == 1) {
				allRows = queryResult.getRows();
			} else {
				allRows.addAll(queryResult.getRows());
			}
			pageToken = queryResult.getPageToken();
			if (pageToken != null) {
				moreResults = true;
				page++;
			} else {
				moreResults = false;
			}
		} while (moreResults);
		
		return allRows;
		
	}
}
