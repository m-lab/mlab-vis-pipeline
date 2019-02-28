package mlab.dataviz.query;

import static com.google.cloud.bigquery.BigQuery.QueryResultsOption.maxWaitTime;
import static com.google.cloud.bigquery.BigQuery.QueryResultsOption.pageSize;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.HttpTransportOptions;

public class BigQueryJob {

	private static final Logger LOG = LoggerFactory.getLogger(BigQueryJob.class);
	private BigQuery bigquery;
	
	/**
	 * Building retry settings to prevent an HTTP timeout for bigquery
	 * jobs. Extending the timeout on the bigquery rpc isn't enough 
	 * since it goes over HTTP and that connection can timeout too.
	 * @return
	 */
	private static RetrySettings retrySettings() {
	    return RetrySettings.newBuilder().setMaxAttempts(10)
	        .setMaxRetryDelay(Duration.ofMillis(30000L))
	        .setTotalTimeout(Duration.ofMillis(120000L))
	        .setInitialRetryDelay(Duration.ofMillis(250L))
	        .setRetryDelayMultiplier(1.0)
	        .setInitialRpcTimeout(Duration.ofMillis(120000L))
	        .setRpcTimeoutMultiplier(1.0)
	        .setMaxRpcTimeout(Duration.ofMillis(120000L))
	        .build();
	  }
	
	/**
	 * @constructor
	 * Creates a new bigquery querier. Can be used to run legacy and standard
	 * sql queries against bigquery.
	 * @throws IOException
	 */
	public BigQueryJob() throws IOException {
	    
		HttpTransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions();
		transportOptions = transportOptions.toBuilder()
				.setConnectTimeout(120000)
				.setReadTimeout(120000)
		        .build();
		
		BigQueryOptions bigqueryOptions = BigQueryOptions.newBuilder()
				.setRetrySettings(retrySettings())
				.setTransportOptions(transportOptions)
				.build();
		
		this.bigquery = bigqueryOptions.getService();
		
		
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
	 * @throws InterruptedException
	 */
	public Iterable<FieldValueList> executeQuery(String querySql)
			throws IOException, InterruptedException {

		// Create a query request
		QueryJobConfiguration queryConfig =
		    QueryJobConfiguration.newBuilder(querySql).build();

		// Request query to be executed and wait for results
		Job job = bigquery.create(JobInfo.of(queryConfig));
		TableResult results = job.getQueryResults(maxWaitTime(60000L), pageSize(1000L));
		return results.getValues();
	}

	/**
	 * Executes a query when the known response is a single string value.
	 * By default uses legacy SQL.
	 * @param querySql
	 * @return response String value
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String executeQueryForValue(String querySql) throws IOException, InterruptedException, GoogleJsonResponseException, BigQueryException {
		return executeQueryForValue(querySql, true);
	}

	/**
	 * Executes a query when the known response is a single string value.
	 * @param querySql
	 * @param legacySQL let's you override legacy or standard.
	 * @return response String value
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String executeQueryForValue(String querySql, boolean legacySql) throws IOException, InterruptedException, GoogleJsonResponseException, BigQueryException {

		try {
			// Create a query request
			QueryJobConfiguration queryConfig =
					QueryJobConfiguration.newBuilder(querySql)
						.setUseLegacySql(legacySql)
						.build();

			// Request query to be executed and wait for results
			Job job = bigquery.create(JobInfo.of(queryConfig));
			TableResult results = job.getQueryResults(maxWaitTime(300000L), pageSize(1000L));

			// Read rows
			String val = null;
			for (FieldValueList row : results.getValues()) {
				val = row.get(0).getStringValue();
			}

			return val;
		} catch (Exception e) {
			LOG.error(e.getMessage());
			System.out.println("Error in query: " + querySql);
			throw e;
		}
	}
}
