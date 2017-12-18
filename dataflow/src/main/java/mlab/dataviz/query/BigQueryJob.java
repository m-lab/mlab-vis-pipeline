package mlab.dataviz.query;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.BigQuery.QueryOption;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQueryException;

public class BigQueryJob {

	private static final Logger LOG = LoggerFactory.getLogger(BigQueryJob.class);
	private BigQuery bigquery;

	/**
	 * @constructor
	 * Creates a new bigquery querier. Can be used to run legacy and standard
	 * sql queries against bigquery.
	 * @throws IOException
	 */
	public BigQueryJob() throws IOException {
		this.bigquery = BigQueryOptions.getDefaultInstance().getService();
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
		    QueryJobConfiguration.of(querySql);

		// Request query to be executed and wait for results
		QueryResponse queryResponse = bigquery.query(
		    queryConfig,
		    QueryOption.of(QueryResultsOption.maxWaitTime(60000L)),
		    QueryOption.of(QueryResultsOption.pageSize(1000L)));

		// Read rows
		QueryResult result = queryResponse.getResult();
		return result.getValues();
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
			QueryResponse queryResponse = bigquery.query(
			    queryConfig,
			    QueryOption.of(QueryResultsOption.maxWaitTime(300000L)), //5 min
			    QueryOption.of(QueryResultsOption.pageSize(1000L)));

			// Read rows
			QueryResult result = queryResponse.getResult();
			String val = null;
			for (FieldValueList row : result.getValues()) {
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
