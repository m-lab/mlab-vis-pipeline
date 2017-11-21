package mlab.dataviz.query;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.TableRow;

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
	
	public static void main(String[] args) throws IOException, InterruptedException {
		BigQueryJob b = new BigQueryJob("mlab-sandbox");
		String q = "SELECT STRING(USEC_TO_TIMESTAMP(UTC_USEC_TO_DAY(max(web100_log_entry.log_time * INTEGER(POW(10, 6)))))) as max_test_date FROM [plx.google:m_lab.ndt.all]";
		List<TableRow> results = b.executeQuery(q);
		System.out.println(results);
	}
}
