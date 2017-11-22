package mlab.dataviz.main;

import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Entity;
import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

public class SimpleDatastoreTest {
	public static void main(String[] args) throws IOException {
		GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
		
		Vector<String> scopes = new Vector();
		scopes.add("https://www.googleapis.com/auth/cloud-platform");
		scopes.add("https://www.googleapis.com/auth/datastore");
		credentials.createScoped(scopes);
		
		System.out.println(credentials.getAuthenticationType());
		System.out.println(credentials.toString());
		
		DatastoreOptions options =
				DatastoreOptions.newBuilder()
				.setCredentials(credentials)
		        .setNamespace("mlab-vis-pipeline").build();
		
		Datastore datastore = options.getService();
		
		KeyFactory keyFactory = datastore.newKeyFactory().setKind("Task");
		Key taskKey = keyFactory.newKey("sample-key-id" + System.currentTimeMillis());
		
		Entity task = Entity.newBuilder(taskKey)
			    .set("category", "Personal")
			    .set("done", false)
			    .set("priority", 4)
			    .set("description", "Learn Cloud Datastore")
			    .build();
		
		datastore.put(task);
		System.out.println("Wrote task. Key: " + task.getKey().toString());
		
		Entity fetchedTask = datastore.get(task.getKey());
		System.out.println("Fetched by key: " + task.getString("description"));	
	}
}
