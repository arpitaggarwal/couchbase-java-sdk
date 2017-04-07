package com.arpit.couchbase.client.operations;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;

public class CouchbaseOperations {

	public static void main(String[] args) throws URISyntaxException,
			IOException {
		Cluster cluster = CouchbaseCluster.create();
		List<URI> list = new ArrayList<>();
		list.add(URI.create("http://127.0.0.1:8091/pools"));
		// System.setProperty("viewmode", "development");
		CouchbaseClient client = new CouchbaseClient(list, "default", "");
		View view = client.getView("dev_arpit", "new");
		Query query = new Query();
		ViewResponse viewResponse = client.query(view, query);
		for (ViewRow viewRow : viewResponse) {
			System.out.println(viewRow.getKey());
		}
		DesignDocument designDoc = createDesignDocument();
		client.createDesignDoc(designDoc);
		Bucket defaultBucket = cluster.openBucket("default");
		createDocument(defaultBucket);
		cluster.disconnect();
	}

	public static void createDocument(final Bucket bucket) {
		// prepare a json object to store in a JsonDocument with ID "walter"
		JsonObject user = JsonObject.empty().put("firstname", "Walter")
				.put("lastname", "White").put("job", "chemistry teacher")
				.put("age", 50);
		JsonDocument doc = JsonDocument.create("walter", user);

		// insert doc in bucket, updating it if it exists
		JsonDocument response = bucket.upsert(doc);
		// retrieve the document and show data
		JsonDocument walter = bucket.get("walter");
		System.out.println("Found: " + walter);
		System.out.println("Age: " + walter.content().getInt("age"));

		// get-and-update operation
		JsonDocument loaded = bucket.get("walter");
		if (loaded == null) {
			System.err.println("Document not found!");
		} else {
			loaded.content().put("age", 52);
			JsonDocument updated = bucket.replace(loaded);
			System.out.println("Updated: " + updated.id());
		}

		// cleanup (in a synchronous way) and disconnect
		System.out.println("Cleaning Up");
		// bucket.remove("walter");
		System.out.println("Exiting");
	}

	public static DesignDocument createDesignDocument() {
		DesignDocument designDoc = new DesignDocument("dev_beer");

		String viewName = "by_name";
		String mapFunction = "function (doc, meta) {\n"
				+ "  if(doc.type && doc.type == \"beer\") {\n"
				+ "    emit(doc.name);\n" + "  }\n" + "}";

		ViewDesign viewDesign = new ViewDesign(viewName, mapFunction);
		designDoc.getViews().add(viewDesign);
		return designDoc;
	}
}
