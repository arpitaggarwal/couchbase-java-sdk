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

	private static final String COUCHBASE_HOST = "127.0.0.1";
	private static final String COUCHBASE_BUCKET = "default";
	private static final String COUCHBASE_VIEWMODE = "production"; // "development";

	private static final String COUCHBASE_DESIGN_DOCUMENT_NAME = "dev_arpit";
	private static final String COUCHBASE_VIEW_NAME = "new";

	public static void main(String[] args) throws URISyntaxException,
			IOException {
		Cluster cluster = CouchbaseCluster.create();
		List<URI> list = new ArrayList<>();
		list.add(URI.create("http://" + COUCHBASE_HOST + ":8091/pools"));
		CouchbaseClient client = getAllKeys(list);
		DesignDocument designDoc = createDesignDocument();
		client.createDesignDoc(designDoc);
		Bucket defaultBucket = cluster.openBucket(COUCHBASE_BUCKET);
		createDocument(defaultBucket);
		cluster.disconnect();
	}

	private static CouchbaseClient getAllKeys(List<URI> list)
			throws IOException {
		System.setProperty("viewmode", COUCHBASE_VIEWMODE);
		CouchbaseClient client = new CouchbaseClient(list, COUCHBASE_BUCKET, "");
		View view = client.getView(COUCHBASE_DESIGN_DOCUMENT_NAME,
				COUCHBASE_VIEW_NAME);
		Query query = new Query();
		ViewResponse viewResponse = client.query(view, query);
		for (ViewRow viewRow : viewResponse) {
			System.out.println(viewRow.getKey());
		}
		return client;
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
