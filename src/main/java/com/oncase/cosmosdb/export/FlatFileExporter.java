package com.oncase.cosmosdb.export;

import java.util.List;

import com.beust.jcommander.JCommander;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.oncase.cosmosdb.export.executor.Parameters;

public class FlatFileExporter {

	private static Parameters params;
	private static DocumentClient documentClient;
	private static DocumentCollection collectionCache;
	private static Database databaseCache;

    public static void main( String[] args ){
    	params = new Parameters();
    	JCommander jc = JCommander.newBuilder()
            .addObject(params)
            .build();
        jc.parse(args);
        jc.setProgramName("exporter.sh");
        
        if(params.help){
        	jc.usage();
        } else {
        	run();
        }
    }

	private static void run() {
		System.out.println("host: " + params.host);
		System.out.println("key: " + params.key);
		documentClient = new DocumentClient(params.host,
                params.key, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

	}
	
	static List<Document> getAllDocumentsByDay(String date) {
		// Retrieve the document using the DocumentClient.   	
		String query = "SELECT * FROM root r WHERE r.date = '" +date+"'";

		List<Document> documentList = documentClient
				.queryDocuments(getTodoCollection().getSelfLink(),
						query, null).getQueryIterable().toList();


		return documentList;

	}
	
	public static Database getTodoDatabase() {
		if (databaseCache == null) {
			// Get the database if it exists
			List<Database> databaseList = documentClient
					.queryDatabases(
							"SELECT * FROM root r WHERE r.id='" + params.db
							+ "'", null).getQueryIterable().toList();

			if (databaseList.size() > 0) {
				// Cache the database object so we won't have to query for it
				// later to retrieve the selfLink.
				databaseCache = databaseList.get(0);
			} else {
				// Create the database if it doesn't exist.

			}
		}

		return databaseCache;
	}
	
	public static DocumentCollection getTodoCollection() {
		if (collectionCache == null) {
			// Get the collection if it exists.
			List<DocumentCollection> collectionList = documentClient
					.queryCollections(
							getTodoDatabase().getSelfLink(),
							"SELECT * FROM root r WHERE r.id='" + params.collection
							+ "'", null).getQueryIterable().toList();

			if (collectionList.size() > 0) {
				// Cache the collection object so we won't have to query for it
				// later to retrieve the selfLink.
				collectionCache = collectionList.get(0);
			} else {
				// Create the collection if it doesn't exist.
				try {
					DocumentCollection collectionDefinition = new DocumentCollection();
					collectionDefinition.setId(params.collection);

					collectionCache = documentClient.createCollection(
							getTodoDatabase().getSelfLink(),
							collectionDefinition, null).getResource();
				} catch (DocumentClientException e) {
					// TODO: Something has gone terribly wrong - the app wasn't
					// able to query or create the collection.
					// Verify your connection, endpoint, and key.
					e.printStackTrace();
				}
			}
		}

		return collectionCache;
	}
	
}
