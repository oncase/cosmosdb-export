package com.oncase.cosmosdb.export.executor;

import java.util.List;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.oncase.cosmosdb.export.executor.exception.EmptyDatabaseException;

public class DatabaseHandler {
	
	private Database database;
	private DocumentClient documentClient;
	
	public DatabaseHandler(DocumentClient documentClient, String db) 
			throws EmptyDatabaseException{
		
		this.documentClient = documentClient;
		List<Database> databaseList = documentClient
				.queryDatabases(
						"SELECT * FROM root r WHERE r.id='" + db
						+ "'", null).getQueryIterable().toList();

		if (databaseList.size() > 0) {
			database = databaseList.get(0);
		} else {
			throw new EmptyDatabaseException(documentClient, db);
		}
		
	}
	
	public Database getDatabase(){
		return database;
	}
	
	public DocumentClient getDocumentClient(){
		return documentClient;
	}
	
}
