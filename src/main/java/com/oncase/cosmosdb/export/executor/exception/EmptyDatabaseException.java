package com.oncase.cosmosdb.export.executor.exception;

import com.microsoft.azure.documentdb.DocumentClient;

public class EmptyDatabaseException extends Exception {
	
	private static final long serialVersionUID = -4666513635845520694L;
	private DocumentClient documentClient;
	private String db;
	
	public EmptyDatabaseException(DocumentClient documentClient, String db){
		this.documentClient = documentClient;
		this.db = db;
	}
	
	public DocumentClient getDocumentClient() {
		return documentClient;
	}

	public String getDb() {
		return db;
	}
	
}
