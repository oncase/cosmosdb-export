package com.oncase.cosmosdb.export.executor.exception;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;

public class EmptyCollectionException extends Exception {

	private static final long serialVersionUID = -6927224787522837118L;
	private DocumentClient documentClient;
	private Database db;
	private String coll;
	
	public EmptyCollectionException(DocumentClient documentClient, 
			Database db, String coll){
		
		this.documentClient = documentClient;
		this.db = db;
		this.coll = coll;
		
	}
	
	public DocumentClient getDocumentClient() {
		return documentClient;
	}

	public Database getDb() {
		return db;
	}

	public String getColl() {
		return coll;
	}
	
	
}
