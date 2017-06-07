package com.oncase.cosmosdb.export.executor;

import java.util.List;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.oncase.cosmosdb.export.executor.exception.EmptyCollectionException;

public class CollectionHandler {
	
	private DocumentCollection collection;
	private DocumentClient documentClient;
	private Database db;
	private FeedOptions options = null;
	
	/**
	 * Creates a Collection Handler that can query and operate on the 
	 * collection level 
	 * 
	 * @param DocumentClient documentClient
	 * @param Database db
	 * @param String coll
	 * @throws EmptyCollectionException
	 */
	public CollectionHandler(DatabaseHandler db, String coll) 
			throws EmptyCollectionException {

		this.documentClient = db.getDocumentClient();
		this.db = db.getDatabase();
		
		List<DocumentCollection> collectionList = this.documentClient
				.queryCollections( this.db.getSelfLink(),
						"SELECT * FROM root r WHERE r.id='" + coll + "'", 
						null).getQueryIterable().toList();

		if (collectionList.size() > 0) {
			collection = collectionList.get(0);
		}else{
			throw new EmptyCollectionException(this.documentClient, 
					this.db, coll);
		}
	}
	
	/**
	 * Creates a Collection Handler that can query and operate on the 
	 * collection level 
	 * 
	 * @param DocumentClient documentClient
	 * @param Database db
	 * @param String coll
	 * @param String partKey
	 * @throws EmptyCollectionException
	 */
	public CollectionHandler(DatabaseHandler db, String coll, boolean enablePartitionQuery) 
			throws EmptyCollectionException {

		this.documentClient = db.getDocumentClient();
		this.db = db.getDatabase();
		
		this.options = new FeedOptions();
		this.options.setEnableCrossPartitionQuery(enablePartitionQuery);
		
		List<DocumentCollection> collectionList = this.documentClient
				.queryCollections( this.db.getSelfLink(),
						"SELECT * FROM root r WHERE r.id='" + coll + "'", 
						this.options).getQueryIterable().toList();

		if (collectionList.size() > 0) {
			collection = collectionList.get(0);
		}else{
			throw new EmptyCollectionException(this.documentClient, 
					this.db, coll);
		}
	}
	
	/**
	 * 
	 * @return DocumentCollection collection
	 */
	public DocumentCollection getCollection(){
		return collection;
	}
	
	/**
	 * Specialist implementation to retrieve documents from the collection
	 * by the field named `date` WHERE date = 'date'
	 * @param String date
	 * @return List<Document> documentList
	 */
	public List<Document> getAllDocumentsByDay(String date) {

		String query = "SELECT * FROM root r WHERE r.date = '" +date+"'";

		List<Document> documentList = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable().toList();

		return documentList;

	}

	/**
	 * Queries the collection with SELECT * FROM root r WHERE {wherePart}
	 * and retrieves a List of documents
	 * @param String wherePart
	 * @return List<Document> documentList
	 */
	public List<Document> getDocsWhere(String wherePart) {

		String query = "SELECT * FROM root r WHERE " +wherePart+"  ";
		
		List<Document> documentList = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable().toList();

		return documentList;

	}
	
	/**
	 * Queries the collection with SELECT {fields} FROM root r WHERE {wherePart}
	 * and retrieves a List of documents
	 * @param String wherePart
	 * @return List<Document> documentList
	 */
	public List<Document> getDocsFieldsWhere(String fields, String wherePart) {

		String query = "SELECT "+ fields +" FROM root r WHERE " +wherePart+"  ";

		List<Document> documentList = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable().toList();

		return documentList;

	}
	
	
	public List<Document> getAllDocs(){
		String query = "SELECT top 10 * FROM root";

		List<Document> documentList = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable().toList();

		return documentList;
	}


}
