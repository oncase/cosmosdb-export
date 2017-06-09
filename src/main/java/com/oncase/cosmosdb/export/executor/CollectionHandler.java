package com.oncase.cosmosdb.export.executor;

import java.util.List;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.QueryIterable;
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
	 * Gets the instance collection
	 * @return DocumentCollection collection
	 */
	public DocumentCollection getCollection(){
		return collection;
	}

	/**
	 * Gets the instance documentClient. Useful to closing connection after done
	 * @return DocumentClient documentClient
	 */
	public DocumentClient getDocumentClient() {
		return documentClient;
	}

	/**
	 * Gets the instance DB
	 * @return Database db
	 */
	public Database getDb() {
		return db;
	}

	/**
	 * Gets the current feed options used to perform the query
	 * @return FeedOptions options
	 */
	public FeedOptions getOptions() {
		return options;
	}

	/**
	 * Queries the collection with SELECT * FROM root r WHERE {wherePart}
	 * and returns a List with all Documents matched
	 * @param String wherePart
	 * @return List<Document> documentList
	 */
	public List<Document> getDocsWhere(String wherePart) {

		String query = "SELECT * FROM root WHERE " +wherePart+"  ";
		
		List<Document> documentList = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable().toList();

		return documentList;

	}

	/**
	 * Queries the collection with SELECT * FROM root r WHERE {wherePart}
	 * and returns an Iterable to read from the query - good for big collections
	 * @param String wherePart
	 * @return QueryIterable<Document> iterable
	 */
	public QueryIterable<Document> getIterableWhere(String wherePart) {

		String query = "SELECT * FROM root WHERE " +wherePart+"  ";
		
		QueryIterable<Document> iterable = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable();

		return iterable;

	}
	
	/**
	 * Queries the collection with SELECT {fields} FROM root r WHERE {wherePart}
	 * and returns a List with all Documents matched
	 * @param String fields - r. prefixed and comma separated
	 * @param String wherePart
	 * @return List<Document> documentList
	 */
	public List<Document> getDocsFieldsWhere(String fields, String wherePart) {

		String query = "SELECT "+ fields +" FROM root r WHERE " +wherePart+"  ";
		System.out.println(query);
		List<Document> documentList = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable().toList();

		return documentList;

	}
	
	/**
	 * Queries the collection with SELECT {fields} FROM root r WHERE {wherePart}
	 * and returns an Iterable to read from the query - good for big collections
	 * @param String fields - r. prefixed and comma separated
	 * @param String wherePart
	 * @return QueryIterable<Document> iterable
	 */
	public QueryIterable<Document> getIterableFieldsWhere(String fields, 
			String wherePart) {

		String query = "SELECT "+ fields +" FROM root r WHERE " +wherePart+"  ";
		System.out.println(query);
		QueryIterable<Document> iterable = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable();

		return iterable;

	}
	
	/**
	 * Queries the collection with SELECT * FROM root
	 * and returns a List with all Documents from the collection.
	 * NOTE: When querying a large collection, this method will try to put all
	 * the matched documents in memory. Use getAllIterable() instead.
	 * @return List<Document> documentList
	 */
	public List<Document> getAllDocs(){
		String query = "SELECT * FROM root";

		List<Document> documentList = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable().toList();

		return documentList;
	}
	
	/**
	 * Queries the collection with SELECT * FROM root
	 * and returns an Iterable to read from the query - good for big collections
	 * @return QueryIterable<Document> iterable
	 */
	public QueryIterable<Document> getAllIterable(){
		String query = "SELECT * FROM root";

		QueryIterable<Document> iterable = documentClient
				.queryDocuments(collection.getSelfLink(),
						query, options).getQueryIterable();

		return iterable;
	}


}
