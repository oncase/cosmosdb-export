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
	 * @param String partKey
	 * @throws EmptyCollectionException
	 */
	public CollectionHandler(DatabaseHandler db, String coll, 
			boolean enablePartitionQuery) throws EmptyCollectionException {

		this.documentClient = db.getDocumentClient();
		this.db = db.getDatabase();

		options = new FeedOptions();

		options.setPageSize(100000);
		
		if( enablePartitionQuery ){
			options.setEnableCrossPartitionQuery(enablePartitionQuery);	
		}
		
		List<DocumentCollection> collectionList = this.documentClient
				.queryCollections( this.db.getSelfLink(),
						"SELECT * FROM root r WHERE r.id='" + coll + "'", 
						options).getQueryIterable().toList();

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
	 * Queries the collection with SELECT {fields} FROM root r WHERE {wherePart}
	 * and returns an Iterable to read from the query - good for big collections
	 * @param String fields - r. prefixed and comma separated
	 * @param String wherePart
	 * @return QueryIterable<Document> iterable
	 */
	public QueryIterable<Document> getIterableFieldsWhere(String limit, 
			String fields, String wherePart) {
		String query = "SELECT " + limit + fields +" FROM root r " +wherePart+"  ";
		System.out.println(query);
		QueryIterable<Document> iterable = documentClient.queryDocuments(
				collection.getSelfLink(), query, options).getQueryIterable();
		return iterable;
	}

	/**
	 * Queries the collection with SELECT {fields} FROM root r WHERE {wherePart}
	 * and returns a List with all Documents matched
	 * @param String fields - r. prefixed and comma separated
	 * @param String wherePart
	 * @return List<Document> documentList
	 */
	public List<Document> getDocsFieldsWhere(String limit, 
			String fields, String wherePart) {
		return getIterableFieldsWhere(limit, fields, wherePart).toList();
	}

}
