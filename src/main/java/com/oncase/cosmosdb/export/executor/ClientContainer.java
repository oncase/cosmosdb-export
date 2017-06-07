package com.oncase.cosmosdb.export.executor;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;

/**
 * Singleton to store the DocumentClient connection
 */
public class ClientContainer {

	private static ClientContainer instance = null;
	private DocumentClient documentClient;

	protected ClientContainer() {

	}

	/**
	 * Configures the documentClient property
	 */
	public ClientContainer init(String host, String key) {
		documentClient = new DocumentClient(host, key, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
		return this;
	}

	/**
	 * Public method to get the instance from
	 */
	public static ClientContainer getInstance() {
		if (instance == null) {
			instance = new ClientContainer();
		}
		return instance;
	}
	
	public DocumentClient getDocumentClient(){
		return documentClient;
	}

}
