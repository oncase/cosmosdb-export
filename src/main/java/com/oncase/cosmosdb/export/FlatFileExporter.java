package com.oncase.cosmosdb.export;

import java.util.Iterator;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.QueryIterable;
import com.oncase.cosmosdb.export.executor.ClientContainer;
import com.oncase.cosmosdb.export.executor.CollectionHandler;
import com.oncase.cosmosdb.export.executor.DatabaseHandler;
import com.oncase.cosmosdb.export.executor.exception.EmptyCollectionException;
import com.oncase.cosmosdb.export.executor.exception.EmptyDatabaseException;

public class FlatFileExporter {

	public static ExecutionParameters params;

	/**
	 * Main method receives the parameters specified in the Parameters class
	 * 
	 * @throws EmptyDatabaseException
	 * @throws EmptyCollectionException
	 */
	public static void main(String[] args) 
			throws EmptyDatabaseException, EmptyCollectionException {
		params = new ExecutionParameters();
		JCommander jc = JCommander.newBuilder().addObject(params).build();
		jc.parse(args);
		jc.setProgramName("cosmosdb-export");

		// If help command issued, only show the usage
		if (params.help) {
			jc.usage();
		} else {
			runCommand();
		}
	}

	/**
	 * Executes the given command
	 * 
	 * @throws EmptyDatabaseException
	 * @throws EmptyCollectionException
	 */
	private static void runCommand() 
			throws EmptyDatabaseException, EmptyCollectionException {

		CollectionHandler coll = getCollectionHandler();
		
		QueryIterable<Document> iterable = coll.getIterableFieldsWhere(
				params.getLimit(), params.getQueryFields(), params.where);

		//TODO: iterate over the resultset and write to file
		
		coll.getDocumentClient().close();
		System.out.println("Terminated");
		System.exit(0);

	}
	
	public static CollectionHandler getCollectionHandler() 
			throws EmptyDatabaseException, EmptyCollectionException{

		// Client
		ClientContainer cli = ClientContainer.getInstance();
		cli.init(params.host, params.key);

		// Database
		DatabaseHandler db = new DatabaseHandler(
				cli.getDocumentClient(), params.db);
		
		// Collection
		return new CollectionHandler(
				db, params.collection, params.enablePartitionQuery);
	}	
	

}
