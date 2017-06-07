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

	public static Parameters params;

	/**
	 * Main method receives the parameters specified in the Parameters class
	 * 
	 * @throws EmptyDatabaseException
	 * @throws EmptyCollectionException
	 */
	public static void main(String[] args) 
			throws EmptyDatabaseException, EmptyCollectionException {
		params = new Parameters();
		JCommander jc = JCommander.newBuilder().addObject(params).build();
		jc.parse(args);
		jc.setProgramName("exporter.sh");

		// If help command issued, only show the usage
		if (params.help) {
			jc.usage();
		} else {
			run();
		}
	}

	/**
	 * Executes the given command
	 * 
	 * @throws EmptyDatabaseException
	 * @throws EmptyCollectionException
	 */
	public static void run() 
			throws EmptyDatabaseException, EmptyCollectionException {

		List<Document> docs = getDocsFieldsWhere(" r.date = '2017-03-15' ").toList();

		Iterator<Document> docsIterator = docs.iterator();
		int count = 0;
		
		while( count < 3 || docsIterator.hasNext() ){

			Document doc = docsIterator.next();
			System.out.println(doc.toString());

			count++;
		}
		
		System.out.println("Terminated");
		System.exit(0);

	}
	
	public static QueryIterable<Document> getDocsFieldsWhere(String whereClause) 
			throws EmptyDatabaseException, EmptyCollectionException{

		// Client
		ClientContainer cli = ClientContainer.getInstance();
		cli.init(params.host, params.key);

		// Database
		DatabaseHandler db = new DatabaseHandler(cli.getDocumentClient(), params.db);
		// Collection
		CollectionHandler coll;
		if( params.enablePartitionQuery ){
			coll = new CollectionHandler(db, params.collection, params.enablePartitionQuery);
		} else {
			coll = new CollectionHandler(db, params.collection);
		}

		// Query construction
		String[] fields = params.fields.split(",");
		String queryFields = "";
		for(int x = 0 ; x < fields.length ; x++){
			queryFields += "r." + fields[x].trim() + ",";
		}
		queryFields = queryFields.substring(0, queryFields.length()-1);
		
		QueryIterable<Document> iterable = coll.getIterableFieldsWhere(queryFields, whereClause);

		return iterable;

	}
	
	

}
