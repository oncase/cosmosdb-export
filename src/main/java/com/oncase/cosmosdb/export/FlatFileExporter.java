package com.oncase.cosmosdb.export;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.QueryIterable;
import com.oncase.cosmosdb.export.executor.ClientContainer;
import com.oncase.cosmosdb.export.executor.CollectionHandler;
import com.oncase.cosmosdb.export.executor.DatabaseHandler;
import com.oncase.cosmosdb.export.executor.exception.EmptyCollectionException;
import com.oncase.cosmosdb.export.executor.exception.EmptyDatabaseException;
import com.opencsv.CSVWriter;

public class FlatFileExporter {

	public static ExecutionParameters params;

	/**
	 * Main method receives the parameters specified in the Parameters class
	 * 
	 * @throws EmptyDatabaseException
	 * @throws EmptyCollectionException
	 * @throws DocumentClientException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws EmptyDatabaseException, 
			EmptyCollectionException, DocumentClientException, IOException {
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
	 * @throws DocumentClientException 
	 * @throws IOException 
	 */
	private static void runCommand() throws EmptyDatabaseException, 
			EmptyCollectionException, DocumentClientException, IOException {

		CollectionHandler coll = getCollectionHandler(params);

		QueryIterable<Document> iterable = coll.getIterableFieldsWhere(
				params.getLimit(), params.getQueryFields(), params.where);


		List<Document> next = iterable.fetchNextBlock();
		String[] fields = params.getFieldsArray();
		
		System.out.println("Output file: ");
		System.out.println(params.file);
		CSVWriter writer = new CSVWriter(
				new FileWriter(params.file), params.separator, params.enclosure);

		while( next != null && next.size() > 0 ){
			Iterator<Document> docs = next.iterator();

			while(docs.hasNext()){
				Document doc = docs.next();
				String[] cols = new String[fields.length];

				for(int i = 0 ; i < cols.length ; i++) {
					try {
						cols[i] = getOutputVal( doc.get( fields[i] ) );
						
					} catch(Exception e) {
						cols[i] ="";
						System.out.println("Error");
						System.out.println(doc.get( fields[i] ));
					}
				}
				writer.writeNext(cols);
			}

			next = iterable.fetchNextBlock();
		}
		
		writer.close();
		coll.getDocumentClient().close();
		System.out.println("Terminated");
		System.exit(0);

	}

	public static CollectionHandler getCollectionHandler(ExecutionParameters p) 
			throws EmptyDatabaseException, EmptyCollectionException{

		// Client
		ClientContainer cli = ClientContainer.getInstance();
		cli.init(p.host, p.key);

		// Database
		DatabaseHandler db = new DatabaseHandler(
				cli.getDocumentClient(), p.db);
		
		// Collection
		return new CollectionHandler(db, p.collection, 
				p.enablePartitionQuery, p.pageSize);
		
	}

	static String getOutputVal( Object value ){
		if( value == null || value == ""){
			return "";
		} else {
			return String.valueOf(value);
		}
	}


}
