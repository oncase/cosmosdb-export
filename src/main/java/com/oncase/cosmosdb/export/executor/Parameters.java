package com.oncase.cosmosdb.export.executor;

import com.beust.jcommander.Parameter;

public class Parameters {

	@Parameter(
		names = "--help", 
		help = true, 
		description = "Shows this beautiful help :)"
	)
	public boolean help;
	
    @Parameter(
    	names={"--database", "-d"}, 
    	description = "Specifies the database to point to"
    )
    public String db;
    
    @Parameter(
    	names={"--collection", "-c"}, 
    	description = "Specifies the collection to retrieve data from"
    )
    public String collection;
    
    @Parameter(
    	names={"--host", "-h"}, 
    	description = "Specifies host where the database is"
    )
    public String host;
    
    @Parameter(
    	names={"--key", "-k"}, 
    	description = "Specifies the master key to authorize connection"
    )
    public String key;
    
    @Parameter(
    	names={"--partitionkey", "-p"}, 
    	description = "Specifies the partition key to authorize connection"
    )
    public String partitionKey;
    
    @Parameter(
    	names={"--separator", "-s"}, 
    	description = "Specifies the field separator on the flat file"
    )
    public String separator;
    
    @Parameter(
    	names={"--linefeed", "-l"}, 
    	description = "Specifies line feed character. Eg.: \n"
    )
    public String lineFeed;
    
    @Parameter(
    	names={"--fields", "-f"}, 
    	description = "Comma-separated list of the fields to be exported"
    )
    public String config;
}
