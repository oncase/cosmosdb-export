package com.oncase.cosmosdb.export;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

@Parameters(separators = "=", commandDescription = "Record changes to the repository")

public class ExecutionParameters {
	/* Execution ------------------------------------------------------------ */
	@Parameter(
		names = "--help", 
		help = true, 
		description = "Shows this beautiful help :)"
	)
	public boolean help;

	/* Communication -------------------------------------------------------- */
    @Parameter(
    	names = {"--database", "-d"}, 
    	description = "Specifies the database to point to",
    	required = true
    )
    public String db;
    
    @Parameter(
    	names = {"--collection", "-c"}, 
    	description = "Specifies the collection to retrieve data from",
    	required = true
    )
    public String collection;
    
    @Parameter(
    	names = {"--host", "-h"}, 
    	description = "Specifies host where the database is",
    	required = true
    )
    public String host;
    
    @Parameter(
    	names = {"--key", "-k"}, 
    	description = "Specifies the master key to authorize connection",
    	required = true
    )
    public String key;
    
    @Parameter(
    	names = {"--enable-partition-query", "-ep"}, 
    	description = "Whether to enable or not partitioned queries"
    )
    public boolean enablePartitionQuery = false;

	/* Querying ------------------------------------------------------------- */
    @Parameter(
    	names = {"--fields", "-f"}, 
    	description = "Comma-separated list of the fields to be requested"
    )
    public String fields = "*";
    
    @Parameter(
    	names = {"--where", "-w"}, 
    	description = "Where clause to the query. Fields must be `r.` prefixed"
    )
    public String where = "";
    
    @Parameter(
    	names = {"--limit", "-w"}, 
    	description = "Where clause to the query. Fields must be `r.` prefixed"
    )
    private int limit = -1;

	/* Output --------------------------------------------------------------- */
    @Parameter(
    	names = {"--separator", "-s"}, 
    	description = "Specifies a field separator for the csv type. Default: ;"
    )
    public String separator = ";";
    
    @Parameter(
    	names = {"--enclosure", "-e"}, 
    	description = "Specifies line enclosure character. Default.: \""
    )
    public String enclosure = "\"";
    
    @Parameter(
    	names = {"--linefeed", "-lf"}, 
    	description = "Specifies line feed character. Eg.: \n"
    )
    public String lineFeed;
    
    @Parameter(
    	names = {"--file", "-o"}, 
    	description = "The path of the file to output to"
    )
    public String file;
    
    @Parameter(
    	names = {"--type", "-t"}, 
    	description = "The output type (csv | json). Default: json"
    )
    public String type;
    
    @Parameter(
    	names = {"--no-header", "-nh"}, 
    	description = "Disables the header for --type=csv"
    )
    public boolean noHeader = false;
    
    public String getLimit(){
    	return limit == -1 ? "" : " TOP "+limit+"";
    }
    
    public void setLimit(int limit){
    	this.limit = limit;
    }
    
    public String getQueryFields(){
    	if(fields == "*") return fields;
    	
		String[] fieldsArray = fields.split(",");
		String queryFields = "";
		for(int x = 0 ; x < fieldsArray.length ; x++){
			queryFields += "r." + fieldsArray[x].trim() + ",";
		}
		queryFields = queryFields.substring(0, queryFields.length()-1);
		return queryFields;
    }
}
