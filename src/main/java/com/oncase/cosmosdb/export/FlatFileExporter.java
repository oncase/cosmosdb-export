package com.oncase.cosmosdb.export;

import com.beust.jcommander.JCommander;
import com.oncase.cosmosdb.export.executor.Parameters;

public class FlatFileExporter {

	private static Parameters params;

    public static void main( String[] args ){
    	params = new Parameters();
    	JCommander jc = JCommander.newBuilder()
            .addObject(params)
            .build();
        jc.parse(args);
        jc.setProgramName("exporter.sh");
        
        if(params.help){
        	jc.usage();
        } else {
        	run();
        }
    }

	private static void run() {
		System.out.println(params.host);
	}

}
