package org.hobbit.sparql_snb.systems;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.sparql_snb.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtuosoSystemAdapter extends AbstractSystemAdapter {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoSystemAdapter.class);
    private String virtuosoContName = "localhost";
    private QueryExecutionFactory queryExecFactory;
//    private UpdateExecutionFactory updateExecFactory;

    private int dataTerminationCount = 0;
    private int numberOfDataGenerators;
    
	public VirtuosoSystemAdapter(int numberOfDataGenerators) {
        this.numberOfDataGenerators = numberOfDataGenerators;
	}
	
	public VirtuosoSystemAdapter() {
        this.numberOfDataGenerators = 1;
	}

	@Override
	public void receiveGeneratedData(byte[] data) {
    	byte [] data1 = Arrays.copyOf(data, 4);	
    	int fileNameLength = ByteBuffer.wrap(data1).getInt();
    	
    	data1 = Arrays.copyOfRange(data, 4, 4 + fileNameLength);
    	String fileName = RabbitMQUtils.readString(data1);
    	
    	data1 = Arrays.copyOfRange(data, 4 + fileNameLength, 8 + fileNameLength);
    	int fileContentLength = ByteBuffer.wrap(data1).getInt();
    	
    	FileOutputStream fos;
		try {
			fos = new FileOutputStream(System.getProperty("user.dir") + File.separator + "datasets" + File.separator + fileName);
	    	fos.write(Arrays.copyOfRange(data, 8 + fileNameLength, 8 + fileNameLength + fileContentLength));
	    	fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
    	LOGGER.info("SPARQL query received.");
		String queryString = RabbitMQUtils.readString(data);
		
    	// Create a QueryExecution object from a query string ...
    	QueryExecution qe = queryExecFactory.createQueryExecution(queryString);
    	// and run it.
    	try {
    		ResultSet results = qe.execSelect();
    		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    		ResultSetFormatter.outputAsJSON(outputStream, results);
    		try {
    			this.sendResultToEvalStorage(taskId, outputStream.toByteArray());
    		} catch (IOException e) {
    			LOGGER.error("Got an exception while sending results.", e);
    		}

    	} catch (Exception e) {
    		e.printStackTrace();
    	} finally {
    		qe.close();
    	}
    	LOGGER.info("SELECT SPARQL query has been processed.");
	}
	
    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        internalInit();
        LOGGER.info("Initialization is over.");
    }
    
    private void internalInit() {
		String datasetsFolderName = "datasets"; 
		File theDir = new File(datasetsFolderName);
		theDir.mkdir();
    	
//    	String[] envVariablesVirtuoso = new String[] {
//    			"SPARQL_UPDATE=true",
//    			"DEFAULT_GRAPH=sib"
//    			};
//    	virtuosoContName = this.createContainer("tenforce/virtuoso:latest", envVariablesVirtuoso);
//    	virtuosoContName = "localhost";
    	queryExecFactory = new QueryExecutionFactoryHttp("http://" + virtuosoContName + ":8890/sparql");
    	
//
//    	try {
//    		TimeUnit.MINUTES.sleep(2);
//    	} catch (InterruptedException e) {
//    		// TODO Auto-generated catch block
//    		e.printStackTrace();
//    	}
    }
    
    @Override
    public void receiveCommand(byte command, byte[] data) {
        LOGGER.info("received command {}", Commands.toString(command));
    	if (VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED == command) {
    		LOGGER.info("Bulk phase begins");

    		try {
    			TimeUnit.SECONDS.sleep(2);
    		} catch (InterruptedException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    		loadDataset();
    		
    		try {
//    			String datasetsFolderName = System.getProperty("user.dir") + File.separator + "datasets"; 
//    			File theDir = new File(datasetsFolderName);
//    			FileUtils.deleteDirectory(theDir);
    			sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    		
    		LOGGER.info("Bulk phase is over.");
    	}
    	super.receiveCommand(command, data);
    }
    
    private void loadDataset() {
    	String scriptFilePath = System.getProperty("user.dir") + File.separator + "load.sh";
    	String[] command = {"/bin/bash", scriptFilePath, virtuosoContName, System.getProperty("user.dir") + File.separator + "datasets", "2"};
    	Process p;
    	try {
    		p = new ProcessBuilder(command).redirectErrorStream(true).start();
    		BufferedReader reader = 
                    new BufferedReader(new InputStreamReader(p.getInputStream()));
    		p.waitFor();
    		String line = null;
    		while ( (line = reader.readLine()) != null) {
    			LOGGER.info(line);
    		}
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	} catch (InterruptedException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }
    
    @Override
    public void close() throws IOException {
    	try {
    		queryExecFactory.close();
    	} catch (Exception e) {
    	}
//    	try {
//    		updateExecFactory.close();
//    	} catch (Exception e) {
//    	}
//    	this.stopContainer(virtuosoContName);
    	super.close();
    	LOGGER.info("Virtuoso has stopped.");
    }
	
    protected synchronized void dataGeneratorTerminated() {
        ++dataTerminationCount;
        if (dataTerminationCount == numberOfDataGenerators - 1) {
            try {
                sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED);
            } catch (IOException e) {
                e.printStackTrace();
            }        	
        }
    }
}
