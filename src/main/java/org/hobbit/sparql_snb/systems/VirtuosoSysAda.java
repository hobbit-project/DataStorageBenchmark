package org.hobbit.sparql_snb.systems;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.core.UpdateExecutionFactory;
import org.aksw.jena_sparql_api.core.UpdateExecutionFactoryHttp;
import org.aksw.jena_sparql_api.core.utils.UpdateRequestUtils;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.pagination.core.QueryExecutionFactoryPaginated;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.update.UpdateRequest;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.sparql_snb.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.atlas.web.auth.SimpleAuthenticator;

public class VirtuosoSysAda extends AbstractSystemAdapter {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoSysAda.class);
    private String virtuosoContName = "localhost";
    private QueryExecutionFactory queryExecFactory;
    private UpdateExecutionFactory updateExecFactory;
    
    private boolean phase2 = true;
    SortedSet<String> graphUris = new TreeSet<String>(); 
    private int insertsReceived = 0;
	private int insertsProcessed = 0;
    private int selectsReceived = 0;
    private int selectsProcessed = 0;
    
    private int counter = 0;
    
	public VirtuosoSysAda(int numberOfMessagesInParallel) {
		super(numberOfMessagesInParallel);
	}
	
	public VirtuosoSysAda() {
		
	}

	@Override
	public void receiveGeneratedData(byte[] arg0) {
		if (phase2 == true) {
			ByteBuffer dataBuffer = ByteBuffer.wrap(arg0);    	
			String fileName = RabbitMQUtils.readString(dataBuffer);
			LOGGER.info("Receiving graph URI " + fileName);
			graphUris.add(fileName);
			byte [] content = new byte[dataBuffer.remaining()];
			dataBuffer.get(content, 0, dataBuffer.remaining());
			//byte [] content = RabbitMQUtils.readByteArray(dataBuffer);

			if (content.length != 0) {
				FileOutputStream fos;
				try {
					if (fileName.contains("/"))
						fileName = "file" + String.format("%010d", counter++);
					fos = new FileOutputStream(System.getProperty("user.dir") + File.separator + "datasets" + File.separator + fileName);
					fos.write(content);
					fos.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else {			
            this.insertsReceived++;
            ByteBuffer buffer = ByteBuffer.wrap(arg0);
            // read the insert query
            String insertQuery = RabbitMQUtils.readString(buffer);
            // insert query
            UpdateRequest updateRequest = UpdateRequestUtils.parse(insertQuery);

            try {
                updateExecFactory.createUpdateProcessor(updateRequest).execute();
            } catch (Exception e) {
                e.printStackTrace();
            }

            this.insertsProcessed ++;
		}
	}

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		ByteBuffer buffer = ByteBuffer.wrap(data);
		String queryString = RabbitMQUtils.readString(buffer);
		LOGGER.info(queryString);
		if (queryString.contains("INSERT DATA")) {
			
			//TODO: Virtuoso hack
			queryString = queryString.replaceFirst("INSERT DATA", "INSERT");
			queryString += "WHERE { }\n";
			
						
	    	HttpAuthenticator auth = new SimpleAuthenticator("dba", "dba".toCharArray());
	    	updateExecFactory = new UpdateExecutionFactoryHttp("http://" + virtuosoContName + ":8890/sparql-auth", auth);
	    	UpdateRequest updateRequest = UpdateRequestUtils.parse(queryString);
            try {
            	updateExecFactory.createUpdateProcessor(updateRequest).execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
						
			try {
				this.sendResultToEvalStorage(taskId, RabbitMQUtils.writeString(""));
			} catch (IOException e) {
				LOGGER.error("Got an exception while sending results.", e);
			}
		}
		else {
			long timestamp1 = System.currentTimeMillis();
			this.selectsReceived++;
			// Create a QueryExecution object from a query string ...
			QueryExecution qe = queryExecFactory.createQueryExecution(queryString);
			// and run it.
			try {
				ResultSet results = qe.execSelect();
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				ResultSetFormatter.outputAsJSON(outputStream, results);
				try {
					this.sendResultToEvalStorage(taskId, outputStream.toByteArray());
					long timestamp2 = System.currentTimeMillis();
//					LOGGER.info("TIME: " + (timestamp2-timestamp1));
				} catch (IOException e) {
					LOGGER.error("Got an exception while sending results.", e);
				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				qe.close();
			}
			this.selectsProcessed++;
		}
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
    	
    	queryExecFactory = new QueryExecutionFactoryHttp("http://" + virtuosoContName + ":8890/sparql");
		//This is needed for ODIN bench
        queryExecFactory = new QueryExecutionFactoryPaginated(queryExecFactory, 100);
    	
        // create update factory
        HttpAuthenticator auth = new SimpleAuthenticator("dba", "dba".toCharArray());
        updateExecFactory = new UpdateExecutionFactoryHttp("http://" + virtuosoContName + ":8890/sparql-auth", auth);

    }
    
    @Override
    public void receiveCommand(byte command, byte[] data) {
    	//LOGGER.info("received command {}", Commands.toString(command));
    	if (VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED == command) {
    		
    		LOGGER.info("Bulk phase begins");
    		
            for (String uri : this.graphUris) {
                String create = "CREATE GRAPH " + "<" + uri + ">";
                UpdateRequest updateRequest = UpdateRequestUtils.parse(create);
                updateExecFactory.createUpdateProcessor(updateRequest).execute();
            }

            loadDataset();
    		
    		try {
    			String datasetsFolderName = System.getProperty("user.dir") + File.separator + "datasets"; 
    			File theDir = new File(datasetsFolderName);
    			FileUtils.deleteDirectory(theDir);
    			sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    		
            phase2 = false;
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
    		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
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
        if (this.insertsProcessed != this.insertsReceived) {
            LOGGER.error("INSERT queries received and processed are not equal (" + this.insertsReceived + " - " + this.insertsProcessed + ")");
        }
        if (this.selectsProcessed != this.selectsReceived) {
            LOGGER.error("SELECT queries received and processed are not equal (" + this.selectsReceived + " - " + this.selectsProcessed + ")");
        }
    	try {
    		queryExecFactory.close();
    		updateExecFactory.close();
    	} catch (Exception e) {
    	}
    	super.close();
    	LOGGER.info("Virtuoso has stopped.");
    }
	
}
