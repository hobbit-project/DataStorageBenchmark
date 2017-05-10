package org.hobbit.sparql_snb;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sparql_snb.util.SNBConstants;
import org.hobbit.sparql_snb.util.VirtuosoSystemAdapterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBDataGenerator extends AbstractDataGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBDataGenerator.class);
	private Semaphore generateTasks = new Semaphore(0);
	private int scaleFactor;
	private int numberOfOperations;
	
    public SNBDataGenerator() {
    	
    }
    
    @Override
    public void init() throws Exception {
    	LOGGER.info("Initialization begins.");
        // Always init the super class first!
        super.init();

		// Your initialization code comes here...
        internalInit();
        LOGGER.info("Initialization is over.");
    }
    
	private void internalInit() {
    	Map<String, String> env = System.getenv();
    	
    	// Number of operations
    	if (!env.containsKey(SNBConstants.GENERATOR_SCALE_FACTOR)) {
            LOGGER.error("Couldn't get \"" + SNBConstants.GENERATOR_SCALE_FACTOR + "\" from the properties. Aborting.");
            System.exit(1);
        }
    	scaleFactor = Integer.parseInt(env.get(SNBConstants.GENERATOR_SCALE_FACTOR));
    	    	
    	// Number of operations
    	if (!env.containsKey(SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS)) {
            LOGGER.error("Couldn't get \"" + SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS + "\" from the properties. Aborting.");
            System.exit(1);
        }
    	numberOfOperations = Integer.parseInt(env.get(SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS));
	}

	@Override
	protected void generateData() throws Exception {
		LOGGER.info("Data Generator is running...");
    	
		downloadFileAndSendData();
		
		generateTasks.acquire();
	}
	
    @Override
	public void close() throws IOException {
		// Free the resources you requested here
    	
		
        // Always close the super class after yours!
        super.close();
    }
    
    private void downloadFileAndSendData() {
    	String directory = "http://hobbitdata.informatik.uni-leipzig.de/mighty-storage-challenge/Task2/sf" + scaleFactor + "/";
    	String datasetFiles = directory + "dataset_files.txt";
    	try { 
			InputStream is = new URL(datasetFiles).openStream();
			String [] files = IOUtils.toString(is).split("\n");
			//TODO: remove the following:
			//files = new String[0];
			is.close();
			
    		for (String remoteFile : files) {
    			remoteFile = directory + remoteFile;
    			LOGGER.info("Downloading file " + remoteFile);           
    			InputStream inputStream = new URL(remoteFile).openStream();
    			byte[] bytesArray = null;

    			byte [] fileContent = IOUtils.toByteArray(inputStream);
    			String remoteFileName = remoteFile.replaceFirst(".*/", "");
    			ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
    			outputStream.write(ByteBuffer.allocate(4).putInt(remoteFileName.length()).array());
    			outputStream.write(RabbitMQUtils.writeString(remoteFileName));
    			outputStream.write(ByteBuffer.allocate(4).putInt(fileContent.length).array());
    			outputStream.write(fileContent);
    			bytesArray = outputStream.toByteArray();
    			sendDataToSystemAdapter(bytesArray);           	
    			inputStream.close();
    			
    			LOGGER.info("File " + remoteFile + " has been downloaded successfully and sent.");
    		}
    		sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED);
    	} catch (IOException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
	}
    
    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED) {
        	String tasksFile = "http://hobbitdata.informatik.uni-leipzig.de/mighty-storage-challenge/Task2/sf1/tasks.txt";
        	String answersFile = "http://hobbitdata.informatik.uni-leipzig.de/mighty-storage-challenge/Task2/sf1/answers.txt";
        	LOGGER.info("Downloading tasks");
        	try {            
        		InputStream inputStream1 = new URL(tasksFile).openStream();
        		BufferedReader inputStream2 = new BufferedReader(new InputStreamReader(new URL(answersFile).openStream()));
        		byte[] bytesArray = null;
        		String fileContent = IOUtils.toString(inputStream1);
        		String [] lines = fileContent.split("\n");
        		for (int i = 0; i < lines.length && i < numberOfOperations; i++) {
        			StringBuilder builder = new StringBuilder();
        			builder.append(lines[i]);
        			if (!lines[i].startsWith("LdbcUpdate")) {
        				String l = null;
        				while(!(l = inputStream2.readLine()).equals("")) {
        				    builder.append("\n" + l);
        				}
        			}
        			bytesArray = RabbitMQUtils.writeString(builder.toString());
        			sendDataToTaskGenerator(bytesArray);
        		}
        		LOGGER.info("Files with tasks have been downloaded successfully and sent.");
        		inputStream1.close();
        		inputStream2.close();
        	} catch (IOException ex) {
        		System.out.println("Error: " + ex.getMessage());
        		ex.printStackTrace();
        	}
        	generateTasks.release();
        }
        super.receiveCommand(command, data);
    }
}