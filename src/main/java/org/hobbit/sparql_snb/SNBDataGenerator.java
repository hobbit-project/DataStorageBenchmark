package org.hobbit.sparql_snb;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sparql_snb.util.VirtuosoSystemAdapterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBDataGenerator extends AbstractDataGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBDataGenerator.class);
	private Semaphore generateTasks = new Semaphore(0);
	
    public SNBDataGenerator() {
    	
    }
    
    @Override
    public void init() throws Exception {
        // Always init the super class first!
        super.init();

		// Your initialization code comes here...
        
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
    	String directory = "http://hobbitdata.informatik.uni-leipzig.de/mighty-storage-challenge/Task2/sf1/";
    	String[] files = {"social_network_static_0_0.ttl.gz", "social_network_person_0_0.ttl.gz", "social_network_activity_0_0.ttl.gz"};
    	try { 
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

    			LOGGER.info("File " + remoteFile + " has been downloaded successfully and sent.");
    			inputStream.close();
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
        	String remoteFile = "http://hobbitdata.informatik.uni-leipzig.de/mighty-storage-challenge/Task2/sf1/tasks.txt";
        	LOGGER.info("Downloading file " + remoteFile);
        	try {            
        		InputStream inputStream = new URL(remoteFile).openStream();
        		byte[] bytesArray = null;

        		String msg = null;
        		String fileContent = IOUtils.toString(inputStream);
        		String [] lines = fileContent.split("\n");
        		//TODO: Remove the following line
        		lines = Arrays.copyOfRange(lines, 0, 199300);
        		int current1 = 0;
        		int current2 = current1 + 1;
        		while (current1 < lines.length) {
        			while (current2 < lines.length && !lines[current2].startsWith("Ldbc"))
        				current2++;
        			msg = StringUtils.join(Arrays.copyOfRange(lines, current1, current2), "\n");
        			bytesArray = RabbitMQUtils.writeString(msg);
        			sendDataToTaskGenerator(bytesArray);
        			current1 = current2;
        			current2 = current1 + 1;
        		}

        		LOGGER.info("File " + remoteFile + " has been downloaded successfully and sent.");
        		inputStream.close();
        	} catch (IOException ex) {
        		System.out.println("Error: " + ex.getMessage());
        		ex.printStackTrace();
        	}
        	generateTasks.release();
        }
        super.receiveCommand(command, data);
    }
}