package org.hobbit.sparql_snb;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sparql_snb.util.VirtuosoSystemAdapterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBDataGenerator extends AbstractDataGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBDataGenerator.class);
	private String server, user, password, remoteFile;
    private boolean paramsGen;
    private Semaphore tasksReady = new Semaphore(0);
	
    public SNBDataGenerator(String server, String user, String password, String remoteFile) {
    	this.server = server;
    	this.user = user;
    	this.password = password;
    	this.remoteFile = remoteFile;
    	this.paramsGen = remoteFile.endsWith("tasks.txt");
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
    	
    	try {
    		if (paramsGen)
    			tasksReady.acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		downloadFileAndSendData();
	}
	
    @Override
	public void close() throws IOException {
		// Free the resources you requested here
    	
		
        // Always close the super class after yours!
        super.close();
    }
    
    private void downloadFileAndSendData() {
    	LOGGER.info("Downloading file " + remoteFile + " from " + server);
		FTPClient ftpClient = new FTPClient();
        try {
        	int port = 21;
            ftpClient.connect(server, port);
            ftpClient.login(user, password);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            
            InputStream inputStream = ftpClient.retrieveFileStream(remoteFile);
            byte[] bytesArray = null;
            
            if (!paramsGen) {
            	byte [] fileContent = IOUtils.toByteArray(inputStream);
            	String remoteFileName = remoteFile.replaceFirst(".*/", "");
            	ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
            	outputStream.write(ByteBuffer.allocate(4).putInt(remoteFileName.length()).array());
            	outputStream.write(RabbitMQUtils.writeString(remoteFileName));
            	outputStream.write(ByteBuffer.allocate(4).putInt(fileContent.length).array());
            	outputStream.write(fileContent);
                bytesArray = outputStream.toByteArray();
            	sendDataToSystemAdapter(bytesArray);           	
        	}
        	else {
                String msg = null;
                String fileContent = IOUtils.toString(inputStream);
        		String [] lines = fileContent.split("\n");
        		//TODO: Remove the following line
        		lines = Arrays.copyOfRange(lines, 0, 4499);
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
        	}
 
            boolean success = ftpClient.completePendingCommand();
            if (success) {
            	LOGGER.info("File " + remoteFile + " has been downloaded successfully and sent.");
            }
            inputStream.close();
 
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
	}
    
    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED) {
            // release the mutex
            tasksReady.release();
        }
        super.receiveCommand(command, data);
    }
}
