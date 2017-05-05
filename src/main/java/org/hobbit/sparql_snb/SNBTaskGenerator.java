package org.hobbit.sparql_snb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.hobbit.core.components.AbstractSequencingTaskGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sparql_snb.util.SNBConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBTaskGenerator extends AbstractSequencingTaskGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBTaskGenerator.class);
	private int numberOfOperations;
	
	public SNBTaskGenerator() {
		
	}
	
    @Override
    public void init() throws Exception {
        LOGGER.info("Initialization begins.");
        super.init();
        
        internalInit();
        LOGGER.info("Initialization is over.");
    }
    
    private void internalInit() {
    	Map<String, String> env = System.getenv();
    	
    	// Number of operations
    	if (!env.containsKey(SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS)) {
            LOGGER.error("Couldn't get \"" + SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS + "\" from the properties. Aborting.");
            System.exit(1);
        }
    	numberOfOperations = Integer.parseInt(env.get(SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS));	
	}

	@Override
	public void close() throws IOException {
		// Free the resources you requested here

		
        // Always close the super class after yours!
        super.close();
    }


	@Override
	protected void generateTask(byte[] data) throws Exception {
        String taskIdString = getNextTaskId();
        
        // Total number of operations achieved
        if (Integer.parseInt(taskIdString) >= numberOfOperations)
        	return;
        
        long timestamp = System.currentTimeMillis();
        
        String dataString = RabbitMQUtils.readString(data);
        String [] lines = dataString.split("\n");
        String queryText = prepareQueryText(lines[0]);
        data = RabbitMQUtils.writeString(queryText);
        sendTaskToSystemAdapter(taskIdString, data);

        data = RabbitMQUtils.writeString(dataString);
        sendTaskToEvalStorage(taskIdString, timestamp, data);
	}
	
    private String prepareQueryText(String text) throws Exception {
    	String [] parts = text.split("[{]");
    	String queryType = parts[0];
    	String [] arguments = parts[1].substring(0, parts[1].length()-1).split(", ");
    	String queryString = null;
    	if (queryType.startsWith("LdbcQuery"))
    		queryString = file2string(new File("snb_queries", "query" + queryType.replaceAll("[^0-9]*", "") + ".txt"));
    	else
    		queryString = file2string(new File("snb_queries", "s" + queryType.replaceAll("[^0-9]*", "") + ".txt"));
    	for (String arg : arguments) {
			String [] tmp = arg.split("=");
			switch (tmp[0]) {
			case "personId":
				if (queryType.startsWith("LdbcQuery"))
					queryString = queryString.replaceAll("%" + tmp[0] + "%", String.format("%020d", Long.parseLong(tmp[1])));
				else
					queryString = queryString.replaceAll("%" + tmp[0] + "%", tmp[1]);
				break;
			case "maxDate":
			case "minDate":
			case "startDate":
				DateFormat format1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
				Date date = format1.parse(tmp[1]);
				DateFormat format2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
				queryString = queryString.replaceAll("%" + tmp[0] + "%", format2.format(date));
				break;
			case "month":
				queryString = queryString.replaceAll("%month1%", tmp[1]);
				int nextMonth = Integer.parseInt(tmp[1]) + 1;
				if (nextMonth == 13)
				    nextMonth = 1;
				queryString = queryString.replaceAll("%month2%", String.valueOf(nextMonth));
				break;
			case "countryXName":
			case "countryYName":
				queryString = queryString.replaceAll("%" + tmp[0] + "%", tmp[1].substring(1, tmp[1].length()-1));
				break;
			default:
				queryString = queryString.replaceAll("%" + tmp[0] + "%", tmp[1]);
				break;
			}
		}

		return queryString;
	}
    
	private String file2string(File file) throws Exception {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			StringBuffer sb = new StringBuffer();

			while (true) {
				String line = reader.readLine();
				if (line == null)
					break;
				else {
					sb.append(line);
					sb.append("\n");
				}
			}
			return sb.toString();
		} catch (IOException e) {
			throw new Exception("Error openening or reading file: " + file.getAbsolutePath(), e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
