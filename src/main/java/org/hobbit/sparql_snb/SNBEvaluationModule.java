package org.hobbit.sparql_snb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.hobbit.core.components.AbstractEvaluationModule;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBEvaluationModule extends AbstractEvaluationModule {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBEvaluationModule.class);
    private ArrayList<String> wrongAnswers = new ArrayList<>();
    private Map<String, ArrayList<Long> > executionTimes = new HashMap<>();

	
    @Override
    public void init() throws Exception {
        // Always init the super class first!
        super.init();
		
		// Your initialization code comes here...
    }
    
	@Override
	protected void evaluateResponse(byte[] expectedData, byte[] receivedData, long taskSentTimestamp,
			long responseReceivedTimestamp) throws Exception {
		LOGGER.info("Evaluate response");
		String eStr = RabbitMQUtils.readString(expectedData);
    	String rStr = RabbitMQUtils.readString(receivedData);
    	String [] lines = eStr.split("\n");
        //String taskId = lines[0];
        String type = lines[0].replaceAll("[{].*", "");
        String eAnswers = eStr.replaceFirst(".*\n", "");
        		        
        if (!eAnswers.equals(rStr)) {
        	wrongAnswers.add(lines[0]);
        	wrongAnswers.add(eAnswers);
        	wrongAnswers.add(rStr);
        }
        
        if (!executionTimes.containsKey(type))
        	executionTimes.put(type, new ArrayList<Long>());
        executionTimes.get(type).add(responseReceivedTimestamp - taskSentTimestamp);
	}

	@Override
	protected Model summarizeEvaluation() throws Exception {
		// All tasks/responsens have been evaluated. Summarize the results,
		// write them into a Jena model and send it to the benchmark controller.
		LOGGER.info("Summarize evaluation...");
		try {
			TimeUnit.MINUTES.sleep(1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (Map.Entry<String, ArrayList<Long>> entry : executionTimes.entrySet()) {
    		long total = 0;
    		for (long l : entry.getValue()) {
				total += l;
			}
    		LOGGER.info(entry.getKey() + "-" + ((double)total)/entry.getValue().size());
		}
    	for (int i = 0; i < wrongAnswers.size(); i+=3) {
    		LOGGER.info("Wrong answer on query:");
    		LOGGER.info(wrongAnswers.get(i));
    		LOGGER.info("Expected:");
    		LOGGER.info(wrongAnswers.get(i+1));
    		LOGGER.info("Actual:");
    		LOGGER.info(wrongAnswers.get(i+2));
		}
        return ModelFactory.createDefaultModel();
	}
	
    @Override
	public void close() throws IOException {
		// Free the resources you requested here
    	
		
        // Always close the super class after yours!
        super.close();
    }

}
