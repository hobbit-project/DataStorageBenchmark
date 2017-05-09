package org.hobbit.sparql_snb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractEvaluationModule;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.sparql_snb.util.SNBConstants;
import org.hobbit.vocab.HOBBIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBEvaluationModule extends AbstractEvaluationModule {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBEvaluationModule.class);
	
	/* Final evaluation model */
	private Model finalModel = ModelFactory.createDefaultModel();
	/* Property for query execution average time */
	private Property EVALUATION_QE_AVERAGE_TIME = null;
	
	/* Properties for query execution average time per type */
	private Property EVALUATION_Q01E_AVERAGE_TIME = null;
	private Property EVALUATION_Q02E_AVERAGE_TIME = null;
	private Property EVALUATION_Q03E_AVERAGE_TIME = null;
	private Property EVALUATION_Q04E_AVERAGE_TIME = null;
	private Property EVALUATION_Q05E_AVERAGE_TIME = null;
	private Property EVALUATION_Q06E_AVERAGE_TIME = null;
	private Property EVALUATION_Q07E_AVERAGE_TIME = null;
	private Property EVALUATION_Q08E_AVERAGE_TIME = null;
	private Property EVALUATION_Q09E_AVERAGE_TIME = null;
	private Property EVALUATION_Q10E_AVERAGE_TIME = null;
	private Property EVALUATION_Q11E_AVERAGE_TIME = null;
	private Property EVALUATION_Q12E_AVERAGE_TIME = null;
	private Property EVALUATION_Q13E_AVERAGE_TIME = null;
	private Property EVALUATION_Q14E_AVERAGE_TIME = null;
	private Property EVALUATION_S1E_AVERAGE_TIME = null;
	private Property EVALUATION_S2E_AVERAGE_TIME = null;
	private Property EVALUATION_S3E_AVERAGE_TIME = null;
	private Property EVALUATION_S4E_AVERAGE_TIME = null;
	private Property EVALUATION_S5E_AVERAGE_TIME = null;
	private Property EVALUATION_S6E_AVERAGE_TIME = null;
	private Property EVALUATION_S7E_AVERAGE_TIME = null;
	private Property EVALUATION_U1E_AVERAGE_TIME = null;
	private Property EVALUATION_U2E_AVERAGE_TIME = null;
	private Property EVALUATION_U3E_AVERAGE_TIME = null;
	private Property EVALUATION_U4E_AVERAGE_TIME = null;
	private Property EVALUATION_U5E_AVERAGE_TIME = null;
	private Property EVALUATION_U6E_AVERAGE_TIME = null;
	private Property EVALUATION_U7E_AVERAGE_TIME = null;
	private Property EVALUATION_U8E_AVERAGE_TIME = null;
	
	/* Property for loading time" */
	private Property EVALUATION_LOADING_TIME = null;
	
	/* Property for throughput" */
	private Property EVALUATION_THROUGHPUT = null;

	
    private ArrayList<String> wrongAnswers = new ArrayList<>();
    private Map<String, ArrayList<Long> > executionTimes = new HashMap<>();
    
    private Map<String, Long> totalTimePerQueryType = new HashMap<>();
    private Map<String, Integer> numberOfQueriesPerQueryType = new HashMap<>();

	
    @Override
    public void init() throws Exception {
    	LOGGER.info("Initialization begins.");
        // Always init the super class first!
        super.init();
		
		// Your initialization code comes here...
        internalInit();
        
        LOGGER.info("Initialization ends.");
    }
    
	private void internalInit() {
		totalTimePerQueryType.put("LdbcQuery1", (long) 0);
		totalTimePerQueryType.put("LdbcQuery2", (long) 0);
		totalTimePerQueryType.put("LdbcQuery3", (long) 0);
		totalTimePerQueryType.put("LdbcQuery4", (long) 0);
		totalTimePerQueryType.put("LdbcQuery5", (long) 0);
		totalTimePerQueryType.put("LdbcQuery6", (long) 0);
		totalTimePerQueryType.put("LdbcQuery7", (long) 0);
		totalTimePerQueryType.put("LdbcQuery8", (long) 0);
		totalTimePerQueryType.put("LdbcQuery9", (long) 0);
		totalTimePerQueryType.put("LdbcQuery10", (long) 0);
		totalTimePerQueryType.put("LdbcQuery11", (long) 0);
		totalTimePerQueryType.put("LdbcQuery12", (long) 0);
		totalTimePerQueryType.put("LdbcQuery13", (long) 0);
		totalTimePerQueryType.put("LdbcQuery14", (long) 0);
		totalTimePerQueryType.put("LdbcShortQuery1PersonProfile", (long) 0);
		totalTimePerQueryType.put("LdbcShortQuery2PersonPosts", (long) 0);
		totalTimePerQueryType.put("LdbcShortQuery3PersonFriends", (long) 0);
		totalTimePerQueryType.put("LdbcShortQuery4MessageContent", (long) 0);
		totalTimePerQueryType.put("LdbcShortQuery5MessageCreator", (long) 0);
		totalTimePerQueryType.put("LdbcShortQuery6MessageForum", (long) 0);
		totalTimePerQueryType.put("LdbcShortQuery7MessageReplies", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate1AddPerson", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate2AddPostLike", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate3AddCommentLike", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate4AddForum", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate5AddForumMembership", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate6AddPost", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate7AddComment", (long) 0);
		totalTimePerQueryType.put("LdbcUpdate8AddFriendship", (long) 0);
		
		
		numberOfQueriesPerQueryType.put("LdbcQuery1", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery2", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery3", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery4", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery5", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery6", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery7", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery8", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery9", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery10", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery11", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery12", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery13", 0);
		numberOfQueriesPerQueryType.put("LdbcQuery14", 0);
		numberOfQueriesPerQueryType.put("LdbcShortQuery1PersonProfile", 0);
		numberOfQueriesPerQueryType.put("LdbcShortQuery2PersonPosts", 0);
		numberOfQueriesPerQueryType.put("LdbcShortQuery3PersonFriends", 0);
		numberOfQueriesPerQueryType.put("LdbcShortQuery4MessageContent", 0);
		numberOfQueriesPerQueryType.put("LdbcShortQuery5MessageCreator", 0);
		numberOfQueriesPerQueryType.put("LdbcShortQuery6MessageForum", 0);
		numberOfQueriesPerQueryType.put("LdbcShortQuery7MessageReplies", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate1AddPerson", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate2AddPostLike", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate3AddCommentLike", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate4AddForum", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate5AddForumMembership", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate6AddPost", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate7AddComment", 0);
		numberOfQueriesPerQueryType.put("LdbcUpdate8AddFriendship", 0);
		
		
		Map<String, String> env = System.getenv();
		
        /* average query time */
        if (!env.containsKey(SNBConstants.EVALUATION_QE_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_QE_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        EVALUATION_QE_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_QE_AVERAGE_TIME));
        
        
        /* average query times per type */
        if (!env.containsKey(SNBConstants.EVALUATION_Q01E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q01E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q02E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q02E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q03E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q03E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q04E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q04E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q05E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q05E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q06E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q06E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q07E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q07E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q08E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q08E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q09E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q09E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q10E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q10E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q11E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q11E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q12E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q12E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q13E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q13E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_Q14E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_Q14E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        EVALUATION_Q01E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q01E_AVERAGE_TIME));
        EVALUATION_Q02E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q02E_AVERAGE_TIME));
        EVALUATION_Q03E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q03E_AVERAGE_TIME));
        EVALUATION_Q04E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q04E_AVERAGE_TIME));
        EVALUATION_Q05E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q05E_AVERAGE_TIME));
        EVALUATION_Q06E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q06E_AVERAGE_TIME));
        EVALUATION_Q07E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q07E_AVERAGE_TIME));
        EVALUATION_Q08E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q08E_AVERAGE_TIME));
        EVALUATION_Q09E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q09E_AVERAGE_TIME));
        EVALUATION_Q10E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q10E_AVERAGE_TIME));
        EVALUATION_Q11E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q11E_AVERAGE_TIME));
        EVALUATION_Q12E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q12E_AVERAGE_TIME));
        EVALUATION_Q13E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q13E_AVERAGE_TIME));
        EVALUATION_Q14E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_Q14E_AVERAGE_TIME));
        if (!env.containsKey(SNBConstants.EVALUATION_S1E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_S1E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_S2E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_S2E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_S3E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_S3E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_S4E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_S4E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_S5E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_S5E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_S6E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_S6E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_S7E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_S7E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        EVALUATION_S1E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_S1E_AVERAGE_TIME));
        EVALUATION_S2E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_S2E_AVERAGE_TIME));
        EVALUATION_S3E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_S3E_AVERAGE_TIME));
        EVALUATION_S4E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_S4E_AVERAGE_TIME));
        EVALUATION_S5E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_S5E_AVERAGE_TIME));
        EVALUATION_S6E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_S6E_AVERAGE_TIME));
        EVALUATION_S7E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_S7E_AVERAGE_TIME));
        
        if (!env.containsKey(SNBConstants.EVALUATION_U1E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U1E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_U2E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U2E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_U3E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U3E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_U4E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U4E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_U5E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U5E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_U6E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U6E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_U7E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U7E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        if (!env.containsKey(SNBConstants.EVALUATION_U8E_AVERAGE_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_U8E_AVERAGE_TIME + "\" from the environment. Aborting.");
        }
        EVALUATION_U1E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U1E_AVERAGE_TIME));
        EVALUATION_U2E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U2E_AVERAGE_TIME));
        EVALUATION_U3E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U3E_AVERAGE_TIME));
        EVALUATION_U4E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U4E_AVERAGE_TIME));
        EVALUATION_U5E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U5E_AVERAGE_TIME));
        EVALUATION_U6E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U6E_AVERAGE_TIME));
        EVALUATION_U7E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U7E_AVERAGE_TIME));
        EVALUATION_U8E_AVERAGE_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_U8E_AVERAGE_TIME));
        
        /* loading time */
        if (!env.containsKey(SNBConstants.EVALUATION_LOADING_TIME)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_LOADING_TIME + "\" from the environment. Aborting.");
        }
        EVALUATION_LOADING_TIME = finalModel.createProperty(env.get(SNBConstants.EVALUATION_LOADING_TIME));
        
        /* throughput */
        if (!env.containsKey(SNBConstants.EVALUATION_THROUGHPUT)) {
            throw new IllegalArgumentException("Couldn't get \"" + SNBConstants.EVALUATION_THROUGHPUT + "\" from the environment. Aborting.");
        }
        EVALUATION_THROUGHPUT = finalModel.createProperty(env.get(SNBConstants.EVALUATION_THROUGHPUT));
	}

	@Override
	protected void evaluateResponse(byte[] expectedData, byte[] receivedData, long taskSentTimestamp,
			long responseReceivedTimestamp) throws Exception {
		String eStr = RabbitMQUtils.readString(expectedData);
    	String rStr = RabbitMQUtils.readString(receivedData);
    	String [] lines = eStr.split("\n");
    	if (eStr.equals("LOADING STARTED")) {
    		executionTimes.put("LoadingTime", new ArrayList<Long>());
    		executionTimes.get("LoadingTime").add(responseReceivedTimestamp - taskSentTimestamp);
    		return;
    	}
        //String taskId = lines[0];
        String type = lines[0].replaceAll("[{].*", "");
        String eAnswers = eStr.replaceFirst(".*\n", "");
        		        
        if (!eAnswers.trim().equals(rStr.trim())) {
        	wrongAnswers.add(lines[0] + " : " + eAnswers.length() + " - " + rStr.length());
        	wrongAnswers.add(eAnswers.trim());
        	wrongAnswers.add(rStr.trim());
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
		//TODO: remove this sleeping
//		try {
//			TimeUnit.SECONDS.sleep(30);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
        if (experimentUri == null)
            experimentUri = System.getenv().get(Constants.HOBBIT_EXPERIMENT_URI_KEY);
		
		Resource experiment = finalModel.createResource(experimentUri);
		finalModel.add(experiment, RDF.type, HOBBIT.Experiment);
		
		long totalMS = 0;
		long totalQueries = 0;
		for (Map.Entry<String, ArrayList<Long>> entry : executionTimes.entrySet()) {
    		long totalMSPerQueryType = 0;
    		for (long l : entry.getValue()) {
				totalMSPerQueryType += l;
			}
    		numberOfQueriesPerQueryType.put(entry.getKey(), entry.getValue().size());
    		totalTimePerQueryType.put(entry.getKey(), totalMSPerQueryType);
    		
    		totalMS += totalMSPerQueryType;
    		totalQueries += entry.getValue().size();
    		    		    		
    		LOGGER.info(entry.getKey() + "-" + ((double)totalMSPerQueryType)/ entry.getValue().size());
		}
		
		Literal qeAverageTimeLiteral = finalModel.createTypedLiteral((double)totalMS / totalQueries, XSDDatatype.XSDdouble);
		finalModel.add(experiment, EVALUATION_QE_AVERAGE_TIME, qeAverageTimeLiteral);
		
		Literal q01eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery1")/numberOfQueriesPerQueryType.get("LdbcQuery1"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery1") > 0)
			finalModel.add(experiment, EVALUATION_Q01E_AVERAGE_TIME, q01eAverageTimeLiteral);
		Literal q02eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery2")/numberOfQueriesPerQueryType.get("LdbcQuery2"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery2") > 0)
			finalModel.add(experiment, EVALUATION_Q02E_AVERAGE_TIME, q02eAverageTimeLiteral);
		Literal q03eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery3")/numberOfQueriesPerQueryType.get("LdbcQuery3"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery3") > 0)
			finalModel.add(experiment, EVALUATION_Q03E_AVERAGE_TIME, q03eAverageTimeLiteral);
		Literal q04eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery4")/numberOfQueriesPerQueryType.get("LdbcQuery4"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery4") > 0)
			finalModel.add(experiment, EVALUATION_Q04E_AVERAGE_TIME, q04eAverageTimeLiteral);
		Literal q05eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery5")/numberOfQueriesPerQueryType.get("LdbcQuery5"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery5") > 0)
			finalModel.add(experiment, EVALUATION_Q05E_AVERAGE_TIME, q05eAverageTimeLiteral);
		Literal q06eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery6")/numberOfQueriesPerQueryType.get("LdbcQuery6"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery6") > 0)
			finalModel.add(experiment, EVALUATION_Q06E_AVERAGE_TIME, q06eAverageTimeLiteral);
		Literal q07eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery7")/numberOfQueriesPerQueryType.get("LdbcQuery7"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery7") > 0)
			finalModel.add(experiment, EVALUATION_Q07E_AVERAGE_TIME, q07eAverageTimeLiteral);
		Literal q08eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery8")/numberOfQueriesPerQueryType.get("LdbcQuery8"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery8") > 0)
			finalModel.add(experiment, EVALUATION_Q08E_AVERAGE_TIME, q08eAverageTimeLiteral);
		Literal q09eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery9")/numberOfQueriesPerQueryType.get("LdbcQuery9"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery9") > 0)
			finalModel.add(experiment, EVALUATION_Q09E_AVERAGE_TIME, q09eAverageTimeLiteral);
		Literal q10eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery10")/numberOfQueriesPerQueryType.get("LdbcQuery10"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery10") > 0)
			finalModel.add(experiment, EVALUATION_Q10E_AVERAGE_TIME, q10eAverageTimeLiteral);
		Literal q11eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery11")/numberOfQueriesPerQueryType.get("LdbcQuery11"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery11") > 0)
			finalModel.add(experiment, EVALUATION_Q11E_AVERAGE_TIME, q11eAverageTimeLiteral);
		Literal q12eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery12")/numberOfQueriesPerQueryType.get("LdbcQuery12"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery12") > 0)
			finalModel.add(experiment, EVALUATION_Q12E_AVERAGE_TIME, q12eAverageTimeLiteral);
		Literal q13eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery13")/numberOfQueriesPerQueryType.get("LdbcQuery13"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery13") > 0)
			finalModel.add(experiment, EVALUATION_Q13E_AVERAGE_TIME, q13eAverageTimeLiteral);
		Literal q14eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcQuery14")/numberOfQueriesPerQueryType.get("LdbcQuery14"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcQuery14") > 0)
			finalModel.add(experiment, EVALUATION_Q14E_AVERAGE_TIME, q14eAverageTimeLiteral);
		Literal s1eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcShortQuery1PersonProfile")/numberOfQueriesPerQueryType.get("LdbcShortQuery1PersonProfile"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcShortQuery1PersonProfile") > 0)
			finalModel.add(experiment, EVALUATION_S1E_AVERAGE_TIME, s1eAverageTimeLiteral);
		Literal s2eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcShortQuery2PersonPosts")/numberOfQueriesPerQueryType.get("LdbcShortQuery2PersonPosts"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcShortQuery2PersonPosts") > 0)
			finalModel.add(experiment, EVALUATION_S2E_AVERAGE_TIME, s2eAverageTimeLiteral);
		Literal s3eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcShortQuery3PersonFriends")/numberOfQueriesPerQueryType.get("LdbcShortQuery3PersonFriends"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcShortQuery3PersonFriends") > 0)
			finalModel.add(experiment, EVALUATION_S3E_AVERAGE_TIME, s3eAverageTimeLiteral);
		Literal s4eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcShortQuery4MessageContent")/numberOfQueriesPerQueryType.get("LdbcShortQuery4MessageContent"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcShortQuery4MessageContent") > 0)
			finalModel.add(experiment, EVALUATION_S4E_AVERAGE_TIME, s4eAverageTimeLiteral);
		Literal s5eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcShortQuery5MessageCreator")/numberOfQueriesPerQueryType.get("LdbcShortQuery5MessageCreator"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcShortQuery5MessageCreator") > 0)
			finalModel.add(experiment, EVALUATION_S5E_AVERAGE_TIME, s5eAverageTimeLiteral);
		Literal s6eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcShortQuery6MessageForum")/numberOfQueriesPerQueryType.get("LdbcShortQuery6MessageForum"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcShortQuery6MessageForum") > 0)
			finalModel.add(experiment, EVALUATION_S6E_AVERAGE_TIME, s6eAverageTimeLiteral);
		Literal s7eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcShortQuery7MessageReplies")/numberOfQueriesPerQueryType.get("LdbcShortQuery7MessageReplies"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcShortQuery7MessageReplies") > 0)
			finalModel.add(experiment, EVALUATION_S7E_AVERAGE_TIME, s7eAverageTimeLiteral);
		Literal u1eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate1AddPerson")/numberOfQueriesPerQueryType.get("LdbcUpdate1AddPerson"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate1AddPerson") > 0)
			finalModel.add(experiment, EVALUATION_U1E_AVERAGE_TIME, u1eAverageTimeLiteral);
		Literal u2eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate2AddPostLike")/numberOfQueriesPerQueryType.get("LdbcUpdate2AddPostLike"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate2AddPostLike") > 0)
			finalModel.add(experiment, EVALUATION_U2E_AVERAGE_TIME, u2eAverageTimeLiteral);
		Literal u3eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate3AddCommentLike")/numberOfQueriesPerQueryType.get("LdbcUpdate3AddCommentLike"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate3AddCommentLike") > 0)
			finalModel.add(experiment, EVALUATION_U3E_AVERAGE_TIME, u3eAverageTimeLiteral);
		Literal u4eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate4AddForum")/numberOfQueriesPerQueryType.get("LdbcUpdate4AddForum"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate4AddForum") > 0)
			finalModel.add(experiment, EVALUATION_U4E_AVERAGE_TIME, u4eAverageTimeLiteral);
		Literal u5eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate5AddForumMembership")/numberOfQueriesPerQueryType.get("LdbcUpdate5AddForumMembership"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate5AddForumMembership") > 0)
			finalModel.add(experiment, EVALUATION_U5E_AVERAGE_TIME, u5eAverageTimeLiteral);
		Literal u6eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate6AddPost")/numberOfQueriesPerQueryType.get("LdbcUpdate6AddPost"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate6AddPost") > 0)
			finalModel.add(experiment, EVALUATION_U6E_AVERAGE_TIME, u6eAverageTimeLiteral);
		Literal u7eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate7AddComment")/numberOfQueriesPerQueryType.get("LdbcUpdate7AddComment"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate7AddComment") > 0)
			finalModel.add(experiment, EVALUATION_U7E_AVERAGE_TIME, u7eAverageTimeLiteral);
		Literal u8eAverageTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LdbcUpdate8AddFriendship")/numberOfQueriesPerQueryType.get("LdbcUpdate8AddFriendship"), XSDDatatype.XSDdouble);
		if (numberOfQueriesPerQueryType.get("LdbcUpdate8AddFriendship") > 0)
			finalModel.add(experiment, EVALUATION_U8E_AVERAGE_TIME, u8eAverageTimeLiteral);
		
		Literal loadingTimeLiteral = finalModel.createTypedLiteral(
				(double)totalTimePerQueryType.get("LoadingTime")/numberOfQueriesPerQueryType.get("LoadingTime"), XSDDatatype.XSDdouble);
		finalModel.add(experiment, EVALUATION_LOADING_TIME, loadingTimeLiteral);
		
		Literal throughputLiteral = finalModel.createTypedLiteral((double)totalQueries * 1000 / totalMS, XSDDatatype.XSDdouble);
		finalModel.add(experiment, EVALUATION_THROUGHPUT, throughputLiteral);

		// Log the wrong answers
    	for (int i = 0; i < wrongAnswers.size(); i+=3) {
    		LOGGER.info("Wrong answer on query:");
    		LOGGER.info(wrongAnswers.get(i));
    		LOGGER.info("Expected:" + "(" + wrongAnswers.get(i+1) + ")");
    		LOGGER.info(wrongAnswers.get(i+1));
    		LOGGER.info("Actual:" + "(" + wrongAnswers.get(i+2) + ")");
    		LOGGER.info(wrongAnswers.get(i+2));
		}
    	
    	LOGGER.info(finalModel.toString());
        return finalModel;
	}
	
    @Override
	public void close() throws IOException {
		// Free the resources you requested here
    	LOGGER.info("End of evaluation.");
		
        // Always close the super class after yours!
        super.close();
    }

}
