package org.hobbit.sparql_snb;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.rdf.model.NodeIterator;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.hobbit.sparql_snb.util.SNBConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBBenchmarkController extends AbstractBenchmarkController {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBBenchmarkController.class);
	private String[] envVariablesEvaluationModule = null;
	private int numberOfOperations = -1;
	private int scaleFactor = -1;

	// TODO: Add image names of containers
	/* Data generator Docker image */
	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/mspasic/sparql-snbdatagenerator";
	/* Task generator Docker image */
	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/mspasic/sparql-snbtaskgenerator";
	/* Evaluation module Docker image */
	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/mspasic/sparql-snbevaluationmodule";

	public SNBBenchmarkController() {

	}

	@Override
	public void init() throws Exception {
		LOGGER.info("Initialization begins.");
		super.init();

		// Your initialization code comes here...

		// You might want to load parameters from the benchmarks parameter model
		//	        NodeIterator iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel
		//	                    .getProperty("http://example.org/myParameter"));

		NodeIterator iterator;
		
        /* Number of operations */
        if (numberOfOperations == -1) {

            iterator = benchmarkParamModel.listObjectsOfProperty(
                    benchmarkParamModel.getProperty("http://w3id.org/bench#numberOfOperations"));
            if (iterator.hasNext()) {
                try {
                    numberOfOperations = iterator.next().asLiteral().getInt();
                } catch (Exception e) {
                    LOGGER.error("Exception while parsing parameter.", e);
                }
            }
            if (numberOfOperations < 0) {
                LOGGER.error("Couldn't get the number of operations from the parameter model. Using the default value.");
                numberOfOperations = 10000;
            }
        }
        
        /* Scale Factor */
        if (scaleFactor == -1) {

            iterator = benchmarkParamModel.listObjectsOfProperty(
                    benchmarkParamModel.getProperty("http://w3id.org/bench#hasSF"));
            if (iterator.hasNext()) {
                try {
                    scaleFactor = iterator.next().asLiteral().getInt();
                } catch (Exception e) {
                    LOGGER.error("Exception while parsing parameter.", e);
                }
            }
            //TODO: Add different scale factors
            if (scaleFactor != 1) {
                LOGGER.error("Scale factor can be 1, 3 or 10. Using the default value.");
                scaleFactor = 1;
            }
        }

		// Create data generators
		int numberOfDataGenerators = 1;
		String[] envVariables = new String[]{SNBConstants.GENERATOR_SCALE_FACTOR + "=" + scaleFactor};
		createDataGenerators(DATA_GENERATOR_CONTAINER_IMAGE, numberOfDataGenerators, envVariables);

		// Create task generators
		int numberOfTaskGenerators = 1;
		envVariables = new String[]{SNBConstants.GENERATOR_NUMBER_OF_OPERATIONS + "=" + numberOfOperations};
		createTaskGenerators(TASK_GENERATOR_CONTAINER_IMAGE, numberOfTaskGenerators, envVariables);

		// Create evaluation storage
		envVariables = ArrayUtils.add(DEFAULT_EVAL_STORAGE_PARAMETERS,
                Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + this.rabbitMQHostName);
		envVariables = ArrayUtils.add(envVariables, "ACKNOWLEDGEMENT_FLAG=true");
		createEvaluationStorage(DEFAULT_EVAL_STORAGE_IMAGE, envVariables);
		// TODO: get KPIs for evaluation module
		this.envVariablesEvaluationModule = new String[] {
				SNBConstants.EVALUATION_QE_AVERAGE_TIME + "=" + "http://w3id.org/bench#QEAverageTime",
				SNBConstants.EVALUATION_Q01E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q01EAverageTime",
				SNBConstants.EVALUATION_Q02E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q02EAverageTime",
				SNBConstants.EVALUATION_Q03E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q03EAverageTime",
				SNBConstants.EVALUATION_Q04E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q04EAverageTime",
				SNBConstants.EVALUATION_Q05E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q05EAverageTime",
				SNBConstants.EVALUATION_Q06E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q06EAverageTime",
				SNBConstants.EVALUATION_Q07E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q07EAverageTime",
				SNBConstants.EVALUATION_Q08E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q08EAverageTime",
				SNBConstants.EVALUATION_Q09E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q09EAverageTime",
				SNBConstants.EVALUATION_Q10E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q10EAverageTime",
				SNBConstants.EVALUATION_Q11E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q11EAverageTime",
				SNBConstants.EVALUATION_Q12E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q12EAverageTime",
				SNBConstants.EVALUATION_Q13E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q13EAverageTime",
				SNBConstants.EVALUATION_Q14E_AVERAGE_TIME + "=" + "http://w3id.org/bench#Q14EAverageTime",
				SNBConstants.EVALUATION_S1E_AVERAGE_TIME + "=" + "http://w3id.org/bench#S1EAverageTime",
				SNBConstants.EVALUATION_S2E_AVERAGE_TIME + "=" + "http://w3id.org/bench#S2EAverageTime",
				SNBConstants.EVALUATION_S3E_AVERAGE_TIME + "=" + "http://w3id.org/bench#S3EAverageTime",
				SNBConstants.EVALUATION_S4E_AVERAGE_TIME + "=" + "http://w3id.org/bench#S4EAverageTime",
				SNBConstants.EVALUATION_S5E_AVERAGE_TIME + "=" + "http://w3id.org/bench#S5EAverageTime",
				SNBConstants.EVALUATION_S6E_AVERAGE_TIME + "=" + "http://w3id.org/bench#S6EAverageTime",
				SNBConstants.EVALUATION_S7E_AVERAGE_TIME + "=" + "http://w3id.org/bench#S7EAverageTime",
				SNBConstants.EVALUATION_THROUGHPUT + "=" + "http://w3id.org/bench#throughput"
		};

		// Wait for all components to finish their initialization
		waitForComponentsToInitialize();

		LOGGER.info("Initialization is over.");
	}

	@Override
	protected void executeBenchmark() throws Exception {
		LOGGER.info("Executing benchmark has started.");

		// give the start signals
		LOGGER.info("Send start signal to Data and Task Generators.");
		sendToCmdQueue(Commands.TASK_GENERATOR_START_SIGNAL);
		sendToCmdQueue(Commands.DATA_GENERATOR_START_SIGNAL);

		// wait for the data generators to finish their work
		waitForDataGenToFinish();
		// wait for the task generators to finish their work
		waitForTaskGenToFinish();
		// wait for the system to terminate
		waitForSystemToFinish();

		LOGGER.info("Evaluation in progress...");
		createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE, this.envVariablesEvaluationModule);
		LOGGER.info("Waiting for the evaluation to finish...");
		// wait for the evaluation to finish
		waitForEvalComponentsToFinish();

		sendResultModel(this.resultModel);

		LOGGER.info("Executing benchmark is over.");

	}

}
