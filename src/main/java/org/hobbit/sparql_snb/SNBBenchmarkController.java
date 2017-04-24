package org.hobbit.sparql_snb;

import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNBBenchmarkController extends AbstractBenchmarkController {

	private static final Logger LOGGER = LoggerFactory.getLogger(SNBBenchmarkController.class);
	private String[] envVariablesEvaluationModule = null;

	// TODO: Add image names of containers
	/* Data generator Docker image */
	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "";
	/* Task generator Docker image */
	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "";
	/* Evaluation module Docker image */
	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "";

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


		// Create data generators
		int numberOfDataGenerators = 1;
		String[] envVariables = new String[]{"key1=value1"};
		createDataGenerators(DATA_GENERATOR_CONTAINER_IMAGE, numberOfDataGenerators, envVariables);

		// Create task generators
		int numberOfTaskGenerators = 1;
		envVariables = new String[]{"key1=value1"};
		createTaskGenerators(TASK_GENERATOR_CONTAINER_IMAGE, numberOfTaskGenerators, envVariables);

		// Create evaluation storage
		createEvaluationStorage();
		// TODO: get KPIs for evaluation module
		this.envVariablesEvaluationModule = new String[] {
				//	                OdinConstants.EVALUATION_AVERAGE_TASK_DELAY + "=" + "http://w3id.org/bench#averageTaskDelay",
				//	                OdinConstants.EVALUATION_MICRO_AVERAGE_RECALL + "=" + "http://w3id.org/bench#microAverageRecall",
				//	                OdinConstants.EVALUATION_MICRO_AVERAGE_PRECISION + "=" + "http://w3id.org/bench#microAveragePrecision",
				//	                OdinConstants.EVALUATION_MICRO_AVERAGE_FMEASURE + "=" + "http://w3id.org/bench#microAverageFmeasure",
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

		createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE, this.envVariablesEvaluationModule);
		// wait for the evaluation to finish
		waitForEvalComponentsToFinish();

		sendResultModel(this.resultModel);

		LOGGER.info("Executing benchmark is over.");

	}

}
