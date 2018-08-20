package org.hobbit.sparql_snb;

import org.hobbit.core.components.Component;
import org.hobbit.sdk.EnvironmentVariablesWrapper;
import org.hobbit.sdk.JenaKeyValue;
import org.hobbit.sdk.docker.MultiThreadedImageBuilder;
import org.hobbit.sdk.docker.RabbitMqDockerizer;
import org.hobbit.sdk.docker.builders.hobbit.*;

import org.hobbit.sdk.examples.dummybenchmark.DummyEvalStorage;
import org.hobbit.sdk.utils.CommandQueueListener;
import org.hobbit.sdk.utils.ComponentsExecutor;

import org.hobbit.sdk.utils.commandreactions.CommandReactionsBuilder;
import org.hobbit.sparql_snb.systems.VirtuosoSystemAdapter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.StringReader;
import java.util.Date;

import static org.hobbit.core.Constants.*;
import static org.hobbit.sdk.CommonConstants.*;
import static org.hobbit.sparql_snb.SNBBenchmarkController.*;

/**
 * @author Pavel Smirnov
 */

public class BenchmarkTest extends EnvironmentVariablesWrapper {

    private RabbitMqDockerizer rabbitMqDockerizer;
    private ComponentsExecutor componentsExecutor;
    private CommandQueueListener commandQueueListener;

    BenchmarkDockerBuilder benchmarkBuilder;
    DataGenDockerBuilder dataGeneratorBuilder;
    TaskGenDockerBuilder taskGeneratorBuilder;
    EvalStorageDockerBuilder evalStorageBuilder;
    SystemAdapterDockerBuilder systemAdapterBuilder;
    EvalModuleDockerBuilder evalModuleBuilder;



    public void init(Boolean useCachedImage) throws Exception {

        benchmarkBuilder = new BenchmarkDockerBuilder(new CommonDockersBuilder(SNBBenchmarkController.class, "git.project-hobbit.eu:4567/mspasic/benchmark-controller").customDockerFileReader(new StringReader("docker/sparql-snbbenchmarkcontroller.docker")).useCachedImage(useCachedImage));
        dataGeneratorBuilder = new DataGenDockerBuilder(new CommonDockersBuilder(SNBDataGenerator.class, DATA_GENERATOR_CONTAINER_IMAGE).useCachedImage(useCachedImage).customDockerFileReader(new StringReader("docker/sparql-snbdatagenerator.docker")));
        taskGeneratorBuilder = new TaskGenDockerBuilder(new CommonDockersBuilder(SNBTaskGenerator.class, TASK_GENERATOR_CONTAINER_IMAGE).useCachedImage(useCachedImage).customDockerFileReader(new StringReader("docker/sparql-snbtaskgenerator.docker")));

        evalStorageBuilder = new EvalStorageDockerBuilder(new CommonDockersBuilder(DummyEvalStorage.class, "git.project-hobbit.eu:4567/defaulthobbituser/defaultevaluationstorage:1.0.5").useCachedImage(useCachedImage));

        systemAdapterBuilder = new SystemAdapterDockerBuilder(new CommonDockersBuilder(VirtuosoSystemAdapter.class, "git.project-hobbit.eu:4567/mspasic/virtuososystem").useCachedImage(useCachedImage).customDockerFileReader(new StringReader("docker/virtuososystem.docker")));
        evalModuleBuilder = new EvalModuleDockerBuilder(new CommonDockersBuilder(SNBEvaluationModule.class, "git.project-hobbit.eu:4567/mspasic/evaluation-module").useCachedImage(useCachedImage).customDockerFileReader(new StringReader("docker/sparql-snbevaluationmodule.docker")));

    }


    @Test
    @Ignore
    public void buildImages() throws Exception {

        init(false);

        MultiThreadedImageBuilder builder = new MultiThreadedImageBuilder(5);
        builder.addTask(benchmarkBuilder);
        builder.addTask(dataGeneratorBuilder);
        builder.addTask(taskGeneratorBuilder);
        //builder.addTask(evalStorageBuilder);
        builder.addTask(systemAdapterBuilder);
        builder.build();
    }

    @Test
    public void checkHealth() throws Exception {
        checkHealth(false);
    }

    @Test
    @Ignore
    public void checkHealthDockerized() throws Exception {
        checkHealth(true);
    }

    private void checkHealth(Boolean dockerized) throws Exception {

        Boolean useCachedImages = true;
        init(useCachedImages);

        rabbitMqDockerizer = RabbitMqDockerizer.builder().build();

        environmentVariables.set(RABBIT_MQ_HOST_NAME_KEY, rabbitMqDockerizer.getHostName());
        environmentVariables.set(HOBBIT_SESSION_ID_KEY, "session_"+String.valueOf(new Date().getTime()));


        Component benchmarkController = new SNBBenchmarkController();
        Component dataGen = new SNBDataGenerator();
        Component taskGen = new SNBTaskGenerator();
        Component evalStorage = new DummyEvalStorage();

        Component systemAdapter = new VirtuosoSystemAdapter();
        Component evalModule = new SNBEvaluationModule();

        if(dockerized) {

            benchmarkController = benchmarkBuilder.build();
            dataGen = dataGeneratorBuilder.build();
            taskGen = taskGeneratorBuilder.build();
            evalStorage = evalStorageBuilder.build();
            evalModule = evalModuleBuilder.build();
            systemAdapter = systemAdapterBuilder.build();
        }

        commandQueueListener = new CommandQueueListener();
        componentsExecutor = new ComponentsExecutor();

        rabbitMqDockerizer.run();


        CommandReactionsBuilder commandReactionsBuilder = new CommandReactionsBuilder(componentsExecutor, commandQueueListener)
                .benchmarkController(benchmarkController).benchmarkControllerImageName(benchmarkBuilder.getImageName())
                .dataGenerator(dataGen).dataGeneratorImageName(dataGeneratorBuilder.getImageName())
                .taskGenerator(taskGen).taskGeneratorImageName(taskGeneratorBuilder.getImageName())
                .evalStorage(evalStorage).evalStorageImageName(evalStorageBuilder.getImageName())
                .systemAdapter(systemAdapter).systemAdapterImageName(systemAdapterBuilder.getImageName())
                .evalModule(evalModule).evalModuleImageName(evalModuleBuilder.getImageName());

        commandQueueListener.setCommandReactions(
                commandReactionsBuilder.buildStartCommandsReaction(),
                commandReactionsBuilder.buildTerminateCommandsReaction(),
                commandReactionsBuilder.buildPlatformCommandsReaction()
        );


        componentsExecutor.submit(commandQueueListener);
        commandQueueListener.waitForInitialisation();

        String benchmarkContainerId = "benchmark";
        benchmarkContainerId = commandQueueListener.createContainer(benchmarkBuilder.getImageName(), "benchmark", new String[]{ HOBBIT_EXPERIMENT_URI_KEY+"="+EXPERIMENT_URI,  BENCHMARK_PARAMETERS_MODEL_KEY+"="+ createBenchmarkParameters() });
        //componentsExecutor.submit(benchmarkController, benchmarkContainerId, new String[]{ HOBBIT_EXPERIMENT_URI_KEY+"="+EXPERIMENT_URI,  BENCHMARK_PARAMETERS_MODEL_KEY+"="+ createBenchmarkParameters() });


        String systemContainerId = commandQueueListener.createContainer(systemAdapterBuilder.getImageName(), "system" ,new String[]{ SYSTEM_PARAMETERS_MODEL_KEY+"="+ createSystemParameters() });

        environmentVariables.set("BENCHMARK_CONTAINER_ID", benchmarkContainerId);
        environmentVariables.set("SYSTEM_CONTAINER_ID", systemContainerId);

        //componentsExecutor.submit(systemAdapter, "system", new String[]{ SYSTEM_PARAMETERS_MODEL_KEY+"="+ createSystemParameters() });


        commandQueueListener.waitForTermination();

        rabbitMqDockerizer.stop();

        Assert.assertFalse(componentsExecutor.anyExceptions());
    }


    public String createBenchmarkParameters() {
        JenaKeyValue kv = new JenaKeyValue();

        return kv.encodeToString();
    }

    private static String createSystemParameters(){
        JenaKeyValue kv = new JenaKeyValue();
        //kv.setValue(BENCHMARK_MODE_INPUT_NAME, BENCHMARK_MODE_DYNAMIC+":10:1");
        return kv.encodeToString();
    }



}
