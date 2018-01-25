#!/bin/bash

mvn package

#docker build -f docker/virtuososystem.docker -t git.project-hobbit.eu:4567/mspasic/virtuososystem .
docker build -f docker/sparql-snbbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/mspasic/dsb-benchmarkcontroller .
docker build -f docker/sparql-snbdatagenerator.docker -t git.project-hobbit.eu:4567/mspasic/dsb-datagenerator .
docker build -f docker/sparql-snbtaskgenerator.docker -t git.project-hobbit.eu:4567/mspasic/dsb-taskgenerator .
docker build -f docker/sparql-snbseqtaskgenerator.docker -t git.project-hobbit.eu:4567/mspasic/dsb-seqtaskgenerator .
docker build -f docker/sparql-snbevaluationmodule.docker -t git.project-hobbit.eu:4567/mspasic/dsb-evaluationmodule .

#docker push git.project-hobbit.eu:4567/mspasic/virtuososystem
docker push git.project-hobbit.eu:4567/mspasic/dsb-benchmarkcontroller
docker push git.project-hobbit.eu:4567/mspasic/dsb-datagenerator
docker push git.project-hobbit.eu:4567/mspasic/dsb-taskgenerator
docker push git.project-hobbit.eu:4567/mspasic/dsb-seqtaskgenerator
docker push git.project-hobbit.eu:4567/mspasic/dsb-evaluationmodule
