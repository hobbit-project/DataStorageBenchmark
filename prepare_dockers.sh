#!/bin/bash

mvn package

#docker build -f docker/sparql-snbvirtuososystem.docker -t git.project-hobbit.eu:4567/mspasic/sparql-snbvirtuososystem .
docker build -f docker/virtuososystem.docker -t git.project-hobbit.eu:4567/mspasic/virtuososystem .
#docker build -f docker/virtuosocomsystem72.docker -t git.project-hobbit.eu:4567/mspasic/virtuosocomsystem72 .
#docker build -f docker/virtuosocomsystem80.docker -t git.project-hobbit.eu:4567/mspasic/virtuosocomsystem80 .
#docker build -f docker/virtuosocomsystem81.docker -t git.project-hobbit.eu:4567/mspasic/virtuosocomsystem81 .
#docker build -f docker/virtuosocomsystem82.docker -t git.project-hobbit.eu:4567/mspasic/virtuosocomsystem82 .
docker build -f docker/sparql-snbbenchmarkcontroller.docker -t git.project-hobbit.eu:4567/mspasic/sparql-snbbenchmarkcontroller .
docker build -f docker/sparql-snbdatagenerator.docker -t git.project-hobbit.eu:4567/mspasic/sparql-snbdatagenerator .
docker build -f docker/sparql-snbtaskgenerator.docker -t git.project-hobbit.eu:4567/mspasic/sparql-snbtaskgenerator .
docker build -f docker/sparql-snbevaluationmodule.docker -t git.project-hobbit.eu:4567/mspasic/sparql-snbevaluationmodule .

#docker push git.project-hobbit.eu:4567/mspasic/sparql-snbvirtuososystem
docker push git.project-hobbit.eu:4567/mspasic/virtuososystem
#docker push git.project-hobbit.eu:4567/mspasic/virtuosocomsystem72
#docker push git.project-hobbit.eu:4567/mspasic/virtuosocomsystem80
#docker push git.project-hobbit.eu:4567/mspasic/virtuosocomsystem81
#docker push git.project-hobbit.eu:4567/mspasic/virtuosocomsystem82
docker push git.project-hobbit.eu:4567/mspasic/sparql-snbbenchmarkcontroller
docker push git.project-hobbit.eu:4567/mspasic/sparql-snbdatagenerator
docker push git.project-hobbit.eu:4567/mspasic/sparql-snbtaskgenerator
docker push git.project-hobbit.eu:4567/mspasic/sparql-snbevaluationmodule
