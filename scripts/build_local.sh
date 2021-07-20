#! /bin/bash

set -e
set -u
set -o pipefail

# Set Maven log level
MAVEN_OPTS="-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"

# Build Java App
mvn clean install 

# Push container to local docker
docker build . -f Dockerfile --tag s3parallelmove
docker run --publish 8000:8080 --detach --name S3ParallelMove s3parallelmove