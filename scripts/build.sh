#! /bin/bash

set -e
set -u
set -o pipefail

# Set Maven log level
MAVEN_OPTS="-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
# Build Java App
mvn clean deploy --no-transfer-progress

# Push container to ECR
REPOSITORY_URI=""
IMAGE_TAG="s3-parallel-move-testing"

$(aws ecr get-login --region "" --no-include-email)
docker build -t $REPOSITORY_URI:latest .
docker tag $REPOSITORY_URI:latest $REPOSITORY_URI:$IMAGE_TAG
docker push $REPOSITORY_URI:latest
docker push $REPOSITORY_URI:$IMAGE_TAG