FROM openjdk:8-jre-alpine

ADD target/S3ParallelMove-1.0-SNAPSHOT.jar /S3ParallelMove.jar

ENTRYPOINT ["java", "-jar", "/S3ParallelMove.jar"]

EXPOSE 8080