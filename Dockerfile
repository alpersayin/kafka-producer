FROM openjdk:8-jdk-alpine
EXPOSE 8082
ARG JAR_FILE=target/kafka-0.0.1-SNAPSHOT.jar
WORKDIR /opt/app
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java", "-jar","app.jar"]
