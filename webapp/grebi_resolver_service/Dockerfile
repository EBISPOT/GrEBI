
FROM maven:3.9-eclipse-temurin-11

RUN apt-get update && apt-get install -y rsync

RUN mkdir -p /opt
COPY . /opt/
RUN cd /opt/ && ls && mvn clean package assembly:single -DskipTests

EXPOSE 8084
ENV GREBI_RESOLVER_PORT=8084
ENTRYPOINT ["java", "-jar", "/opt/target/grebi_resolver_service-1.0-SNAPSHOT-jar-with-dependencies.jar"]


