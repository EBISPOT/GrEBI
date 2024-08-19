
FROM maven:3.9-eclipse-temurin-11

RUN mkdir -p /opt
COPY . /opt/
RUN cd /opt/ && ls && mvn clean package assembly:single -DskipTests

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/opt/target/grebi_resolver_service-1.0-SNAPSHOT-jar-with-dependencies.jar"]


