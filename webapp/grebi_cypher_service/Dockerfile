FROM maven:3.9.6-amazoncorretto-21

RUN mkdir -p /opt
COPY . /opt/
RUN cd /opt/ && mvn clean package -DskipTests

EXPOSE 8085
ENV GREBI_CYPHER_PORT=8085

ENTRYPOINT ["java", "--add-modules", "jdk.incubator.vector", "-jar", "/opt/target/grebi_cypher_service-1.0-SNAPSHOT.jar"]
