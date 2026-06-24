# syntax=docker/dockerfile:1

###############################################################################
# Stage 1 — Build the shaded jar
###############################################################################
FROM maven:3.9.6-amazoncorretto-21 AS builder
WORKDIR /opt
COPY . .
RUN mvn clean package -DskipTests

###############################################################################
# Stage 2 — Slim JRE runtime (no Maven/build toolchain), non-root
# eclipse-temurin:21-jre includes jdk.incubator.vector, required by embedded Neo4j.
###############################################################################
FROM eclipse-temurin:21-jre
RUN groupadd -r grebi && useradd -r -g grebi -d /opt grebi
COPY --from=builder /opt/target/grebi_cypher_service-1.0-SNAPSHOT.jar /opt/grebi_cypher_service.jar
USER grebi
EXPOSE 8085
ENV GREBI_CYPHER_PORT=8085
ENTRYPOINT ["java", "--add-modules", "jdk.incubator.vector", "-jar", "/opt/grebi_cypher_service.jar"]
