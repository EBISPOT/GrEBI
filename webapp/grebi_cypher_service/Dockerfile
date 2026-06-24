# syntax=docker/dockerfile:1

###############################################################################
# Stage 1 — Build the shaded jar
###############################################################################
FROM maven:3.9.6-amazoncorretto-21 AS builder
WORKDIR /opt
COPY . .
RUN mvn clean package -DskipTests

###############################################################################
# Stage 2 — JRE runtime on Red Hat UBI9 (ships as the image's non-root user)
# ubi9/openjdk-21-runtime includes jdk.incubator.vector, required by embedded Neo4j.
###############################################################################
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
COPY --from=builder /opt/target/grebi_cypher_service-1.0-SNAPSHOT.jar /opt/grebi_cypher_service.jar
# Non-root (185 = the image's built-in 'default' user); explicit so scanners see it.
# In k8s this is overridden by runAsUser 5533 for NFS access.
USER 185
EXPOSE 8085
ENV GREBI_CYPHER_PORT=8085
ENTRYPOINT ["java", "--add-modules", "jdk.incubator.vector", "-jar", "/opt/grebi_cypher_service.jar"]
