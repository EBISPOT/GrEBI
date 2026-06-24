# syntax=docker/dockerfile:1

###############################################################################
# Stage 1 — Build the fat jar
###############################################################################
FROM maven:3.9.6-amazoncorretto-17 AS builder
WORKDIR /opt
COPY . .
RUN mvn clean package assembly:single -DskipTests

###############################################################################
# Stage 2 — Slim JRE runtime (no Maven/build toolchain), non-root
###############################################################################
FROM eclipse-temurin:17-jre
RUN groupadd -r grebi && useradd -r -g grebi -d /opt grebi
COPY --from=builder /opt/target/grebi-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/grebi.jar
USER grebi
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/opt/grebi.jar"]
