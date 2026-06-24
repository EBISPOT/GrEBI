# syntax=docker/dockerfile:1
#
# Build context is the repository root (see .github/workflows/docker.yml) so we
# can build both the Java fat jar and the native grebi_reprefix helper that the
# backend spawns over stdio.

###############################################################################
# Stage 1 — Build the fat jar
###############################################################################
FROM maven:3.9.6-amazoncorretto-17 AS builder
WORKDIR /opt
COPY webapp/grebi_api/ .
RUN mvn clean package assembly:single -DskipTests

###############################################################################
# Stage 2 — Build the native grebi_reprefix helper
###############################################################################
FROM rust:1.90.0-bullseye AS rust-builder
WORKDIR /app
COPY dataload/grebi_shared ./dataload/grebi_shared
COPY webapp/grebi_reprefix ./webapp/grebi_reprefix
WORKDIR /app/webapp/grebi_reprefix
RUN cargo build --release && \
    cp target/release/grebi_reprefix /usr/local/bin/grebi_reprefix

###############################################################################
# Stage 3 — Slim JRE runtime (no Maven/build toolchain), non-root
###############################################################################
FROM eclipse-temurin:17-jre
RUN groupadd -r grebi && useradd -r -g grebi -d /opt grebi
COPY --from=builder /opt/target/grebi-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/grebi.jar
COPY --from=rust-builder /usr/local/bin/grebi_reprefix /usr/local/bin/grebi_reprefix
USER grebi
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/opt/grebi.jar"]
