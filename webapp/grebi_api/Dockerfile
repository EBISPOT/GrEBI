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
# Same OS family as the runtime stage below (UBI9, glibc 2.34), so the helper
# can never need a newer glibc than the runtime has. Toolchain via rustup, pinned.
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS rust-builder
ARG RUST_VERSION=1.90.0
RUN microdnf install -y --setopt=install_weak_deps=0 gcc make curl-minimal && microdnf clean all
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}" --no-modify-path
WORKDIR /app
COPY dataload/grebi_shared ./dataload/grebi_shared
COPY webapp/grebi_reprefix ./webapp/grebi_reprefix
WORKDIR /app/webapp/grebi_reprefix
RUN cargo build --release && \
    cp target/release/grebi_reprefix /usr/local/bin/grebi_reprefix

###############################################################################
# Stage 3 — JRE runtime on Red Hat UBI9 (ships as the image's non-root user)
###############################################################################
FROM registry.access.redhat.com/ubi9/openjdk-17-runtime:1.24
COPY --from=builder /opt/target/grebi-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/grebi.jar
COPY --from=rust-builder /usr/local/bin/grebi_reprefix /usr/local/bin/grebi_reprefix
# Non-root (185 = the image's built-in 'default' user); explicit so scanners see it.
USER 185
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/opt/grebi.jar"]
