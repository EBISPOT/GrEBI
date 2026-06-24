# syntax=docker/dockerfile:1

# Base image carrying the heavy runtime layers (built by base.yml, on demand).
# Override to build against a locally-built base, e.g.
#   docker build --build-arg BASE_IMAGE=grebi_base:local ...
ARG BASE_IMAGE=ghcr.io/ebispot/grebi_base:dev

###############################################################################
# Stage 1 — Compile the Rust binaries (natively; CI builds each arch on a
# native runner, so no cross-compilation is needed).
#
# cargo-chef splits dependency compilation from the app build: the large, slow
# dependency layer (arrow/parquet et al.) is cooked separately and keyed on
# Cargo.lock + manifests, so the GHA layer cache reuses it across builds and it
# only recompiles when dependencies actually change — not on every source edit.
###############################################################################
FROM rust:1.90.0-bullseye AS chef

# Retry transient apt mirror errors instead of aborting the build.
RUN printf 'Acquire::Retries "5";\n' > /etc/apt/apt.conf.d/99-grebi-retries

# cmake is needed by some crates.
RUN apt-get update -y && apt-get install -y cmake && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked

# ---- Plan: derive the dependency recipe for the dataload workspace ----
FROM chef AS planner
COPY dataload /opt/grebi_dataload
WORKDIR /opt/grebi_dataload
RUN cargo chef prepare --recipe-path /recipe.json

# ---- Build: cook deps (cached layer), then compile the binaries ----
FROM chef AS rust-builder
WORKDIR /opt/grebi_dataload

# Cook ONLY the dependencies. This layer is keyed on recipe.json, so it is reused
# across builds until the dependency set changes — the expensive arrow/parquet
# compile no longer happens on every push.
COPY --from=planner /recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Compile the actual dataload binaries (dependencies already built above).
COPY dataload /opt/grebi_dataload
RUN cargo build --release && \
    cp target/release/grebi_* /usr/local/bin/ 2>/dev/null || true

# Build grebi_reprefix (tiny: serde_json + grebi_shared via ../../dataload/grebi_shared).
# The Java backend spawns this binary over stdio to normalise prefixes.
COPY dataload/grebi_shared /dataload/grebi_shared
COPY webapp/grebi_reprefix /webapp/grebi_reprefix
WORKDIR /webapp/grebi_reprefix
RUN cargo build --release && \
    cp target/release/grebi_reprefix /usr/local/bin/

###############################################################################
# Stage 2a/2b — Build the Java jars (deps cached via dependency:go-offline)
#
# The Maven analogue of cargo-chef: resolve dependencies in a layer keyed only
# on the pom, so the (large, esp. embedded-Neo4j) dependency download is cached
# by the GHA layer cache and only re-runs when the pom changes. Doing this in
# builder stages keeps the hundreds-of-MB .m2 out of the final runtime image.
###############################################################################
FROM ${BASE_IMAGE} AS api-builder
USER root
WORKDIR /opt/grebi_api
COPY webapp/grebi_api/pom.xml .
RUN mvn -B dependency:go-offline
COPY webapp/grebi_api .
RUN mvn -B clean package assembly:single -DskipTests

FROM ${BASE_IMAGE} AS cypher-builder
USER root
WORKDIR /opt/grebi_cypher_service
COPY webapp/grebi_cypher_service/pom.xml .
RUN mvn -B dependency:go-offline
COPY webapp/grebi_cypher_service .
RUN mvn -B clean package -DskipTests

###############################################################################
# Stage 2 — Runtime image (built FROM the on-demand grebi_base image)
#
# grebi_base carries the heavy, rarely-changing layers (Debian packages,
# PostgreSQL, Node/Caddy/Docker-CLI, Java, Maven, Neo4j, Nextflow). It is built
# separately by .github/workflows/base.yml and only rebuilt when Dockerfile.base
# changes — so day-to-day combined builds skip ~1 GB of downloads.
###############################################################################
FROM ${BASE_IMAGE}
# Reset to root for the build/COPY steps below (base ships as non-root).
USER root

# ---- Copy pre-built Rust binaries from cross-compile stage ----
# (grebi_reprefix is included in the grebi_* glob and is spawned by grebi_api)
COPY --from=rust-builder /usr/local/bin/grebi_* /usr/local/bin/
ENV PATH="$PATH:/usr/local/bin"

# Copy prefix maps
COPY dataload/prefix_maps /opt/grebi/data/prefix_maps

# Copy full dataload directory (scripts, prefix_maps, python utils needed at runtime by Nextflow processes)
COPY dataload /opt/grebi_dataload

# ---- Java service jars (built in the cached builder stages above) ----
COPY --from=api-builder /opt/grebi_api/target/grebi-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/grebi_api.jar
COPY --from=cypher-builder /opt/grebi_cypher_service/target/grebi_cypher_service-1.0-SNAPSHOT.jar /opt/grebi_cypher_service.jar

# ---- Build grebi_ui (Node) ----
COPY docs /opt/grebi_ui/docs
COPY webapp/grebi_ui /opt/grebi_ui
WORKDIR /opt/grebi_ui

# Create .env.ebi if missing (gitignored so not included in COPY)
RUN test -f .env.ebi || printf 'PUBLIC_URL=/\nGREBI_FRONTEND=ebi\n' > .env.ebi

RUN --mount=type=cache,target=/root/.npm \
    npm install && \
    mkdir -p dist && \
    chmod -R 777 dist

# ---- Copy remaining files ----
COPY query_templates /opt/query_templates
COPY webapp/combined_supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY webapp/test_query_templates.py /opt/test_query_templates.py
COPY webapp/test_queries_and_make_docs.py /opt/test_queries_and_make_docs.py
COPY webapp/generate_docs_pdf.mjs /opt/generate_docs_pdf.mjs
COPY webapp/api2code.mjs /opt/api2code.mjs
COPY webapp/query2code.mjs /opt/query2code.mjs
RUN --mount=type=cache,target=/root/.npm \
    cd /opt && npm install js-yaml marked
COPY docs /opt/docs
COPY tests/export_neo4j.py /opt/export_neo4j.py
COPY tests/export_postgres.py /opt/export_postgres.py
COPY tests/compare_snapshots.py /opt/compare_snapshots.py
COPY tests/test_api_snapshots.py /opt/test_api_snapshots.py
COPY webapp/combined_entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

WORKDIR /opt

# Ship as non-root. At runtime this is overridden by the injected uid
# (docker -u $HOST_UID via Nextflow; k8s runAsUser); the entrypoint resolves
# whatever uid it runs as against the world-writable /etc/passwd.
USER grebi
