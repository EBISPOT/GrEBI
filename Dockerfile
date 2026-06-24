# syntax=docker/dockerfile:1

# Base image carrying the heavy runtime layers (built by base.yml, on demand).
# Override to build against a locally-built base, e.g.
#   docker build --build-arg BASE_IMAGE=grebi_base:local ...
ARG BASE_IMAGE=ghcr.io/ebispot/grebi_base:dev

###############################################################################
# Stage 1 — Cross-compile Rust binaries (runs natively on the builder arch)
#
# cargo-chef splits dependency compilation from the app build: the large, slow
# dependency layer (arrow/parquet et al.) is cooked separately and keyed on
# Cargo.lock + manifests, so the GHA layer cache reuses it across builds and it
# only recompiles when dependencies actually change — not on every source edit.
###############################################################################
FROM --platform=$BUILDPLATFORM rust:1.90.0-bullseye AS chef
ARG TARGETPLATFORM

# Retry transient apt mirror errors instead of aborting the build.
RUN printf 'Acquire::Retries "5";\n' > /etc/apt/apt.conf.d/99-grebi-retries

# cmake is needed by some crates; the arm64 cross toolchain lets the native
# (amd64) builder link aarch64 binaries.
RUN apt-get update -y && apt-get install -y cmake && \
    case "$TARGETPLATFORM" in \
      "linux/arm64") \
        apt-get install -y gcc-aarch64-linux-gnu && \
        rustup target add aarch64-unknown-linux-gnu ;; \
    esac && \
    rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc

# ---- Plan: derive the dependency recipe for the dataload workspace ----
FROM chef AS planner
COPY dataload /opt/grebi_dataload
WORKDIR /opt/grebi_dataload
RUN cargo chef prepare --recipe-path /recipe.json

# ---- Build: cook deps (cached layer), then compile the binaries ----
FROM chef AS rust-builder
ARG TARGETPLATFORM
WORKDIR /opt/grebi_dataload

# Cook ONLY the dependencies. This layer is keyed on recipe.json, so it is reused
# across builds until the dependency set changes — the expensive arrow/parquet
# compile no longer happens on every push.
COPY --from=planner /recipe.json recipe.json
RUN case "$TARGETPLATFORM" in \
      "linux/arm64") cargo chef cook --release --target aarch64-unknown-linux-gnu --recipe-path recipe.json ;; \
      *)             cargo chef cook --release --recipe-path recipe.json ;; \
    esac

# Compile the actual dataload binaries (dependencies already built above).
COPY dataload /opt/grebi_dataload
RUN case "$TARGETPLATFORM" in \
      "linux/arm64") \
        cargo build --release --target aarch64-unknown-linux-gnu && \
        cp target/aarch64-unknown-linux-gnu/release/grebi_* /usr/local/bin/ 2>/dev/null || true ;; \
      *) \
        cargo build --release && \
        cp target/release/grebi_* /usr/local/bin/ 2>/dev/null || true ;; \
    esac

# Build grebi_reprefix (tiny: serde_json + grebi_shared via ../../dataload/grebi_shared).
# The Java backend spawns this binary over stdio to normalise prefixes.
COPY dataload/grebi_shared /dataload/grebi_shared
COPY webapp/grebi_reprefix /webapp/grebi_reprefix
WORKDIR /webapp/grebi_reprefix
RUN case "$TARGETPLATFORM" in \
      "linux/arm64") \
        cargo build --release --target aarch64-unknown-linux-gnu && \
        cp target/aarch64-unknown-linux-gnu/release/grebi_reprefix /usr/local/bin/ ;; \
      *) \
        cargo build --release && \
        cp target/release/grebi_reprefix /usr/local/bin/ ;; \
    esac

###############################################################################
# Stage 2a/2b — Build the Java jars (deps cached via dependency:go-offline)
#
# The Maven analogue of cargo-chef: resolve dependencies in a layer keyed only
# on the pom, so the (large, esp. embedded-Neo4j) dependency download is cached
# by the GHA layer cache and only re-runs when the pom changes. Doing this in
# builder stages keeps the hundreds-of-MB .m2 out of the final runtime image.
###############################################################################
FROM ${BASE_IMAGE} AS api-builder
WORKDIR /opt/grebi_api
COPY webapp/grebi_api/pom.xml .
RUN mvn -B dependency:go-offline
COPY webapp/grebi_api .
RUN mvn -B clean package assembly:single -DskipTests

FROM ${BASE_IMAGE} AS cypher-builder
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
