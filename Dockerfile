# syntax=docker/dockerfile:1

# Base image carrying the heavy runtime layers (built by base.yml, on demand).
# Override to build against a locally-built base, e.g.
#   docker build --build-arg BASE_IMAGE=grebi_base:local ...
ARG BASE_IMAGE=ghcr.io/ebispot/grebi_base:dev

###############################################################################
# Stage 1 — Compile the Rust binaries (natively; CI builds each arch on a
# native runner, so no cross-compilation is needed).
#
# The builder is the same OS as the runtime image (UBI9), so the binaries can
# never need a newer glibc than the runtime has: UBI9 keeps glibc 2.34 for its
# whole life, and it is supported until 2032. The Rust toolchain comes from
# rustup, pinned to RUST_VERSION; the C toolchain, cmake (zlib-ng's build
# script) and the dev packages of the shared libraries the binaries link
# (zlib, sqlite) come from the UBI repositories.
#
# cargo-chef splits dependency compilation from the app build: the large, slow
# dependency layer (arrow/parquet et al.) is cooked separately and keyed on
# Cargo.lock + manifests, so the GHA layer cache reuses it across builds and it
# only recompiles when dependencies actually change — not on every source edit.
###############################################################################
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS chef
ARG RUST_VERSION=1.90.0

RUN microdnf install -y --setopt=install_weak_deps=0 \
      gcc gcc-c++ make cmake pkgconf-pkg-config zlib-devel sqlite-devel curl-minimal && \
    microdnf clean all

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}" --no-modify-path && \
    rustc --version

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
# ---- owlmake `om` binary (the ubergraph builder, used by build_ubergraph.nf) ----
# Downloaded as a release binary from owlmake's GitHub releases (a portable glibc
# binary that runs on the UBI9 runtime) — no owlmake build needed. Pinned to a
# tag for reproducible builds; the pinned release must support the flags
# build_ubergraph.nf passes (-i, --graph-prefix, --offline, -o). Override with
# --build-arg OWLMAKE_RELEASE=vX.Y.Z (or =latest).
#
# Arch: the release ships per-arch assets (amd64|arm64), so each per-arch GrEBI
# build fetches the `om` matching its own machine.
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS om-dl
ARG OWLMAKE_RELEASE=v0.1.0
RUN microdnf install -y --setopt=install_weak_deps=0 curl-minimal && microdnf clean all
RUN case "$(uname -m)" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; esac; \
    base="https://github.com/EBISPOT/owlmake/releases"; \
    if [ "$OWLMAKE_RELEASE" = "latest" ]; then url="$base/latest/download/om-linux-$arch"; \
    else url="$base/download/$OWLMAKE_RELEASE/om-linux-$arch"; fi; \
    echo "fetching $url"; curl -fsSL -o /om "$url" && chmod 0755 /om

FROM ${BASE_IMAGE}
# Reset to root for the build/COPY steps below (base ships as non-root).
USER root

# Materialised-query export uses the official Neo4j Python driver.  Keep this
# runtime image functional even when the mutable development base tag predates
# the corresponding dependency addition in Dockerfile.base.
RUN python3 -c 'import neo4j' || pip3 install --no-cache-dir neo4j

# ---- Copy pre-built Rust binaries from cross-compile stage ----
# (grebi_reprefix is included in the grebi_* glob and is spawned by grebi_api)
COPY --from=rust-builder /usr/local/bin/grebi_* /usr/local/bin/
# owlmake `om`, used by the build_ubergraph dataload process.
COPY --from=om-dl /om /usr/local/bin/om
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
    cd /opt && npm install js-yaml@5.1.0 marked@18.0.5
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
