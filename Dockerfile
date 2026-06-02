# syntax=docker/dockerfile:1

###############################################################################
# Stage 1 — Cross-compile Rust binaries (runs natively on the builder arch)
###############################################################################
FROM --platform=$BUILDPLATFORM rust:1.90.0-bullseye AS rust-builder
ARG TARGETPLATFORM

# Retry transient apt mirror errors instead of aborting the build.
RUN printf 'Acquire::Retries "5";\n' > /etc/apt/apt.conf.d/99-grebi-retries

# Install cross-compilation toolchain for arm64 when needed
RUN apt-get update -y && apt-get install -y cmake && \
    case "$TARGETPLATFORM" in \
      "linux/arm64") \
        apt-get install -y gcc-aarch64-linux-gnu && \
        rustup target add aarch64-unknown-linux-gnu ;; \
    esac && \
    rm -rf /var/lib/apt/lists/*

# Build dataload binaries
COPY dataload /opt/grebi_dataload
WORKDIR /opt/grebi_dataload
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/opt/grebi_dataload/target \
    case "$TARGETPLATFORM" in \
      "linux/arm64") \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc && \
        cargo build --release --target aarch64-unknown-linux-gnu && \
        cp target/aarch64-unknown-linux-gnu/release/grebi_* /usr/local/bin/ 2>/dev/null || true ;; \
      *) \
        cargo build --release && \
        cp target/release/grebi_* /usr/local/bin/ 2>/dev/null || true ;; \
    esac

# Build prefix service (needs grebi_shared via relative path ../../dataload/grebi_shared)
COPY dataload/grebi_shared /dataload/grebi_shared
COPY webapp/grebi_prefix_service /webapp/grebi_prefix_service
WORKDIR /webapp/grebi_prefix_service
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/webapp/grebi_prefix_service/target \
    case "$TARGETPLATFORM" in \
      "linux/arm64") \
        export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc && \
        cargo build --release --target aarch64-unknown-linux-gnu && \
        cp target/aarch64-unknown-linux-gnu/release/grebi_prefix_service /usr/local/bin/ ;; \
      *) \
        cargo build --release && \
        cp target/release/grebi_prefix_service /usr/local/bin/ ;; \
    esac

###############################################################################
# Stage 2 — Runtime image
###############################################################################
FROM rust:1.90.0-bullseye

# Make apt resilient to transient mirror errors. The multi-arch build emulates
# arm64 under QEMU and downloads ~250 MB in this layer; it was intermittently
# failing with "Connection reset by peer" while fetching a .deb. Retry instead
# of aborting the whole build. Applies to every apt-get in this stage.
RUN printf 'Acquire::Retries "5";\nAcquire::http::Timeout "60";\nAcquire::https::Timeout "60";\n' \
    > /etc/apt/apt.conf.d/99-grebi-retries

# ---- System packages (single apt-get layer) ----
RUN apt-get update -y && apt-get install -y \
    curl \
    gpg \
    cmake \
    clang \
    pigz \
    jq \
    python3-pip \
    supervisor \
    procps \
    rsync \
    gnupg \
    lsb-release \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# ---- PostgreSQL 18 ----
# Prevent initdb from running during apt install (segfaults under QEMU);
# the cluster is created at runtime instead.
RUN mkdir -p /etc/postgresql-common && \
    echo 'create_main_cluster = false' > /etc/postgresql-common/createcluster.conf && \
    echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y -o Dpkg::Options::="--force-confold" \
        postgresql-18 postgresql-client-18 postgresql-18-pgvector && \
    rm -rf /var/lib/apt/lists/*
ENV PATH="$PATH:/usr/lib/postgresql/18/bin"

# ---- Node.js 24 LTS + Caddy + Docker CLI (single apt layer) ----
RUN curl -sL https://deb.nodesource.com/setup_24.x | bash - && \
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg && \
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
    $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && apt-get install -y \
      nodejs \
      caddy \
      docker-ce-cli=5:29.1.3-1~debian.11~bullseye \
    && rm -rf /var/lib/apt/lists/*

# ---- Python packages ----
RUN pip3 install \
    requests \
    pyyaml \
    pandas \
    tabulate \
    openpyxl \
    py2neo

# ---- Java 21 (Amazon Corretto) ----
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        JAVA_ARCH="x64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        JAVA_ARCH="aarch64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl -L https://corretto.aws/downloads/resources/21.0.6.7.1/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH}.tar.gz | tar -C /opt -xzf - && \
    ln -s /opt/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH} /opt/java
ENV JAVA_HOME="/opt/java"
ENV PATH="$PATH:/opt/java/bin"

# ---- Maven ----
RUN mkdir -p /opt/maven && \
    curl https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz | tar -xz --strip-components=1 -C /opt/maven
ENV PATH="$PATH:/opt/maven/bin"

# ---- Neo4j 2025.03.0 ----
RUN mkdir /opt/neo4j && \
    curl https://ftp.ebi.ac.uk/pub/databases/spot/mirror/neo4j-community-2025.03.0-unix.tar.gz | tar -xz --strip-components=1 -C /opt/neo4j && \
    echo "dbms.security.auth_enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
    echo "dbms.usage_report.enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
    echo "db.recovery.fail_on_missing_files=false" >> /opt/neo4j/conf/neo4j.conf && \
    sed -i '/^server\.directories\.logs=/d' /opt/neo4j/conf/neo4j.conf
ENV PATH="$PATH:/opt/neo4j/bin"

# ---- Nextflow ----
ENV NEXTFLOW_VERSION=24.10.5
ENV NXF_VER=${NEXTFLOW_VERSION}
RUN curl -fsSL https://get.nextflow.io | bash && \
    mv nextflow /usr/local/bin/ && \
    chmod +x /usr/local/bin/nextflow

# ---- Permissions & directories ----
RUN chmod a+w /etc/passwd /etc/group && \
    mkdir -p /opt/grebi/data/neo4j \
             /opt/grebi/data/postgres \
             /opt/grebi/data/prefix_maps \
             /var/run/postgresql && \
    chmod 777 /var/run/postgresql

# ---- Copy pre-built Rust binaries from cross-compile stage ----
COPY --from=rust-builder /usr/local/bin/grebi_* /usr/local/bin/
COPY --from=rust-builder /usr/local/bin/grebi_prefix_service /usr/local/bin/
ENV PATH="$PATH:/usr/local/bin"

# Copy prefix maps
COPY dataload/prefix_maps /opt/grebi/data/prefix_maps

# Copy full dataload directory (scripts, prefix_maps, python utils needed at runtime by Nextflow processes)
COPY dataload /opt/grebi_dataload

# ---- Build grebi_api (Java) ----
COPY webapp/grebi_api /opt/grebi_api
WORKDIR /opt/grebi_api
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package assembly:single -DskipTests && \
    cp target/grebi-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/grebi_api.jar

# ---- Build grebi_cypher_service (Java) ----
COPY webapp/grebi_cypher_service /opt/grebi_cypher_service
WORKDIR /opt/grebi_cypher_service
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -DskipTests && \
    cp target/grebi_cypher_service-1.0-SNAPSHOT.jar /opt/grebi_cypher_service.jar

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
    cd /opt && npm install js-yaml marked puppeteer
COPY docs /opt/docs
COPY tests/export_neo4j.py /opt/export_neo4j.py
COPY tests/export_postgres.py /opt/export_postgres.py
COPY tests/compare_snapshots.py /opt/compare_snapshots.py
COPY tests/test_api_snapshots.py /opt/test_api_snapshots.py
COPY webapp/combined_entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

WORKDIR /opt
