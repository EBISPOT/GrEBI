# syntax=docker/dockerfile:1
FROM rust:1.90.0-bullseye

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
    && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL 18 from PGDG repository (Bullseye only ships PG 13)
RUN echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update -y && \
    apt-get install -y postgresql-18 postgresql-client-18 postgresql-18-pgvector && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/usr/lib/postgresql/18/bin"

RUN pip3 install \
    requests \
    pyyaml \
    pandas \
    tabulate \
    openpyxl \
    py2neo

# Install Chromium for Puppeteer PDF generation
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    chromium \
    fonts-liberation \
    fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Install Java 21 (Amazon Corretto)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        JAVA_ARCH="x64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        JAVA_ARCH="aarch64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl -L https://corretto.aws/downloads/resources/21.0.6.7.1/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH}.tar.gz | tar -C /opt -xzf - && \
    echo "export JAVA_HOME=/opt/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH}" >> ~/.bashrc && \
    echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc && \
    ln -s /opt/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH} /opt/java
ENV JAVA_HOME="/opt/java"
ENV PATH="$PATH:/opt/java/bin"

# Install Maven
RUN mkdir -p /opt/maven && \
    curl https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz | tar -xz --strip-components=1 -C /opt/maven
ENV PATH="$PATH:/opt/maven/bin"

# Install Neo4j 2025.03.0
RUN mkdir /opt/neo4j && \
    curl https://ftp.ebi.ac.uk/pub/databases/spot/mirror/neo4j-community-2025.03.0-unix.tar.gz | tar -xz --strip-components=1 -C /opt/neo4j

RUN echo "dbms.security.auth_enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
    echo "dbms.usage_report.enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
    echo "db.recovery.fail_on_missing_files=false" >> /opt/neo4j/conf/neo4j.conf

# we set this in nextflow with an env var so don't want it to be overridden by the config file
RUN sed -i '/^server\.directories\.logs=/d' /opt/neo4j/conf/neo4j.conf

ENV PATH="$PATH:/opt/neo4j/bin"

# Install Node.js 18
RUN curl -sL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# Install Caddy for serving the UI
RUN curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg && \
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list && \
    apt-get update && apt-get install -y caddy && \
    rm -rf /var/lib/apt/lists/*

# Install Docker CLI (for Nextflow Docker executor)
RUN install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
    $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli=5:29.1.3-1~debian.11~bullseye && \
    rm -rf /var/lib/apt/lists/*

# Install Nextflow
ENV NEXTFLOW_VERSION=24.10.5
ENV NXF_VER=${NEXTFLOW_VERSION}
RUN curl -fsSL https://get.nextflow.io | bash && \
    mv nextflow /usr/local/bin/ && \
    chmod +x /usr/local/bin/nextflow

# Allow arbitrary UIDs to register themselves in /etc/passwd at runtime
# (needed by initdb when Nextflow runs containers with --user UID:GID)
RUN chmod a+w /etc/passwd /etc/group

# Create working directories
RUN mkdir -p /opt/grebi/data/neo4j \
             /opt/grebi/data/postgres \
             /opt/grebi/data/prefix_maps \
             /var/run/postgresql && \
    chmod 777 /var/run/postgresql

# Build Rust dataload pipeline
COPY dataload /opt/grebi_dataload
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/opt/grebi_dataload/target \
    cd /opt/grebi_dataload && cargo build --release && \
    cp target/release/grebi_* /usr/local/bin/ 2>/dev/null || true
ENV PATH="$PATH:/usr/local/bin"

# Copy prefix maps
COPY dataload/prefix_maps /opt/grebi/data/prefix_maps

# Build and install grebi_prefix_service
COPY dataload/grebi_shared /dataload/grebi_shared
COPY webapp/grebi_prefix_service /webapp/grebi_prefix_service
WORKDIR /webapp/grebi_prefix_service
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/webapp/grebi_prefix_service/target \
    cargo build --release && \
    cp target/release/grebi_prefix_service /usr/local/bin/

# Build grebi_api
COPY webapp/grebi_api /opt/grebi_api
WORKDIR /opt/grebi_api
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package assembly:single -DskipTests && \
    cp target/grebi-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/grebi_api.jar

# Build grebi_cypher_service
COPY webapp/grebi_cypher_service /opt/grebi_cypher_service
WORKDIR /opt/grebi_cypher_service
RUN --mount=type=cache,target=/root/.m2 \
    mvn clean package -DskipTests && \
    cp target/grebi_cypher_service-1.0-SNAPSHOT.jar /opt/grebi_cypher_service.jar

# Build grebi_ui
COPY docs /opt/grebi_ui/docs
COPY webapp/grebi_ui /opt/grebi_ui
WORKDIR /opt/grebi_ui

# Create .env.ebi if missing (gitignored so not included in COPY)
RUN test -f .env.ebi || printf 'PUBLIC_URL=/\nGREBI_FRONTEND=ebi\n' > .env.ebi

RUN --mount=type=cache,target=/root/.npm \
    npm install && \
    mkdir -p dist && \
    chmod -R 777 dist

# Copy query templates for integration tests
COPY query_templates /opt/query_templates

# Create supervisor configuration
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
