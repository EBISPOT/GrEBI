

FROM rust:1.90.0-bullseye

RUN apt-get update -y

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        JAVA_ARCH="x64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        JAVA_ARCH="aarch64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl https://corretto.aws/downloads/resources/21.0.6.7.1/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH}.tar.gz | tar -C /opt -xzf - && \
    echo "export JAVA_HOME=/opt/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH}" >> ~/.bashrc && \
    echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc && \
    ln -s /opt/amazon-corretto-21.0.6.7.1-linux-${JAVA_ARCH} /opt/java
ENV JAVA_HOME="/opt/java"
ENV PATH="$PATH:/opt/java/bin"



RUN mkdir /opt/neo4j && \
    curl https://ftp.ebi.ac.uk/pub/databases/spot/mirror/neo4j-community-2025.03.0-unix.tar.gz | tar -xz --strip-components=1 -C /opt/neo4j

RUN mkdir /opt/solr && \
    curl https://ftp.ebi.ac.uk/pub/databases/spot/mirror/solr-9.8.1.tgz | tar -xz --strip-components=1 -C /opt/solr

RUN echo "dbms.security.auth_enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
        echo "dbms.usage_report.enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
        echo "db.recovery.fail_on_missing_files=false" >> /opt/neo4j/conf/neo4j.conf 

# we set this in nextflow with an env var so don't want it to be overriden by the config file
RUN sed -i '/^server\.directories\.logs=/d' /opt/neo4j/conf/neo4j.conf


ENV PATH="$PATH:/opt/neo4j/bin:/opt/solr/bin"

# RUN echo "server.jvm.additional=-Dorg.neo4j.kernel.impl.index.schema.GenericNativeIndexPopulator.blockBasedPopulation=true" >> /opt/neo4j/conf/neo4j.conf


RUN apt-get install -y cmake clang pigz python3-pip && \
        pip3 install pandas tabulate openpyxl pyyaml py2neo requests

# Install PostgreSQL 18 from the official PGDG repository (Bullseye only ships PG 13)
RUN apt-get install -y gnupg lsb-release && \
    echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update -y && \
    apt-get install -y postgresql-18 postgresql-client-18

# Add PostgreSQL 18 server binaries to PATH
ENV PATH="$PATH:/usr/lib/postgresql/18/bin"

# Allow arbitrary UIDs to register themselves in /etc/passwd at runtime
# (needed by initdb when Nextflow runs containers with --user UID:GID)
RUN chmod a+w /etc/passwd /etc/group

COPY . /opt/grebi_dataload

RUN cd /opt/grebi_dataload && cargo build --release

ENV PATH="$PATH:/opt/grebi_dataload/target/release"



