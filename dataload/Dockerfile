

FROM rust:1.90.0-bullseye


RUN curl https://corretto.aws/downloads/resources/21.0.6.7.1/amazon-corretto-21.0.6.7.1-linux-x64.tar.gz | tar -C /opt -xzf - && \
        echo 'export JAVA_HOME=/opt/amazon-corretto-21.0.6.7.1-linux-x64' >> ~/.bashrc && \
        echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
ENV JAVA_HOME="/opt/amazon-corretto-21.0.6.7.1-linux-x64/"
ENV PATH="$PATH:/opt/amazon-corretto-21.0.6.7.1-linux-x64/bin"



RUN mkdir /opt/neo4j && \
    curl https://dist.neo4j.org/neo4j-community-2025.03.0-unix.tar.gz | tar -xz --strip-components=1 -C /opt/neo4j

RUN mkdir /opt/solr && \
    curl https://archive.apache.org/dist/solr/solr/9.8.1/solr-9.8.1.tgz | tar -xz --strip-components=1 -C /opt/solr

RUN echo "dbms.security.auth_enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
        echo "dbms.usage_report.enabled=false" >> /opt/neo4j/conf/neo4j.conf && \
        echo "db.recovery.fail_on_missing_files=false" >> /opt/neo4j/conf/neo4j.conf 

# we set this in nextflow with an env var so don't want it to be overriden by the config file
RUN sed -i '/^server\.directories\.logs=/d' /opt/neo4j/conf/neo4j.conf


ENV PATH="$PATH:/opt/neo4j/bin:/opt/solr/bin"

# RUN echo "server.jvm.additional=-Dorg.neo4j.kernel.impl.index.schema.GenericNativeIndexPopulator.blockBasedPopulation=true" >> /opt/neo4j/conf/neo4j.conf


RUN apt-get install -y cmake clang pigz python3-pip && \
        pip3 install pandas tabulate openpyxl pyyaml py2neo requests

COPY . /opt/grebi_dataload

RUN cd /opt/grebi_dataload && cargo build --release

ENV PATH="$PATH:/opt/grebi_dataload/target/release"



