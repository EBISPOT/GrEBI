#!/bin/bash

set -e

docker build -t ghcr.io/ebispot/grebi_solr_with_python:9.5.0 -f Dockerfile.solr_with_python .
docker build -t ghcr.io/ebispot/grebi_neo4j_with_python:5.18.0 -f Dockerfile.neo4j_with_python .
docker build -t ghcr.io/ebispot/rust_for_codon:1.74 -f Dockerfile.rust_for_codon .

docker push ghcr.io/ebispot/grebi_solr_with_python:9.5.0
docker push ghcr.io/ebispot/grebi_neo4j_with_python:5.18.0
docker push ghcr.io/ebispot/rust_for_codon:1.74
