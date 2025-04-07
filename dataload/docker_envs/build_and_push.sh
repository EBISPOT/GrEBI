#!/bin/bash

set -e

docker build -t ghcr.io/ebispot/grebi_solr_with_extras:9.5.0 -f Dockerfile.solr_with_extras .
docker build -t ghcr.io/ebispot/grebi_neo4j_with_extras:2025.03.0 -f Dockerfile.neo4j_with_extras .
docker build -t ghcr.io/ebispot/grebi_python:3.11 -f Dockerfile.python .
docker build -t ghcr.io/ebispot/rust_for_codon:1.79 -f Dockerfile.rust_for_codon .

docker push ghcr.io/ebispot/grebi_solr_with_extras:9.5.0
docker push ghcr.io/ebispot/grebi_neo4j_with_extras:2025.03.0
docker push ghcr.io/ebispot/grebi_python:3.11
docker push ghcr.io/ebispot/rust_for_codon:1.79



