#!/bin/bash

set -e

docker build -t ghcr.io/ebispot/grebi_neo4j_with_extras:2025.03.0-community -f Dockerfile.neo4j_with_extras .
docker build -t ghcr.io/ebispot/grebi_nextflow:24.10.5 -f Dockerfile.nextflow .

docker push ghcr.io/ebispot/grebi_neo4j_with_extras:2025.03.0-community
docker push ghcr.io/ebispot/grebi_nextflow:24.10.5



