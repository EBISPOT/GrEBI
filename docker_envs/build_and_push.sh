#!/bin/bash

set -e

docker build -t ghcr.io/ebispot/grebi_neo4j_with_extras:2026.05.0-community -f Dockerfile.neo4j_with_extras .

docker push ghcr.io/ebispot/grebi_neo4j_with_extras:2026.05.0-community



