#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <datarelease_path>"
  exit 1
fi

DATARELEASE_PATH=$1

if [ ! -d "$DATARELEASE_PATH" ]; then
  echo "Data release path $DATARELEASE_PATH does not exist"
  exit 1
fi

# Discover subgraphs from neo4j archives
SUBGRAPHS=($(ls "$DATARELEASE_PATH"/*_neo4j.tar.xz 2>/dev/null | sed 's|.*/||; s|_neo4j.tar.xz||'))
if [ ${#SUBGRAPHS[@]} -eq 0 ]; then
  echo "No neo4j archives (*_neo4j.tar.xz) found in $DATARELEASE_PATH"
  exit 1
fi

echo "Checking data release at $DATARELEASE_PATH (subgraphs: ${SUBGRAPHS[*]})"

for SUBGRAPH in "${SUBGRAPHS[@]}"; do
  for f in "${SUBGRAPH}_neo4j.tar.xz" "${SUBGRAPH}_metadata.json"; do
    if [ ! -f "$DATARELEASE_PATH/$f" ]; then
      echo "$f not found in $DATARELEASE_PATH"
      exit 1
    fi
  done
done
if [ ! -f "$DATARELEASE_PATH/postgres.tar.xz" ]; then
  echo "postgres.tar.xz not found in $DATARELEASE_PATH"
  exit 1
fi
if [ ! -d "$DATARELEASE_PATH/query_results" ]; then
  echo "query_results/ not found in $DATARELEASE_PATH"
  exit 1
fi
