#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <subgraph> <datarelease_path>"
  exit 1
fi

SUBGRAPH=$1
DATARELEASE_PATH=$2

if [ ! -d "$DATARELEASE_PATH" ]; then
  echo "Data release path $DATARELEASE_PATH does not exist"
  exit 1
fi

echo "Checking the data release for subgraph $SUBGRAPH at $DATARELEASE_PATH looks sane"

if [ ! -f "$DATARELEASE_PATH/${SUBGRAPH}_neo4j.tgz" ]; then
  echo "neo4j archive $DATARELEASE_PATH/${SUBGRAPH}_neo4j.tgz not found"
  exit 1
fi
if [ ! -f "$DATARELEASE_PATH/${SUBGRAPH}_solr.tgz" ]; then
  echo "solr archive $DATARELEASE_PATH/${SUBGRAPH}_solr.tgz not found"
  exit 1
fi
if [ ! -f "$DATARELEASE_PATH/${SUBGRAPH}.sqlite3" ]; then
  echo "sqlite3 database $DATARELEASE_PATH/${SUBGRAPH}.sqlite3 not found"
  exit 1
fi
if [ ! -f "$DATARELEASE_PATH/${SUBGRAPH}_metadata.json" ]; then
  echo "summary json $DATARELEASE_PATH/${SUBGRAPH}_metadata.json not found"
  exit 1
fi
if [ ! -f "$DATARELEASE_PATH/postgres.tgz" ]; then
  echo "postgres archive $DATARELEASE_PATH/postgres.tgz not found"
  exit 1
fi
if [ ! -d "$DATARELEASE_PATH/query_results" ]; then
  echo "query_results folder $DATARELEASE_PATH/query_results not found"
  exit 1
fi
