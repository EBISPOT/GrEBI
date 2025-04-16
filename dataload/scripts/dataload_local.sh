#!/bin/bash

if [ -z "$GREBI_SUBGRAPH" ]; then
  echo "Set GREBI_SUBGRAPH to run this script"
  exit 1
fi

export GREBI_DATALOAD_HOME=~/grebi/dataload
export GREBI_QUERY_YAMLS_PATH=~/grebi/materialised_queries
export GREBI_OUT_DIR=~/grebi/$GREBI_SUBGRAPH/out
export GREBI_TIMESTAMP=$(date +"%Y-%b-%d")
export GREBI_MAX_ENTITIES=1000000000
export GREBI_NEXTFLOW_CONFIG=$GREBI_DATALOAD_HOME/nextflow/local_nextflow.config

mkdir -p $GREBI_OUT_DIR

nextflow $GREBI_DATALOAD_HOME/nextflow/load_subgraph.nf -c $GREBI_NEXTFLOW_CONFIG -resume

