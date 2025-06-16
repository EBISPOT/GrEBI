#!/bin/bash

if [ -z "$GREBI_SUBGRAPH" ]; then
  echo "Set GREBI_SUBGRAPH to run this script"
  exit 1
fi

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")
GREBI_HOME=$(dirname $(dirname $SCRIPT_PATH))

docker run \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $GREBI_HOME:$GREBI_HOME \
  -e GREBI_HOME=$GREBI_HOME \
  -e GREBI_OUT_DIR=$GREBI_HOME/out \
  -e GREBI_QUERY_YAMLS_PATH=$GREBI_HOME/materialised_queries \
  -e GREBI_DATALOAD_HOME=$GREBI_HOME/dataload \
  -e GREBI_SUBGRAPH=$GREBI_SUBGRAPH \
  -e NXF_USRMAP=$(id -u) \
  ghcr.io/ebispot/grebi_nextflow:latest \
  bash -c "cd $GREBI_HOME && nextflow dataload/nextflow/load_subgraph.nf \
    -c dataload/nextflow/local_64g_nextflow.config -resume"

