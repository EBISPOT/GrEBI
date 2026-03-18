#!/bin/bash

if [ -z "$GREBI_SUBGRAPH" ]; then
  echo "Set GREBI_SUBGRAPH to run this script"
  exit 1
fi

if [ -z "$GREBI_NF_CONFIG" ]; then
  echo "GREBI_NF_CONFIG not set, using default local 64g config"
  GREBI_NF_CONFIG="dataload/nextflow/local_64g_nextflow.config"
fi

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")

# This folder is mounted to have the same path in the Docker containers as it does on the host.
# This includes: 
#   - The nextflow container that runs nextflow (which we start below)
#   - The containers nextflow starts to run processes, configured in the nextflow config files
#
GREBI_HOME=$(dirname $(dirname $SCRIPT_PATH))

TMP_DIR=$GREBI_HOME/tmp/$GREBI_SUBGRAPH
OUT_DIR=$GREBI_HOME/out/$GREBI_SUBGRAPH
GREBI_DOWNLOADS_PATH=${GREBI_DOWNLOADS_PATH:-$GREBI_HOME/downloads}

mkdir -p $TMP_DIR/NXF_WORK $TMP_DIR/NXF_HOME $TMP_DIR/NXF_TEMP $TMP_DIR/NXF_CACHE_DIR

mkdir -p $TMP_DIR

# Ensure nested Docker containers (spawned by Nextflow) run with the same
# UID/GID as the host user to avoid permission issues when writing to the
# bind-mounted work directory on GitHub Actions.
# On macOS (e.g. Rancher Desktop) this is not needed as containers already
# run with proper permissions to access the Docker socket.
HOST_UID=$(id -u)
HOST_GID=$(id -g)

USER_OPT=""
if [ "$(uname)" != "Darwin" ]; then
  USER_OPT="--user $HOST_UID:$HOST_GID --group-add $(stat -c %g /var/run/docker.sock)"
fi

docker run \
  $USER_OPT \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $GREBI_HOME:$GREBI_HOME \
  -v $GREBI_DOWNLOADS_PATH:$GREBI_DOWNLOADS_PATH \
  -e GREBI_HOME=$GREBI_HOME \
  -e GREBI_OUT_DIR=$OUT_DIR \
  -e GREBI_QUERY_YAMLS_PATH=$GREBI_HOME/materialised_queries \
  -e GREBI_DATALOAD_HOME=$GREBI_HOME/dataload \
  -e GREBI_SUBGRAPH=$GREBI_SUBGRAPH \
  -e GREBI_DOWNLOADS_PATH=$GREBI_DOWNLOADS_PATH \
  -e NXF_USRMAP=$(id -u) \
  -e NXF_WORK=$TMP_DIR/NXF_WORK \
  -e NXF_HOME=$TMP_DIR/NXF_HOME\
  -e NXF_TEMP=$TMP_DIR/NXF_TEMP \
  -e NXF_CACHE_DIR=$TMP_DIR/NXF_CACHE_DIR \
  ghcr.io/ebispot/grebi_nextflow:latest \
  bash -c "cd $GREBI_HOME && nextflow dataload/nextflow/main.nf \
    -c $GREBI_NF_CONFIG -resume $GREBI_NF_EXTRA_ARGS"

