#!/bin/bash

if [ -z "$GREBI_SUBGRAPH" ]; then
  echo "Set GREBI_SUBGRAPH to run this script"
  exit 1
fi

if [ -z "$GREBI_NF_DOWNLOAD_CONFIG" ]; then
  echo "GREBI_NF_DOWNLOAD_CONFIG not set, using default local config"
  GREBI_NF_DOWNLOAD_CONFIG="dataload/nextflow/download_local.config"
fi

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")

GREBI_HOME=$(dirname $(dirname $SCRIPT_PATH))
GREBI_DOWNLOADS_PATH=${GREBI_DOWNLOADS_PATH:-$GREBI_HOME/downloads}

TMP_DIR=$GREBI_HOME/tmp/${GREBI_SUBGRAPH}_download
mkdir -p $TMP_DIR/NXF_WORK $TMP_DIR/NXF_HOME $TMP_DIR/NXF_TEMP $TMP_DIR/NXF_CACHE_DIR
mkdir -p $GREBI_DOWNLOADS_PATH

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
  -e GREBI_DOWNLOADS_PATH=$GREBI_DOWNLOADS_PATH \
  -e GREBI_SUBGRAPH=$GREBI_SUBGRAPH \
  -e NXF_USRMAP=$(id -u) \
  -e NXF_WORK=$TMP_DIR/NXF_WORK \
  -e NXF_HOME=$TMP_DIR/NXF_HOME \
  -e NXF_TEMP=$TMP_DIR/NXF_TEMP \
  -e NXF_CACHE_DIR=$TMP_DIR/NXF_CACHE_DIR \
  ghcr.io/ebispot/grebi_nextflow:latest \
  bash -c "cd $GREBI_HOME && nextflow dataload/nextflow/download.nf \
    -c $GREBI_NF_DOWNLOAD_CONFIG -resume $GREBI_NF_EXTRA_ARGS"
