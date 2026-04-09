#!/bin/bash

if [ -z "$GREBI_SUBGRAPHS" ]; then
  echo "Set GREBI_SUBGRAPHS (comma-separated) to run this script"
  exit 1
fi

if [ -z "$GREBI_NF_DOWNLOAD_CONFIG" ]; then
  echo "GREBI_NF_DOWNLOAD_CONFIG not set, using default local config"
  GREBI_NF_DOWNLOAD_CONFIG="dataload/nextflow/download_local.config"
fi

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")

GREBI_HOME=$(dirname $(dirname $SCRIPT_PATH))
GREBI_DOWNLOADS_BASE=${GREBI_DOWNLOADS_PATH:-$GREBI_HOME/downloads}

HOST_UID=$(id -u)
HOST_GID=$(id -g)

USER_OPT=""
if [ "$(uname)" != "Darwin" ]; then
  USER_OPT="--user $HOST_UID:$HOST_GID --group-add $(stat -c %g /var/run/docker.sock)"
fi

IFS=',' read -ra SUBGRAPH_ARRAY <<< "$GREBI_SUBGRAPHS"
for sg in "${SUBGRAPH_ARRAY[@]}"; do
  DL_PATH=$GREBI_DOWNLOADS_BASE/$sg
  TMP_DIR=$GREBI_HOME/tmp/${sg}_download
  REPORTS_DIR=$GREBI_HOME/out/$sg/reports_download
  mkdir -p $TMP_DIR/NXF_WORK $TMP_DIR/NXF_HOME $TMP_DIR/NXF_TEMP $TMP_DIR/NXF_CACHE_DIR $REPORTS_DIR
  mkdir -p $DL_PATH

  docker run \
    $USER_OPT \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v $GREBI_HOME:$GREBI_HOME \
    -v $DL_PATH:$DL_PATH \
    -e GREBI_HOME=$GREBI_HOME \
    -e GREBI_DOWNLOADS_PATH=$DL_PATH \
    -e GREBI_SUBGRAPH=$sg \
    -e NXF_USRMAP=${HOST_UID} \
    -e HOST_UID=${HOST_UID} \
    -e HOST_GID=${HOST_GID} \
    -e NXF_WORK=$TMP_DIR/NXF_WORK \
    -e NXF_HOME=$TMP_DIR/NXF_HOME \
    -e NXF_TEMP=$TMP_DIR/NXF_TEMP \
    -e NXF_CACHE_DIR=$TMP_DIR/NXF_CACHE_DIR \
    ghcr.io/ebispot/grebi_combined:dev \
    bash -c "cd $GREBI_HOME && nextflow dataload/nextflow/download.nf \
      -c $GREBI_NF_DOWNLOAD_CONFIG -resume \
      -with-report $REPORTS_DIR/report.html \
      -with-trace $REPORTS_DIR/trace.txt \
      -with-timeline $REPORTS_DIR/timeline.html \
      -with-dag $REPORTS_DIR/dag.html \
      $GREBI_NF_EXTRA_ARGS"
done
