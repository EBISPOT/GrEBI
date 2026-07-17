#!/bin/bash

if [ -z "$GREBI_SUBGRAPHS" ]; then
  echo "Set GREBI_SUBGRAPHS (comma-separated) to run this script"
  exit 1
fi

if [ -z "$GREBI_NF_CONFIG" ]; then
  echo "GREBI_NF_CONFIG not set, using default local 4 GB RAM config"
  GREBI_NF_CONFIG="dataload/nextflow/local_4g_nextflow.config"
fi

SCRIPT_PATH=$(dirname "$(readlink -f "$0")")

# This folder is mounted to have the same path in the Docker containers as it does on the host.
# This includes: 
#   - The nextflow container that runs nextflow (which we start below)
#   - The containers nextflow starts to run processes, configured in the nextflow config files
#
GREBI_HOME=$(dirname $(dirname $SCRIPT_PATH))

TMP_DIR=$(readlink -f $GREBI_HOME/tmp 2>/dev/null || echo $GREBI_HOME/tmp)
OUT_DIR=$(readlink -f $GREBI_HOME/out 2>/dev/null || echo $GREBI_HOME/out)
GREBI_DOWNLOADS_PATH=${GREBI_DOWNLOADS_PATH:-$GREBI_HOME/downloads}
GREBI_DOWNLOADS_PATH=$(readlink -f $GREBI_DOWNLOADS_PATH 2>/dev/null || echo $GREBI_DOWNLOADS_PATH)

REPORTS_DIR=$OUT_DIR/reports
mkdir -p $TMP_DIR/NXF_WORK $TMP_DIR/NXF_HOME $TMP_DIR/NXF_TEMP $TMP_DIR/NXF_CACHE_DIR $REPORTS_DIR

mkdir -p $TMP_DIR

# Ensure nested Docker containers (spawned by Nextflow) run with the same
# UID/GID as the host user to avoid permission issues when writing to the
# bind-mounted work directory on GitHub Actions.
# On macOS the orchestrator runs as root (Docker Desktop handles socket
# permissions natively); on Linux we run as the host user with the Docker
# socket group.
# The runtime image ships non-root (USER grebi), so the orchestrator must be
# given explicit socket access: root on macOS, or the host uid + docker socket
# group on Linux. Nested process containers still run as the host uid via the
# Nextflow config's docker.runOptions.
HOST_UID=$(id -u)
HOST_GID=$(id -g)

if [ "$(uname)" = "Darwin" ]; then
  USER_OPT="--user 0:0"
else
  USER_OPT="--user $HOST_UID:$HOST_GID --group-add $(stat -c %g /var/run/docker.sock)"
fi

# Build extra volume mounts for paths that live outside GREBI_HOME (e.g. symlinks to /data/)
EXTRA_VOLS=""
case "$TMP_DIR" in "$GREBI_HOME"*) ;; *) EXTRA_VOLS="$EXTRA_VOLS -v $TMP_DIR:$TMP_DIR" ;; esac
case "$OUT_DIR" in "$GREBI_HOME"*) ;; *) EXTRA_VOLS="$EXTRA_VOLS -v $OUT_DIR:$OUT_DIR" ;; esac

docker run \
  $USER_OPT \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $GREBI_HOME:$GREBI_HOME \
  -v $GREBI_DOWNLOADS_PATH:$GREBI_DOWNLOADS_PATH \
  $EXTRA_VOLS \
  -e GREBI_HOME=$GREBI_HOME \
  -e GREBI_OUT_DIR=$OUT_DIR \
  -e GREBI_QUERY_YAMLS_PATH=$GREBI_HOME/query_templates \
  -e GREBI_DATALOAD_HOME=$GREBI_HOME/dataload \
  -e GREBI_SUBGRAPHS=$GREBI_SUBGRAPHS \
  -e GREBI_DOWNLOADS_PATH=$GREBI_DOWNLOADS_PATH \
  -e NXF_USRMAP=${HOST_UID} \
  -e HOST_UID=${HOST_UID} \
  -e HOST_GID=${HOST_GID} \
  -e NXF_WORK=$TMP_DIR/NXF_WORK \
  -e NXF_HOME=$TMP_DIR/NXF_HOME\
  -e NXF_TEMP=$TMP_DIR/NXF_TEMP \
  -e NXF_CACHE_DIR=$TMP_DIR/NXF_CACHE_DIR \
  ghcr.io/ebispot/grebi_combined:dev \
  bash -c "cd $GREBI_HOME && nextflow dataload/nextflow/main.nf \
    -c $GREBI_NF_CONFIG -resume \
    -with-report $REPORTS_DIR/report.html \
    -with-trace $REPORTS_DIR/trace.txt \
    -with-timeline $REPORTS_DIR/timeline.html \
    -with-dag $REPORTS_DIR/dag.html \
    $GREBI_NF_EXTRA_ARGS"

