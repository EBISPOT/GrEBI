#!/bin/bash

if [ "$SLURM_JOB_PARTITION" != "datamover" ]; then
  echo "Must run on a datamover node"
  exit 1
fi

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <datarelease_path>"
  exit 1
fi

DATARELEASE_PATH=$1

if [ ! -d "$DATARELEASE_PATH" ]; then
  echo "Data release path $DATARELEASE_PATH does not exist"
  exit 1
fi

STAGING_PATH=${GREBI_PUBLIC_DIR:-/nfs/public/rw/ontoapps/grebi}/staging

if [ ! -d "$STAGING_PATH" ]; then
  echo "Staging path $STAGING_PATH does not exist"
  exit 1
fi

# Discover subgraphs from neo4j archives
SUBGRAPHS=($(ls "$DATARELEASE_PATH"/*_neo4j.tgz 2>/dev/null | sed 's|.*/||; s|_neo4j.tgz||'))
if [ ${#SUBGRAPHS[@]} -eq 0 ]; then
  echo "No neo4j archives (*_neo4j.tgz) found in $DATARELEASE_PATH"
  exit 1
fi

for SUBGRAPH in "${SUBGRAPHS[@]}"; do
  for f in "${SUBGRAPH}_neo4j.tgz"; do
    if [ ! -f "$DATARELEASE_PATH/$f" ]; then
      echo "$f not found in $DATARELEASE_PATH"
      exit 1
    fi
  done
done

echo "Deploying release to staging (subgraphs: ${SUBGRAPHS[*]})"

echo Removing old files from staging

rm -rf $STAGING_PATH/neo4j

mkdir -p $STAGING_PATH/neo4j

echo Extracting new data release

for SUBGRAPH in "${SUBGRAPHS[@]}"; do
  tar --use-compress-program=pigz -xf $DATARELEASE_PATH/${SUBGRAPH}_neo4j.tgz -C $STAGING_PATH/neo4j
done



