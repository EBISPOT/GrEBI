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

./check_datarelease.sh $DATARELEASE_PATH

STAGING_PATH=/nfs/public/rw/ontoapps/grebi/staging

if [ ! -d "$STAGING_PATH" ]; then
  echo "Staging path $STAGING_PATH does not exist"
  exit 1
fi

# Discover subgraphs from neo4j archives
SUBGRAPHS=($(ls "$DATARELEASE_PATH"/*_neo4j.tgz | sed 's|.*/||; s|_neo4j.tgz||'))

echo "Deploying release to staging (subgraphs: ${SUBGRAPHS[*]})"

echo Removing old files from staging

rm -rf $STAGING_PATH/neo4j
rm -rf $STAGING_PATH/solr
rm -rf $STAGING_PATH/metadata
rm -rf $STAGING_PATH/sqlite
rm -rf $STAGING_PATH/postgres

mkdir -p $STAGING_PATH/neo4j
mkdir -p $STAGING_PATH/solr
mkdir -p $STAGING_PATH/metadata
mkdir -p $STAGING_PATH/sqlite
mkdir -p $STAGING_PATH/postgres

echo Extracting new data release

for SUBGRAPH in "${SUBGRAPHS[@]}"; do
  tar --use-compress-program=pigz -xf $DATARELEASE_PATH/${SUBGRAPH}_neo4j.tgz -C $STAGING_PATH/neo4j
  cp -f $DATARELEASE_PATH/${SUBGRAPH}_metadata.json $STAGING_PATH/metadata/
  cp -f $DATARELEASE_PATH/${SUBGRAPH}.sqlite3 $STAGING_PATH/sqlite/
done
tar --use-compress-program=pigz -xf $DATARELEASE_PATH/solr.tgz -C $STAGING_PATH/solr
tar --use-compress-program=pigz -xf $DATARELEASE_PATH/postgres.tgz -C $STAGING_PATH/postgres







