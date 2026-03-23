#!/bin/bash

if [ "$SLURM_JOB_PARTITION" != "datamover" ]; then
  echo "Must run on a datamover node"
  exit 1
fi

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <subgraph> <datarelease_path>"
  exit 1
fi

SUBGRAPH=$1
DATARELEASE_PATH=$2

./check_datarelease.sh $SUBGRAPH $DATARELEASE_PATH

STAGING_PATH=/nfs/public/rw/ontoapps/grebi/staging

if [ ! -d "$STAGING_PATH" ]; then
  echo "Staging path $STAGING_PATH does not exist"
  exit 1
fi

mkdir -p $STAGING_PATH/neo4j
mkdir -p $STAGING_PATH/solr
mkdir -p $STAGING_PATH/metadata
mkdir -p $STAGING_PATH/sqlite

echo Removing old files from staging

rm -rf $STAGING_PATH/neo4j/${SUBGRAPH}_neo4j
rm -rf $STAGING_PATH/solr/grebi_nodes_${SUBGRAPH}
rm -rf $STAGING_PATH/solr/grebi_autocomplete_${SUBGRAPH}
rm -rf $STAGING_PATH/solr/grebi_results__${SUBGRAPH}__*
rm -rf $STAGING_PATH/metadata/${SUBGRAPH}_metadata.json
rm -rf $STAGING_PATH/sqlite/${SUBGRAPH}.sqlite3

echo Extracting new data release

tar --use-compress-program=pigz -xf $DATARELEASE_PATH/${SUBGRAPH}_neo4j.tgz -C $STAGING_PATH/neo4j
tar --use-compress-program=pigz -xf $DATARELEASE_PATH/${SUBGRAPH}_solr.tgz -C $STAGING_PATH
cp -f $DATARELEASE_PATH/${SUBGRAPH}_metadata.json $STAGING_PATH/metadata/
cp -f $DATARELEASE_PATH/${SUBGRAPH}.sqlite3 $STAGING_PATH/sqlite/







