#!/bin/bash

set -e

export PYTHONUNBUFFERED=TRUE

if [[ -z "$GREBI_CONFIG" ]]; then
    echo "GREBI_CONFIG not set" 1>&2
    exit 1
fi
if [[ -z "$GREBI_NFS_TMP" ]]; then
    echo "GREBI_NFS_TMP not set" 1>&2
    exit 1
fi
if [[ -z "$GREBI_HPS_TMP" ]]; then
    echo "GREBI_HPS_TMP not set" 1>&2
    exit 1
fi

#rm -rf $GREBI_NFS_TMP/$GREBI_CONFIG/*
#srun -t 2:0:0 --mem=2G rm -rf $GREBI_HPS_TMP/$GREBI_CONFIG/*

#python3 ./scripts/dataload_00_prepare.py
#python3 ./scripts/dataload_01_ingest.py

# python3 ./scripts/dataload_02_assign_ids.py
#python3 ./scripts/dataload_03_merge.py
#python3 ./scripts/dataload_04_index.py
#python3 ./scripts/dataload_05_materialise.py
#python3 ./scripts/dataload_06_prepare_db_imports.py
#python3 07_create_db/neo4j/neo4j_import.py
#python3 07_create_db/rocksdb/rocksdb_import.py
python3 07_create_db/solr/solr_import.py

exit 0

echo $(date): Compressing data

mkdir -p $GREBI_NFS_TMP/${GREBI_CONFIG}/release

srun -t 8:0:0 --mem=32G --cpus-per-task 32 tar -cf  \
    $GREBI_NFS_TMP/${GREBI_CONFIG}/release/neo4j.tgz \
    --use-compress-program="pigz --fast" \
    -C $GREBI_HPS_TMP/$GREBI_CONFIG/07_create_db neo4j

srun -t 8:0:0 --mem=32G --cpus-per-task 32 tar -cf  \
    $GREBI_NFS_TMP/${GREBI_CONFIG}/release/solr.tgz \
    --use-compress-program="pigz --fast" \
    -C $GREBI_HPS_TMP/$GREBI_CONFIG/07_create_db solr

srun -t 8:0:0 --mem=32G --cpus-per-task 32 tar -cf  \
    $GREBI_NFS_TMP/${GREBI_CONFIG}/release/rocksdb.tgz \
    --use-compress-program="pigz --fast" \
    -C $GREBI_HPS_TMP/$GREBI_CONFIG/07_create_db rocksdb

srun -t 1:0:0 --mem=8G --cpus-per-task 8 \
    cat $GREBI_HPS_TMP/$GREBI_CONFIG/04_index/metadata.jsonl | pigz --fast > $GREBI_NFS_TMP/$GREBI_CONFIG/release/metadata.jsonl.gz


echo $(date): Copying to FTP

export RELEASE_DATE=`date +%Y_%m_%d__%H_%M`
export FTP_DIR=/nfs/ftp/public/databases/spot/kg

srun --partition=datamover --time 2:30:00 --mem=5G bash -c "\
    mkdir -p $FTP_DIR/$GREBI_CONFIG && cp -r $GREBI_NFS_TMP/${GREBI_CONFIG}/release $FTP_DIR/$GREBI_CONFIG/$RELEASE_DATE"

echo $(date): Data release complete

echo $(date): Running queries
python3 08_run_queries/run_queries.py

echo $(date): Copying materialised queries to FTP

srun -t 1:0:0 --mem=8G --cpus-per-task 8 \
    cat $GREBI_HPS_TMP/$GREBI_CONFIG/08_run_queries/materialised_queries.sqlite | pigz --fast > $GREBI_NFS_TMP/$GREBI_CONFIG/release/materialised_queries.sqlite.gz

srun --partition=datamover --time 2:30:00 --mem=5G bash -c "\
    cp -f $GREBI_NFS_TMP/$GREBI_CONFIG/release/materialised_queries.sqlite.gz $FTP_DIR/$GREBI_CONFIG/$RELEASE_DATE/"

echo $(date): FINISHED

