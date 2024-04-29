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

rm -rf $GREBI_NFS_TMP/$GREBI_CONFIG/*
srun -t 2:0:0 --mem=2G rm -rf $GREBI_HPS_TMP/$GREBI_CONFIG/*

python3 ./scripts/dataload_00_prepare.py
python3 ./scripts/dataload_01_ingest.py
python3 ./scripts/dataload_02_assign_ids.py
python3 ./scripts/dataload_03_merge.py
python3 ./scripts/dataload_04_index.py
python3 ./scripts/dataload_05_prepare_db_imports.py
python3 06_create_db/neo4j/neo4j_import.py

echo $(date): Compressing neo4j data

srun -t 2:0:0 --mem=2G tar -cf  \
    $GREBI_NFS_TMP/$GREBI_CONFIG.tgz \
    --use-compress-program="pigz --fast" \
    -C $GREBI_HPS_TMP/$GREBI_CONFIG/06_create_db/neo4j data \
    -C $GREBI_HPS_TMP/$GREBI_CONFIG/04_index metadata.json
    
echo $(date): Copying to FTP

export RELEASE_DATE=`date +%Y_%m_%d__%H_%M`
export FILENAME=$GREBI_CONFIG-$RELEASE_DATE.tgz

srun --partition=datamover --time 2:30:00 --mem=5G \
    cp -f $GREBI_NFS_TMP/$GREBI_CONFIG.tgz /nfs/ftp/public/databases/spot/kg/$FILENAME

echo $(date): Done



