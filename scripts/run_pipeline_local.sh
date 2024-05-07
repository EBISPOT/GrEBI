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

# rm -rf $GREBI_NFS_TMP/$GREBI_CONFIG/*
# rm -rf $GREBI_HPS_TMP/$GREBI_CONFIG/*

# python3 ./scripts/dataload_00_prepare.py
# python3 ./scripts/dataload_01_ingest.py
# python3 ./scripts/dataload_02_assign_ids.py
# python3 ./scripts/dataload_03_merge.py
# python3 ./scripts/dataload_04_index.py
# python3 ./scripts/dataload_05_materialise_edges.py
python3 ./scripts/dataload_06_prepare_db_imports.py
# python3 07_create_db/neo4j/neo4j_import.py
# python3 08_run_queries/run_queries.py
# python3 07_create_db/solr/solr_import.py


