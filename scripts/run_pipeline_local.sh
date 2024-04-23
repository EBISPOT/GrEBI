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
rm -rf $GREBI_HPS_TMP/$GREBI_CONFIG/*

python3 ./scripts/dataload.py
python3 06_create_db/neo4j/neo4j_import.py


