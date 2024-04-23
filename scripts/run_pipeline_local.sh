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

python3 ./dataload.py ./configs/pipeline_configs/$CONFIG_NAME.json
python3 06_create_db/neo4j/neo4j_import.py ./configs/pipeline_configs/$CONFIG_NAME.json

cp -f $GREBI_HPS_TMP/$GREBI_CONFIG/04_index/metadata.json $GREBI_HPS_TMP/$GREBI_CONFIG/06_create_db/neo4j/

