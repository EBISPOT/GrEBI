#!/bin/bash

set -e

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <solr_core> <tmp_port_to_use>"
    exit 1
fi

CORE=$1
PORT=$2

srun -t 5-0:0:0 --mem 64g -c 8 \
    ./07_create_db/solr/solr_import.slurm.sh \
    $CORE \
    $PORT

