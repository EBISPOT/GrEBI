#!/bin/bash

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <solr_core>"
    exit 1
fi

CORE=$1

srun -t 5-0:0:0 --mem 64g -c 8 \
    ./07_create_db/solr/solr_import.slurm.sh \
    $CORE 

