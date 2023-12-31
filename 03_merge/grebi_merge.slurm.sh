#!/bin/bash

export PYTHONUNBUFFERED=TRUE

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <grebi_config.json> <datasource_files.jsonl>"
    exit 1
fi

python3 ./03_merge/grebi_merge.slurm.py $1 $2


