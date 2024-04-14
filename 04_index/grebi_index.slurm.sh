#!/bin/bash

export PYTHONUNBUFFERED=TRUE

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <grebi_config.json>"
    exit 1
fi

python3 ./04_index/grebi_index.slurm.py $1


