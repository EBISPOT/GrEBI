#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <datasources_files.jsonl> <equivalences_db_path>"
    exit 1
fi

export PYTHONUNBUFFERED=TRUE

python3 ./02_equivalences/grebi_expand_subjects_worker.slurm.py $1 $2
