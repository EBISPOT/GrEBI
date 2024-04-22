#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <datasources_files.jsonl> <groups_txt_path>"
    exit 1
fi

export PYTHONUNBUFFERED=TRUE

python3 ./02_equivalences/grebi_assign_ids_worker.slurm.py $1 $2
