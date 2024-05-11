#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./07_create_db/rocksdb/rocksdb_import.slurm.py $1

