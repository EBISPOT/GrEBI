#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./01_ingest/grebi_ingest_worker.slurm.py $1
