#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./00_fetch_data/uniprot/preload_uniprot.slurm.py $1

