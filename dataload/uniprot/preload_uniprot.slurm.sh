#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./uniprot/preload_uniprot.slurm.py $1

