#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./07_create_db/solr/solr_import.slurm.py $1 $2

