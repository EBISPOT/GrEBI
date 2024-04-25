#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./06_create_db/solr/solr_import.slurm.py $1

