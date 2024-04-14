#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./06_create_db/neo4j_import.slurm.py $1

