#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./07_create_db/neo4j/neo4j_import.slurm.py $1 nojson
python3 ./07_create_db/neo4j/neo4j_import.slurm.py $1 full

