#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./05_materialize_edges/grebi_rocks2neo.slurm.py $1
