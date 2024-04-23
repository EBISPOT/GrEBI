#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./05_materialize_edges/grebi_materialize_edges.slurm.py $1
