#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./05_materialise/grebi_materialise.slurm.py $1
