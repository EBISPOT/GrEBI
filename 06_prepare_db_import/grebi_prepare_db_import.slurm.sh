#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./06_prepare_db_import/grebi_prepare_db_import.slurm.py $1
