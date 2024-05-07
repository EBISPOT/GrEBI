#!/bin/bash

export PYTHONUNBUFFERED=TRUE

python3 ./05_prepare_db_import/grebi_prepare_db_import.local.py $1
