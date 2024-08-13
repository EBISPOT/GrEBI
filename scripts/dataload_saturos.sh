#!/bin/bash
export GREBI_HOME=/home/james/grebi
export GREBI_TMP=/data/grebi_tmp
export GREBI_CONFIG=hett_only
export GREBI_IS_EBI=false
export GREBI_TIMESTAMP=$(date +%Y_%m_%d__%H_%M)
cd $GREBI_TMP
export PYTHONUNBUFFERED=true
rm -rf work tmp
python3 ${GREBI_HOME}/scripts/dataload_local.py



