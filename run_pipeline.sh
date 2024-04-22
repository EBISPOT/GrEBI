#!/bin/bash

export PYTHONUNBUFFERED=TRUE

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <CONFIG_NAME>"
    exit 1
fi

CONFIG_NAME=$1

rm -rf /nfs/production/parkinso/spot/grebi/tmp/$CONFIG_NAME/*
srun -t 2:0:0 --mem=2G rm -rf /hps/nobackup/parkinso/spot/grebi/tmp/$CONFIG_NAME/*

cd /nfs/production/parkinso/spot/grebi

python3 ./dataload.py ./configs/pipeline_configs/$CONFIG_NAME.json
python3 05_materialize_edges/grebi_rocks2neo.py ./configs/pipeline_configs/$CONFIG_NAME.json
python3 06_create_db/neo4j/neo4j_import.py ./configs/pipeline_configs/$CONFIG_NAME.json

echo $(date): Compressing neo4j data

srun -t 2:0:0 --mem=2G tar -cf  \
    /nfs/production/parkinso/spot/grebi/tmp/$CONFIG_NAME.tgz \
    --use-compress-program="pigz --fast" \
    -C /hps/nobackup/parkinso/spot/grebi/tmp/$CONFIG_NAME/06_create_db/neo4j \
    data
    

echo $(date): Copying to FTP

srun --partition=datamover -t 1:30:00 --mem=5G \
    cp -f /nfs/production/parkinso/spot/grebi/tmp/$CONFIG_NAME.tgz /nfs/ftp/public/databases/spot/kg/

echo $(date): Done


