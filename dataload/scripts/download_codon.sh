#!/bin/bash

if [ -z "$GREBI_SUBGRAPH" ]; then
  echo "Set GREBI_SUBGRAPH to run this script"
  exit 1
fi

export GREBI_HOME=/nfs/production/parkinso/spot/grebi
export GREBI_DOWNLOADS_PATH=/hps/nobackup/parkinso/spot/grebi/$GREBI_SUBGRAPH/downloads
export GREBI_NEXTFLOW_CONFIG=$GREBI_HOME/dataload/nextflow/download_codon.config
export NXF_WORK=/hps/nobackup/parkinso/spot/grebi/${GREBI_SUBGRAPH}_download/NXF_WORK
export NXF_HOME=/hps/nobackup/parkinso/spot/grebi/${GREBI_SUBGRAPH}_download/NXF_HOME
export NXF_TEMP=/hps/nobackup/parkinso/spot/grebi/${GREBI_SUBGRAPH}_download/NXF_TEMP
export NXF_CACHE_DIR=/hps/nobackup/parkinso/spot/grebi/${GREBI_SUBGRAPH}_download/NXF_CACHE_DIR
export NXF_SINGULARITY_CACHEDIR=/hps/nobackup/parkinso/spot/grebi/${GREBI_SUBGRAPH}_download/NXF_SINGULARITY_CACHEDIR
module load nextflow/24.10.3

srun --time 1:0:0 --mem 4g mkdir -p $GREBI_DOWNLOADS_PATH $NXF_HOME $NXF_WORK $NXF_TEMP $NXF_CACHE_DIR $NXF_SINGULARITY_CACHEDIR

srun --time 6-0:0:0 --mem 8g nextflow $GREBI_HOME/dataload/nextflow/download.nf -c $GREBI_NEXTFLOW_CONFIG -resume
