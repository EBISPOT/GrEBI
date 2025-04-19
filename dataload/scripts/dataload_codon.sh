#!/bin/bash

if [ -z "$GREBI_SUBGRAPH" ]; then
  echo "Set GREBI_SUBGRAPH to run this script"
  exit 1
fi

export GREBI_DATALOAD_HOME=/nfs/production/parkinso/spot/grebi/dataload
export GREBI_QUERY_YAMLS_PATH=/nfs/production/parkinso/spot/grebi/materialised_queries
export GREBI_OUT_DIR=/hps/nobackup/parkinso/spot/grebi/$GREBI_SUBGRAPH/out
export GREBI_IS_EBI=true
export GREBI_NEXTFLOW_CONFIG=$GREBI_DATALOAD_HOME/nextflow/codon_nextflow.config
module load nextflow/24.10.3


srun --time 1:0:0 --mem 4g mkdir -p $GREBI_OUT_DIR

srun --time 6-0:0:0 --mem 32g nextflow $GREBI_DATALOAD_HOME/nextflow/load_subgraph.nf -c $GREBI_NEXTFLOW_CONFIG -resume


