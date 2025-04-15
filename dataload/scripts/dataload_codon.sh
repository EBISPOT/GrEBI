#!/bin/bash

if [ -z "$GREBI_SUBGRAPH" ]; then
  echo "Set GREBI_SUBGRAPH to run this script"
  exit 1
fi

export GREBI_QUERY_YAMLS_PATH=/nfs/production/parkinso/spot/grebi/materialised_queries
export GREBI_OUT_DIR=/hps/nobackup/parkinso/spot/grebi/$GREBI_SUBGRAPH/out
export GREBI_CONFIG=ebi
export GREBI_IS_EBI=true
export GREBI_TIMESTAMP=$(date +"%Y-%b-%d")
export GREBI_MAX_ENTITIES=1000000000
export GREBI_NEXTFLOW_CONFIG=$GREBI_DATALOAD_HOME/nextflow/codon_nextflow.config
module load nextflow-22.10.1-gcc-11.2.0-ju5saqw
module load python-3.10.2-gcc-9.3.0-gswnsij
source /nfs/production/parkinso/spot/grebi/.venv/bin/activate
export PYTHONUNBUFFERED=true

srun --time 1:0:0 --mem 4g mkdir -p /hps/nobackup/parkinso/spot/grebi/$GREBI_SUBGRAPH
srun --time 1:0:0 --mem 4g mkdir -p $GREBI_OUT_DIR

cd /hps/nobackup/parkinso/spot/grebi/$GREBI_SUBGRAPH

echo "Loading subgraph $GREBI_SUBGRAPH"
echo "pwd is $(pwd)"
echo "user is $(whoami)"

srun --time 6-0:0:0 --mem 32g nextflow $GREBI_DATALOAD_HOME/nextflow/load_subgraph.nf -c $GREBI_NEXTFLOW_CONFIG -resume


