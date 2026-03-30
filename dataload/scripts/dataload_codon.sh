#!/bin/bash

if [ -z "$GREBI_SUBGRAPHS" ]; then
  echo "Set GREBI_SUBGRAPHS (comma-separated) to run this script"
  exit 1
fi

# Use a combined directory name from all subgraphs (replace commas with underscores)
GREBI_DIR_NAME=$(echo "$GREBI_SUBGRAPHS" | tr ',' '_')

export GREBI_HOME=/nfs/production/parkinso/spot/grebi
export GREBI_DATALOAD_HOME=$GREBI_HOME/dataload
export GREBI_QUERY_YAMLS_PATH=$GREBI_HOME/materialised_queries
export GREBI_OUT_DIR=/hps/nobackup/parkinso/spot/grebi/$GREBI_DIR_NAME/out
export GREBI_DOWNLOADS_PATH=${GREBI_DOWNLOADS_PATH:-/hps/nobackup/parkinso/spot/grebi/$GREBI_DIR_NAME/downloads}
export GREBI_IS_EBI=true
export GREBI_NEXTFLOW_CONFIG=$GREBI_DATALOAD_HOME/nextflow/codon_nextflow.config
export NXF_WORK=/hps/nobackup/parkinso/spot/grebi/$GREBI_DIR_NAME/NXF_WORK
export NXF_HOME=/hps/nobackup/parkinso/spot/grebi/$GREBI_DIR_NAME/NXF_HOME
export NXF_TEMP=/hps/nobackup/parkinso/spot/grebi/$GREBI_DIR_NAME/NXF_TEMP
export NXF_CACHE_DIR=/hps/nobackup/parkinso/spot/grebi/$GREBI_DIR_NAME/NXF_CACHE_DIR
export NXF_SINGULARITY_CACHEDIR=/hps/nobackup/parkinso/spot/grebi/$GREBI_DIR_NAME/NXF_SINGULARITY_CACHEDIR
module load nextflow/24.10.3


export REPORTS_DIR=$GREBI_OUT_DIR/reports

srun --time 1:0:0 --mem 4g mkdir -p $GREBI_OUT_DIR $NXF_HOME $NXF_WORK $NXF_TEMP $NXF_CACHE_DIR $NXF_SINGULARITY_CACHEDIR $REPORTS_DIR

srun --time 6-0:0:0 --mem 32g nextflow $GREBI_DATALOAD_HOME/nextflow/main.nf -c $GREBI_NEXTFLOW_CONFIG -resume \
  -with-report $REPORTS_DIR/report.html \
  -with-trace $REPORTS_DIR/trace.txt \
  -with-timeline $REPORTS_DIR/timeline.html \
  -with-dag $REPORTS_DIR/dag.html


