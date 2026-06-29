#!/bin/bash

if [ -z "$GREBI_SUBGRAPHS" ]; then
  echo "Set GREBI_SUBGRAPHS (comma-separated) to run this script"
  exit 1
fi

export GREBI_HOME=${GREBI_HOME:-/nfs/production/parkinso/spot/grebi}
export GREBI_NEXTFLOW_CONFIG=$GREBI_HOME/dataload/nextflow/download_codon.config
module load nextflow/24.10.3

BASE_DIR=${GREBI_NOBACKUP:-/hps/nobackup/parkinso/spot/grebi}

IFS=',' read -ra SUBGRAPH_ARRAY <<< "$GREBI_SUBGRAPHS"
for sg in "${SUBGRAPH_ARRAY[@]}"; do
  export GREBI_SUBGRAPH="$sg"
  export GREBI_DOWNLOADS_PATH=$BASE_DIR/downloads/$sg
  export NXF_WORK=$BASE_DIR/download_${sg}__NXF_WORK
  export NXF_HOME=$BASE_DIR/download_${sg}__NXF_HOME
  export NXF_TEMP=$BASE_DIR/download_${sg}__NXF_TEMP
  export NXF_CACHE_DIR=$BASE_DIR/download_${sg}__NXF_CACHE_DIR
  export NXF_SINGULARITY_CACHEDIR=$BASE_DIR/download_${sg}__NXF_SINGULARITY_CACHEDIR
  export REPORTS_DIR=$BASE_DIR/out/reports_download_${sg}

  srun --time 1:0:0 --mem 4g mkdir -p $GREBI_DOWNLOADS_PATH $NXF_HOME $NXF_WORK $NXF_TEMP $NXF_CACHE_DIR $NXF_SINGULARITY_CACHEDIR $REPORTS_DIR

  srun --time 6-0:0:0 --mem 8g nextflow $GREBI_HOME/dataload/nextflow/download.nf -c $GREBI_NEXTFLOW_CONFIG -resume \
    -with-report $REPORTS_DIR/report.html \
    -with-trace $REPORTS_DIR/trace.txt \
    -with-timeline $REPORTS_DIR/timeline.html \
    -with-dag $REPORTS_DIR/dag.html
done
