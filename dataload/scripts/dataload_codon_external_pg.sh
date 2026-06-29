#!/bin/bash

if [ -z "$GREBI_SUBGRAPHS" ]; then
  echo "Set GREBI_SUBGRAPHS (comma-separated) to run this script"
  exit 1
fi

if [ -z "$PGPASSWORD" ]; then
  echo "Set PGPASSWORD before running this script"
  exit 1
fi

export GREBI_HOME=${GREBI_HOME:-/nfs/production/parkinso/spot/grebi}
export GREBI_DATALOAD_HOME=$GREBI_HOME/dataload
export GREBI_QUERY_YAMLS_PATH=$GREBI_HOME/materialised_queries
export GREBI_NOBACKUP=${GREBI_NOBACKUP:-/hps/nobackup/parkinso/spot/grebi}
export GREBI_OUT_DIR=$GREBI_NOBACKUP/out
export GREBI_DOWNLOADS_PATH=${GREBI_DOWNLOADS_PATH:-$GREBI_NOBACKUP/downloads}
export GREBI_IS_EBI=true
export GREBI_NEXTFLOW_CONFIG=$GREBI_DATALOAD_HOME/nextflow/codon_nextflow.config
export NXF_WORK=$GREBI_NOBACKUP/NXF_WORK
export NXF_HOME=$GREBI_NOBACKUP/NXF_HOME
export NXF_TEMP=$GREBI_NOBACKUP/NXF_TEMP
export NXF_CACHE_DIR=$GREBI_NOBACKUP/NXF_CACHE_DIR
export NXF_SINGULARITY_CACHEDIR=$GREBI_NOBACKUP/NXF_SINGULARITY_CACHEDIR
module load nextflow/24.10.3

# External PostgreSQL connection
export PGHOST=${PGHOST:-pgsql-hlvm-138}
export PGPORT=${PGPORT:-5432}
export PGDATABASE=${PGDATABASE:-spotefoexp}
export PGUSER=${PGUSER:-spot}
# PGPASSWORD is set by the caller

export REPORTS_DIR=$GREBI_OUT_DIR/reports

srun --time 1:0:0 --mem 4g mkdir -p $GREBI_OUT_DIR $NXF_HOME $NXF_WORK $NXF_TEMP $NXF_CACHE_DIR $NXF_SINGULARITY_CACHEDIR $REPORTS_DIR

srun --time 6-0:0:0 --mem 32g nextflow $GREBI_DATALOAD_HOME/nextflow/main.nf -c $GREBI_NEXTFLOW_CONFIG -resume \
  --external_postgres true \
  -with-report $REPORTS_DIR/report.html \
  -with-trace $REPORTS_DIR/trace.txt \
  -with-timeline $REPORTS_DIR/timeline.html \
  -with-dag $REPORTS_DIR/dag.html
