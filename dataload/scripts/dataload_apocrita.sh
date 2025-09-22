#!/bin/bash
#$ -pe smp 4
#$ -l h_vmem=8G
#$ -l h_rt=240:0:0
#$ -cwd
#$ -j y
#$ -m e

set -e

# Set home for project (experimental)
export GREBI_HOME=/data/scratch/hhz161/data

# Set the subgraph to build
export GREBI_SUBGRAPH=impc_x_gwas



# Set export paths for the pipeline
export GREBI_DATALOAD_HOME=/data/scratch/hhz161/data/GrEBI/dataload
export GREBI_QUERY_YAMLS_PATH=/data/scratch/hhz161/data/GrEBI/materialised_queries
export GREBI_OUT_DIR=/data/scratch/hhz161/data/GrEBI/$GREBI_SUBGRAPH/out
export GREBI_IS_EBI=false
export GREBI_NEXTFLOW_CONFIG=$GREBI_DATALOAD_HOME/nextflow/apocrita_nextflow.config

# Export target/release/executables 
export PATH=/$GREBI_DATALOAD_HOME/target/release:$PATH

# Set singulairty cache directory
export NXF_SINGULARITY_CACHEDIR=/data/scratch/hhz161/data/GrEBI/$GREBI_SUBGRAPH/singularity_cache


# Load dependencies
module load nextflow
# NOTE: This is only necessary for dataload (because we made changes to the rust scripts)
module load rust
module load cmake
module load sqlite
# # Beware half the pipeline was executed using pigz 2.8. Downgraded to 2.5 due to conflicts with python 3.11
module load pigz
# module load python/3.11
# source ../.venv/bin/activate

# Create an output directory
## srun --time 1:0:0 --mem 4g mkdir -p $GREBI_OUT_DIR
# mkdir -p $GREBI_HOME
# mkdir -p $GREBI_OUT_DIR

# Run the subgraph creation
##srun --time 6-0:0:0 --mem 32g nextflow $GREBI_DATALOAD_HOME/nextflow/load_subgraph.nf -c $GREBI_NEXTFLOW_CONFIG -resume

# Command to execute with nextflow config --> container
# nextflow $GREBI_DATALOAD_HOME/nextflow/load_subgraph.nf -c $GREBI_NEXTFLOW_CONFIG -resume

# Run for verbose logging
# NXF_OPTS='-Dcapsule.log=verbose' nextflow $GREBI_DATALOAD_HOME/nextflow/load_subgraph.nf -c $GREBI_NEXTFLOW_CONFIG -resume

# Comamand to execute without nextflow config --> no container. 
nextflow $GREBI_DATALOAD_HOME/nextflow/load_subgraph.nf -resume
