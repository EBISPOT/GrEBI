#!/bin/bash
#$ -pe smp 1
#$ -l h_vmem=1G
#$ -l h_rt=1:0:0
#$ -cwd
#$ -j y
#$ -m e

set -e

curl -O https://ftp.ebi.ac.uk/pub/databases/spot/ols/latest/ontologies_linked.json.gz