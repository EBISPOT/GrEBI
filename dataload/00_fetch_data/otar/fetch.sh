#!/bin/bash
#$ -pe smp 4
#$ -l h_vmem=6G
#$ -l h_rt=1:0:0
#$ -cwd
#$ -j y
#$ -m e

set -e

# TODO: all of this expects json format data. Parquet is available. Is it worth switching for efficiency?

echo "Downloading Open Targets data..."
# Download all data in json format for: 
# targets
# rsync -rpltvz --delete rsync.ebi.ac.uk::pub/databases/opentargets/platform/25.06/output/target .
# disease
# rsync -rpltvz --delete rsync.ebi.ac.uk::pub/databases/opentargets/platform/25.06/output/disease .
# molecule
# rsync -rpltvz --delete rsync.ebi.ac.uk::pub/databases/opentargets/platform/25.06/output/drug_molecule .
# evidence 
rsync -rpltvz --delete rsync.ebi.ac.uk::pub/databases/opentargets/platform/25.06/output/evidence .

echo "Done"
# Create directories to store each data
# Then keep those on json format only and compress. 

echo "Converting to jsonl format..." 
/data/WHRI-Phenogenomics/software/bin/duckdb/duckdb <<EOF
    PRAGMA threads=4;
    PRAGMA memory_limit='24GB';
    COPY (SELECT * FROM read_parquet('evidence/*/*.parquet')) to 'evidence.jsonl' (FORMAT JSON);
EOF
echo "Done"

module load pigz
echo "Compressing..."
pigz -p 4 --fast evidence.jsonl
echo "Done"

