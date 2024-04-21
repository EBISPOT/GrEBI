#!/bin/bash

rm -rf targets diseases molecule evidence

srun --partition=datamover -t 1:30:00 --mem=5G --pty bash -c "\
    cp -r /nfs/ftp/public/databases/opentargets/platform/24.03/output/etl/json/targets . && \
    cp -r /nfs/ftp/public/databases/opentargets/platform/24.03/output/etl/json/diseases . && \
    cp -r /nfs/ftp/public/databases/opentargets/platform/24.03/output/etl/json/molecule . && \
    cp -r /nfs/ftp/public/databases/opentargets/platform/24.03/output/etl/json/evidence ." 

cat $(find targets -type f -name "*.json") | pigz --fast > targets.jsonl.gz
cat $(find diseases -type f -name "*.json") | pigz --fast > diseases.jsonl.gz
cat $(find molecule -type f -name "*.json") | pigz --fast > molecule.jsonl.gz
cat $(find evidence -type f -name "*.json") | pigz --fast > evidence.jsonl.gz
