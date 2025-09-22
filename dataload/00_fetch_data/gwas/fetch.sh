#!/bin/bash

# Files to download
    #   - "dataload/00_fetch_data/gwas/gwas-catalog-associations_ontology-annotated.tsv.gz"
    #   - "dataload/00_fetch_data/gwas/gwas-catalog-studies-download-alternative-v1.0.2.1.txt.gz"

# wget -O gwas-catalog-associations_ontology-annotated.tsv.gz --compression=gzip https://www.ebi.ac.uk/gwas/api/search/downloads/alternative
# wget -O gwas-catalog-studies-download-alternative-v1.0.2.1.txt.gz --compression=gzip https://www.ebi.ac.uk/gwas/api/search/downloads/studies/v1.0.2.1

echo "Downloading gwas-catalog-associations_ontology-annotated.tsv.gz"
curl -s https://www.ebi.ac.uk/gwas/api/search/downloads/alternative | gzip > gwas-catalog-associations_ontology-annotated.tsv.gz
# NOTE: Use older version to fit the current contenerised code. From after 2024/10/10 GWAS added a new column: GXE. 
# curl -s https://ftp.ebi.ac.uk/pub/databases/gwas/releases/2024/10/10/gwas-catalog-associations_ontology-annotated.tsv | gzip > gwas-catalog-associations_ontology-annotated.tsv.gz


echo "Downloading gwas-catalog-studies-download-alternative-v1.0.2.1.txt.gz"
curl -s https://www.ebi.ac.uk/gwas/api/search/downloads/studies/v1.0.2.1 | gzip > gwas-catalog-studies-download-alternative-v1.0.2.1.txt.gz
# curl -s https://ftp.ebi.ac.uk/pub/databases/gwas/releases/2024/10/10/gwas-catalog-studies-download-alternative-v1.0.2.1.txt	| gzip > gwas-catalog-studies-download-alternative-v1.0.2.1.txt.gz

