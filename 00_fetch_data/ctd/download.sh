#!/bin/bash

wget https://ctdbase.org/reports/CTD_anatomy.tsv.gz
wget https://ctdbase.org/reports/CTD_curated_genes_diseases.tsv.gz
wget https://ctdbase.org/reports/CTD_Disease-GO_biological_process_associations.tsv.gz
wget https://ctdbase.org/reports/CTD_Disease-GO_cellular_component_associations.tsv.gz
wget https://ctdbase.org/reports/CTD_Disease-GO_molecular_function_associations.tsv.gz
wget https://ctdbase.org/reports/CTD_exposure_events.tsv.gz
wget https://ctdbase.org/reports/CTD_exposure_studies.tsv.gz
wget https://ctdbase.org/reports/CTD_pheno_term_ixns.tsv.gz
wget https://ctdbase.org/reports/CTD_Phenotype-Disease_biological_process_associations.tsv.gz
wget https://ctdbase.org/reports/CTD_Phenotype-Disease_cellular_component_associations.tsv.gz
wget https://ctdbase.org/reports/CTD_Phenotype-Disease_molecular_function_associations.tsv.gz
wget https://ctdbase.org/reports/CTD_chemicals.tsv.gz
wget https://ctdbase.org/reports/CTD_chem_go_enriched.tsv.gz
wget https://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz
wget https://ctdbase.org/reports/CTD_chem_gene_ixns.tsv.gz
wget https://ctdbase.org/reports/CTD_chem_pathways_enriched.tsv.gz
wget https://ctdbase.org/reports/CTD_curated_cas_nbrs.tsv.gz
wget https://ctdbase.org/reports/CTD_diseases_pathways.tsv.gz
wget https://ctdbase.org/reports/CTD_UniProtToCTDIdMapping.txt.gz
wget https://ctdbase.org/reports/CTD_genes.tsv.gz
wget https://ctdbase.org/reports/CTD_genes_diseases.tsv.gz
wget https://ctdbase.org/reports/CTD_genes_pathways.tsv.gz
wget https://ctdbase.org/reports/CTD_pathways.tsv.gz


wget https://ctdbase.org/reports/CTD_chem_gene_ixn_types.obo
curl https://ctdbase.org/reports/CTD_diseases.obo.gz | gzip -d > CTD_diseases.obo

