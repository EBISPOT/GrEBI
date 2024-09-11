#!/bin/bash

rm -f *.sssom.tsv.gz

curl -L https://data.monarchinitiative.org/mappings/latest/gene_mappings.sssom.tsv | gzip > gene_mappings.sssom.tsv.gz
curl -L https://data.monarchinitiative.org/mappings/latest/hp_mesh.sssom.tsv | gzip > hp_mesh.sssom.tsv.gz
curl -L https://data.monarchinitiative.org/mappings/latest/mesh_chebi_biomappings.sssom.tsv | gzip > mesh_chebi_biomappings.sssom.tsv.gz
curl -L https://data.monarchinitiative.org/mappings/latest/mondo.sssom.tsv | gzip > mondo.sssom.tsv.gz
curl -L https://data.monarchinitiative.org/mappings/latest/umls_hp.sssom.tsv | gzip > umls_hp.sssom.tsv.gz
curl -L https://data.monarchinitiative.org/mappings/latest/upheno_custom.sssom.tsv | gzip > upheno_custom.sssom.tsv.gz

curl -L https://raw.githubusercontent.com/mapping-commons/mh_mapping_initiative/master/mappings/mp_hp_mgi_all.sssom.tsv | gzip > mp_hp_mgi_all.sssom.tsv.gz
curl -L https://raw.githubusercontent.com/obophenotype/bio-attribute-ontology/master/src/mappings/oba-efo.sssom.tsv | gzip > oba-efo.sssom.tsv.gz
curl -L https://raw.githubusercontent.com/obophenotype/bio-attribute-ontology/master/src/mappings/oba-vt.sssom.tsv | gzip > oba-vt.sssom.tsv.gz
