#!/bin/bash

rm -f *.sssom.tsv.gz

curl -L https://data.monarchinitiative.org/mappings/latest/upheno_custom.sssom.tsv | gzip > upheno_custom.sssom.tsv.gz
curl -L https://raw.githubusercontent.com/mapping-commons/mh_mapping_initiative/master/mappings/mp_hp_mgi_all.sssom.tsv | gzip > mp_hp_mgi_all.sssom.tsv.gz
curl -L https://raw.githubusercontent.com/obophenotype/bio-attribute-ontology/master/src/mappings/oba-efo.sssom.tsv | gzip > oba-efo.sssom.tsv.gz
curl -L https://raw.githubusercontent.com/obophenotype/bio-attribute-ontology/master/src/mappings/oba-vt.sssom.tsv | gzip > oba-vt.sssom.tsv.gz
