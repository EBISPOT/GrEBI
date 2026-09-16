

all: ebi_monarch_xspecies.json ebi_monarch.json hett.json hra_kg.json impc.json monarch.json impc_x_gwas.json gwas_and_efo.json disease_ontologies.json ubergraph_efo_mondo.json test_pdbe.json test_gwas.json test_biostudies.json pride.json test_pride.json expression_atlas.json test_expression_atlas.json

%.json: src/%.py
	python3 $< > $@

.PHONY: all
