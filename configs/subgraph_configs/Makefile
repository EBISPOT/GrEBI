

all: ebi_monarch_xspecies.json ebi_monarch.json hett.json hra_kg.json impc.json monarch.json impc_x_gwas.json gwas_and_efo.json

%.json: src/%.py
	python3 $< > $@

.PHONY: all
