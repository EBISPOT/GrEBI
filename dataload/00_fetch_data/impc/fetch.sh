#!/bin/bash

# I can automate this but for now let's keep it static
# https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/

urls=(
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/mouse_gene_json/part-00000-4c137d95-3db2-4291-a12b-4acd76cf8055-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/embryo_specimen_json/part-00000-638978ee-7aaf-4390-b0d3-5167b048f784-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/gene_phenotype_association_json/part-00000-c9880b4c-69b7-4c0e-a537-07046ca62653-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/image_record_observation_json/part-00000-f8879bb9-1c2d-4178-9d1d-043005ae7857-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/line_experiment_json/part-00000-7eead2e1-c7a9-47c6-9903-087707e2ee07-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/mouse_specimen_json/part-00000-febeef41-369d-4b5d-98a5-820dbb5901f6-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/ontological_observation_json/part-00000-7e5d999e-b172-4f75-ab6e-13069e0b8aaa-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/parameter_json/part-00000-d6151a7d-dec6-4094-8b68-83f94656a7f6-c000.json"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/pipeline_json/part-00000-4bedf22f-44dc-481a-bf8b-17b75e110c13-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/procedure_json/part-00000-7edac0eb-bb3d-49f0-9d63-48bc1c8796d1-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/text_observation_json/part-00000-28b89d18-6337-4fcc-8e57-5c07c9c84ede-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/human_gene_json/part-00000-61780d0c-199e-49be-a050-939d4b953b5e-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/publications_json/part-00000-1288a7c8-e5d7-4bb9-b63a-45d4885faa1e-c000.json.gz"
    "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/impc-kg/mouse_allele_json/part-00000-48c4e449-5be0-4da0-a0b2-01f3f92c654b-c000.json.gz"
    

)

for url in "${urls[@]}"; 
do echo "Downloading ${url}..." 
curl -O "$url"
done

# TODO: Zip the only missing one? 


