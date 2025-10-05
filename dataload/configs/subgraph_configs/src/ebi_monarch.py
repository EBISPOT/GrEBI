
from shared import config

config['id'] = 'EBI_MONARCH'
config['name'] = 'EBI Resources and MONARCH Initiative KG'
config['datasource_configs'] = [
    "./configs/datasource_configs/gwas.yaml",
    "./configs/datasource_configs/hgnc.yaml",
    "./configs/datasource_configs/impc.yaml",
    "./configs/datasource_configs/sssom.yaml",
    "./configs/datasource_configs/ols.yaml",
    "./configs/datasource_configs/reactome.yaml",
    "./configs/datasource_configs/ubergraph.yaml",
    "./configs/datasource_configs/otar_evidence.yaml",
    "./configs/datasource_configs/otar_drug_indication.yaml",
    "./configs/datasource_configs/otar_drug_molecule.yaml",
    "./configs/datasource_configs/otar_disease_phenotype.yaml",
    "./configs/datasource_configs/monarch.yaml",
    "./configs/datasource_configs/metabolights.yaml",
    "./configs/datasource_configs/mondo_efo.yaml",
    "./configs/datasource_configs/ctd.yaml",
    "./configs/datasource_configs/mgnify.yaml",
    "./configs/datasource_configs/hett_pesticides_appril.yaml",
    "./configs/datasource_configs/hett_pesticides_eu.yaml",
    "./configs/datasource_configs/hett_pesticides_gb.yaml",
    "./configs/datasource_configs/aopwiki.yaml",
    "./configs/datasource_configs/chembl.yaml",
    "./configs/datasource_configs/robokop_alliance.yaml",
    "./configs/datasource_configs/robokop_binding.yaml",
    "./configs/datasource_configs/robokop_cam.yaml",
    "./configs/datasource_configs/robokop_drugcentral.yaml",
    "./configs/datasource_configs/robokop_gtex.yaml",
    "./configs/datasource_configs/robokop_gtopdb.yaml",
    "./configs/datasource_configs/robokop_hetionet.yaml",
    "./configs/datasource_configs/robokop_hgoa.yaml",
    "./configs/datasource_configs/robokop_hmdb.yaml",
    "./configs/datasource_configs/robokop_icees.yaml",
    "./configs/datasource_configs/robokop_intact.yaml",
    "./configs/datasource_configs/robokop_panther.yaml",
    "./configs/datasource_configs/robokop_pharos.yaml",
    "./configs/datasource_configs/robokop_string.yaml",
    "./configs/datasource_configs/robokop_textmining.yaml",
    "./configs/datasource_configs/robokop_viralproteome.yaml",
    "./configs/datasource_configs/mesh.yaml",
    "./configs/datasource_configs/primekg.yaml",
    "./configs/datasource_configs/ols_top_k.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))

