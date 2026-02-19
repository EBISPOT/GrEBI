

from shared import config

config['id'] = 'HETT'
config['name'] = 'EMBL Human Ecosystems'
config['datasource_configs'] = [
    "./configs/datasource_configs/hett_pesticides_appril.yaml",
    "./configs/datasource_configs/hett_pesticides_eu.yaml",
    "./configs/datasource_configs/hett_pesticides_gb.yaml",
    "./configs/datasource_configs/aopwiki.yaml",
    "./configs/datasource_configs/chembl.yaml",
    # OLS ontologies for HETT (split for parallel processing)
    "./configs/datasource_configs/ols_cheminf.yaml",
    "./configs/datasource_configs/ols_chebi.yaml",
    "./configs/datasource_configs/ols_edam.yaml",
    "./configs/datasource_configs/ols_dc.yaml",
    "./configs/datasource_configs/ols_dcterms.yaml",
    "./configs/datasource_configs/ols_go.yaml",
    "./configs/datasource_configs/ols_pato.yaml",
    "./configs/datasource_configs/ols_ncit.yaml",
    "./configs/datasource_configs/ols_ro.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
