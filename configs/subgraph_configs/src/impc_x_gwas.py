

from shared import config

config['id'] = 'IMPC x GWAS'
config['name'] = 'IMPC and the GWAS Catalog'
config['datasource_configs'] = [
    "./configs/datasource_configs/impc.yaml",
    "./configs/datasource_configs/gwas.yaml",
    # OLS ontologies for IMPC x GWAS (split for parallel processing)
    "./configs/datasource_configs/ols/ols_efo.yaml",
    "./configs/datasource_configs/ols/ols_mp.yaml",
    "./configs/datasource_configs/ols/ols_hp.yaml",
    "./configs/datasource_configs/ols/ols_ro.yaml",
    "./configs/datasource_configs/ols/ols_iao.yaml",
    "./configs/datasource_configs/ols/ols_uberon.yaml",
    "./configs/datasource_configs/ols/ols_pato.yaml",
    "./configs/datasource_configs/ols/ols_oba.yaml",
    "./configs/datasource_configs/ols/ols_bspo.yaml",
    "./configs/datasource_configs/ols/ols_obi.yaml",
    "./configs/datasource_configs/ols/ols_bfo.yaml",
    "./configs/datasource_configs/ols/ols_cob.yaml",
    "./configs/datasource_configs/ols/ols_oio.yaml",
    "./configs/datasource_configs/ols/ols_biolink.yaml",
    "./configs/datasource_configs/ols/ols_mondo.yaml",
    "./configs/datasource_configs/ols/ols_doid.yaml",
    "./configs/datasource_configs/ols/ols_dc.yaml",
    "./configs/datasource_configs/ols/ols_dcterms.yaml",
    "./configs/datasource_configs/ols/ols_ncit.yaml",
    "./configs/datasource_configs/ols/ols_edam.yaml",
    "./configs/datasource_configs/ols/ols_upheno.yaml",
    "./configs/datasource_configs/mondo_efo.yaml",
    "./configs/datasource_configs/monarch.yaml",
    "./configs/datasource_configs/sssom.yaml",
    "./configs/datasource_configs/otar.yaml",
    "./configs/datasource_configs/ols_top_k.yaml"
]

if __name__ == '__main__':
    import json
    print(json.dumps(config, indent=2))
