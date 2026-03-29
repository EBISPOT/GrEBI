
# Downloads \& Ingests

GrEBI datasources are defined in YAML files in the [`configs/datasource_configs`](https://github.com/EBISPOT/GrEBI/tree/dev/configs/datasource_configs) directory. For example, [`otar_disease_phenotype.yaml`](https://github.com/EBISPOT/GrEBI/blob/dev/configs/datasource_configs/otar/otar_disease_phenotype.yaml) defines a datasource to import disease-phenotype associations from the Open Targets platform:

```yaml
id: OpenTargets
enabled: true
description: "Disease-to-phenotype associations derived from the Open Targets evidence pipeline"
download:
- dest: otar/disease_phenotype/
    sources:
    - https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/output/disease_phenotype/disease_phenotype.parquet
ingests:
- globs: ["otar/disease_phenotype/*.parquet"]
    command: '
    cat $GREBI_INGEST_FILENAME |
    grebi_parquet2jsonl |
    grebi_nodes2edges --from-field disease --to-field phenotype --edge-type biolink:has_phenotype |
    grebi_transform_jsonl
        --json-inject-type biolink:Disease
        --json-inject-key-prefix otar:
        '
```

The `download` section defines which files are needed, and where to download them. Each file can have multiple sources and the pipeline will try each source in turn (for example, many of the datasources have both an NFS path used on the EBI HPC and a URL to fall back on when running elsewhere).

The `ingests` section defines preprocessing needed before the file is loaded into GrEBI.




