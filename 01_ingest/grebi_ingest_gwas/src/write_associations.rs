
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::{json, Value};

use crate::check_headers::check_headers;
use crate::remove_empty_fields::remove_empty_fields;

pub fn write_associations(csv_reader: &mut csv::Reader<BufReader<StdinLock>>,nodes_writer: &mut BufWriter<StdoutLock>, datasource_name: &str) {
    {
        let headers = csv_reader.headers().unwrap();

        check_headers(headers.into_iter().collect(), vec![
            "DATE ADDED TO CATALOG",
            "PUBMEDID",
            "FIRST AUTHOR",
            "DATE",
            "JOURNAL",
            "LINK",
            "STUDY",
            "DISEASE/TRAIT",
            "INITIAL SAMPLE SIZE",
            "REPLICATION SAMPLE SIZE",
            "REGION",
            "CHR_ID",
            "CHR_POS",
            "REPORTED GENE(S)",
            "MAPPED_GENE",
            "UPSTREAM_GENE_ID",
            "DOWNSTREAM_GENE_ID",
            "SNP_GENE_IDS",
            "UPSTREAM_GENE_DISTANCE",
            "DOWNSTREAM_GENE_DISTANCE",
            "STRONGEST SNP-RISK ALLELE",
            "SNPS",
            "MERGED",
            "SNP_ID_CURRENT",
            "CONTEXT",
            "INTERGENIC",
            "RISK ALLELE FREQUENCY",
            "P-VALUE",
            "PVALUE_MLOG",
            "P-VALUE (TEXT)",
            "OR or BETA",
            "95% CI (TEXT)",
            "PLATFORM [SNPS PASSING QC]",
            "CNV",
            "MAPPED_TRAIT",
            "MAPPED_TRAIT_URI",
            "STUDY ACCESSION",
            "GENOTYPING TECHNOLOGY"
        ]);
    }

    let middle_json_fragment
         = [r#"","datasource":""#.as_bytes(), datasource_name.as_bytes(), r#"","properties":"#.as_bytes() ].concat();

    {
        for record in csv_reader.records() {

            let record = record.unwrap();


            let date_added_to_catalog = record.get(0).unwrap();
            let pubmedid = record.get(1).unwrap();
            let first_author = record.get(2).unwrap();
            let date = record.get(3).unwrap();
            let journal = record.get(4).unwrap();
            let link = record.get(5).unwrap();
            let study = record.get(6).unwrap();
            let disease_trait = record.get(7).unwrap();
            let initial_sample_size = record.get(8).unwrap();
            let replication_sample_size = record.get(9).unwrap();
            let region = record.get(10).unwrap();
            let chr_id = record.get(11).unwrap();
            let chr_pos = record.get(12).unwrap();
            let reported_genes = record.get(13).unwrap();
            let mapped_gene = record.get(14).unwrap();
            let upstream_gene_id = record.get(15).unwrap();
            let downstream_gene_id = record.get(16).unwrap();
            let snp_gene_ids = record.get(17).unwrap();
            let upstream_gene_distance = record.get(18).unwrap();
            let downstream_gene_distance = record.get(19).unwrap();
            let strongest_snp_risk_allele = record.get(20).unwrap();
            let snps = record.get(21).unwrap();
            let merged = record.get(22).unwrap();
            let snp_id_current = record.get(23).unwrap();
            let context = record.get(24).unwrap();
            let intergenic = record.get(25).unwrap();
            let risk_allele_frequency = record.get(26).unwrap();
            let p_value = record.get(27).unwrap();
            let pvalue_mlog = record.get(28).unwrap();
            let p_value_text = record.get(29).unwrap();
            let or_or_beta = record.get(30).unwrap();
            let ci_text = record.get(31).unwrap();
            let platform = record.get(32).unwrap();
            let cnv = record.get(33).unwrap();
            let mapped_trait = record.get(34).unwrap();
            let mapped_trait_uri = record.get(35).unwrap();
            let study_accession = record.get(36).unwrap();
            let genotyping_technology = record.get(37).unwrap();

            nodes_writer.write_all(r#"{"subject":""#.as_bytes()).unwrap();
            nodes_writer.write_all(snps.as_bytes()).unwrap();
            nodes_writer.write_all(&middle_json_fragment).unwrap();

            nodes_writer.write_all(remove_empty_fields(& json!(
                {
                "grebi:type": ["gwas:SNP"],
                "rdf:type": ["so:0000694"], // SNP
                "gwas:mapped_gene": [mapped_gene],
                "gwas:upstream_gene_id": [upstream_gene_id],
                "gwas:downstream_gene_id": [downstream_gene_id],
                "gwas:snp_gene_ids": [snp_gene_ids],
                "gwas:associated_with": Value::Array(mapped_trait_uri.split(", ").map(|tr| {
                    return json!({
                        "value": tr,
                        "properties": {
                            "gwas:study": [ study_accession ],

                            "gwas:disease_trait": [disease_trait],

                            "gwas:initial_sample_size": [initial_sample_size],
                            "gwas:replication_sample_size": [replication_sample_size],
                            "gwas:region": [region],
                            "gwas:chr_id": [chr_id],
                            "gwas:chr_pos": [chr_pos],
                            "gwas:reported_gene":
                                reported_genes.to_string().split(",")
                                    .map(|s| s.trim())
                                    .collect::<Vec<&str>>(),
                            "gwas:upstream_gene_distance": [upstream_gene_distance],
                            "gwas:downstream_gene_distance": [downstream_gene_distance],
                            "gwas:strongest_snp_risk_allele": [strongest_snp_risk_allele],
                            "gwas:snps": [snps],
                            "gwas:merged": [merged],
                            "gwas:snp_id_current": [snp_id_current],
                            "gwas:context": [context],
                            "gwas:intergenic": [intergenic],
                            "gwas:risk_allele_frequency": [risk_allele_frequency],
                            "gwas:p_value": [p_value],
                            "gwas:pvalue_mlog": [pvalue_mlog],
                            "gwas:p_value_text": [p_value_text],
                            "gwas:or_or_beta": [or_or_beta],
                            "gwas:ci_text": [ci_text],
                            "gwas:platform": [platform],
                            "gwas:cnv": [cnv],
                            "gwas:mapped_trait":[mapped_trait_uri],
                            "gwas:mapped_trait_label": [mapped_trait],
                            "gwas:genotyping_technology": [genotyping_technology]
                        },
                    })
                }).collect())
                
            })).unwrap().to_string().as_bytes()).unwrap();

            nodes_writer.write_all("}\n".as_bytes()).unwrap();


            // let equiv = serialize_equivalence(("pubmed:".to_owned()+pubmedid).as_bytes(), study_accession.as_bytes());

            // if equiv.is_some() {
            //     equivalences_writer.write_all(equiv.unwrap().as_slice());
            // }
        }
    }

}
