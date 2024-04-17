
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::json;

use crate::check_headers::check_headers;
use crate::remove_empty_fields::remove_empty_fields;

pub fn write_studies(csv_reader: &mut csv::Reader<BufReader<StdinLock>>,nodes_writer: &mut BufWriter<StdoutLock>, datasource_name: &str) {

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
            "PLATFORM [SNPS PASSING QC]",
            "ASSOCIATION COUNT",
            "MAPPED_TRAIT",
            "MAPPED_TRAIT_URI",
            "STUDY ACCESSION",
            "GENOTYPING TECHNOLOGY",
            "COHORT",
            "FULL SUMMARY STATISTICS",
            "SUMMARY STATS LOCATION"
        ]);
    }

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
            let platform = record.get(10).unwrap();
            let association_count = record.get(11).unwrap();
            // let mapped_trait = record.get(12).unwrap();
            let mapped_trait_uri = record.get(13).unwrap();
            let study_accession = record.get(14).unwrap();
            let genotyping_technology = record.get(15).unwrap();
            let cohort = record.get(16).unwrap();
            let full_summary_statistics = record.get(17).unwrap();
            let summary_stats_location = record.get(18).unwrap();

            nodes_writer.write_all(remove_empty_fields(&json!({
                "id": study_accession,
                "grebi:type": ["gwas:Study"],
                "rdf:type": ["http://edamontology.org/topic_3517"], // gwas study

                "dcterms:created": [date_added_to_catalog],
                "dcterms:creator": [first_author],
                "dcterms:modified": [date],
                "gwas:journal": [journal],
                "gwas:pubmedid": ["pmid:".to_owned()+pubmedid],

                // Seems derived from PUBMEDID
                // "link": link,

                "dcterms:title": [study],

                "gwas:disease_trait": [disease_trait],

                "gwas:initial_sample_size": [initial_sample_size],
                "gwas:replication_sample_size": [replication_sample_size],
                "gwas:platform": [platform],
                "gwas:mapped_trait":[ mapped_trait_uri ],
                "gwas:association_count": [association_count],
                // "study_accession": [study_accession],
                "gwas:genotyping_technology": [genotyping_technology],
                "gwas:cohort": [cohort],
                "gwas:full_summary_statistics": [full_summary_statistics],
                "gwas:summary_stats_location": [summary_stats_location]

            })).unwrap().to_string().as_bytes()).unwrap();

            nodes_writer.write_all("\n".as_bytes()).unwrap();


            // let equiv = serialize_equivalence(("pubmed:".to_owned()+pubmedid).as_bytes(), study_accession.as_bytes());

            // if equiv.is_some() {
            //     equivalences_writer.write_all(equiv.unwrap().as_slice());
            // }
        }
    }

}