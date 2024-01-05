
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, self, BufReader, StdinLock, StdoutLock, Write};
use std::ptr::eq;
use clap::Parser;
use grebi_shared::prefix_map::PrefixMap;
use grebi_shared::prefix_map::PrefixMapBuilder;
use grebi_shared::serialize_equivalence;
use serde_json::json;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(short, long)]
    datasource_name: String,

    #[arg(short, long)]
    filename: String,

    #[arg(short, long)]
    output_nodes:String,

    #[arg(short, long)]
    output_equivalences:String
}

fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let reader = BufReader::new(stdin);


    let mut output_nodes = BufWriter::new(
        File::create(args.output_nodes.as_str()).unwrap());

    let mut output_equivalences = BufWriter::new(
         File::create(args.output_equivalences.as_str()).unwrap());
    // output_equivalences.write_all(b"subject_id\tobject_id\n").unwrap();



    let normalise = {
        let rdr = BufReader::new( std::fs::File::open("prefix_map_normalise.json").unwrap() );
        let mut builder = PrefixMapBuilder::new();
        serde_json::from_reader::<_, HashMap<String, String>>(rdr).unwrap().into_iter().for_each(|(k, v)| {
            builder.add_mapping(k, v);
        });
        builder.build()
    };


    let mut csv_reader =
        csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(reader);

    if args.filename.starts_with("gwas-catalog-associations_ontology-annotated.") {
        write_associations(&mut csv_reader, &mut output_nodes, &mut output_equivalences, &args.datasource_name, &normalise);
    } else {
        panic!("Unknown filename: {}", args.filename);
    }

}

fn write_associations(csv_reader: &mut csv::Reader<BufReader<StdinLock>>,nodes_writer: &mut BufWriter<File>, equivalences_writer:&mut BufWriter<File>, datasource_name: &str, normalise: &PrefixMap) {

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

            nodes_writer.write_all(r#"{"subject":"pubmed:"#.as_bytes()).unwrap();
            nodes_writer.write_all(pubmedid.as_bytes()).unwrap();
            nodes_writer.write_all(&middle_json_fragment).unwrap();

            nodes_writer.write_all(json!({
                "dcterms:created": [date_added_to_catalog],
                "dcterms:creator": [first_author],
                "dcterms:modified": [date],
                "journal": [journal],

                // Seems derived from PUBMEDID
                // "link": link,

                "dcterms:title": [study],

                "disease_trait": [disease_trait],

                "initial_sample_size": [initial_sample_size],
                "replication_sample_size": [replication_sample_size],
                "region": [region],
                "chr_id": [chr_id],
                "chr_pos": [chr_pos],
                "reported_gene":
                    reported_genes.to_string().split(",")
                        .map(|s| s.trim())
                        .collect::<Vec<&str>>(),
                "mapped_gene": [mapped_gene],
                "upstream_gene_id": [upstream_gene_id],
                "downstream_gene_id": [downstream_gene_id],
                "snp_gene_ids": [snp_gene_ids],
                "upstream_gene_distance": [upstream_gene_distance],
                "downstream_gene_distance": [downstream_gene_distance],
                "strongest_snp_risk_allele": [strongest_snp_risk_allele],
                "snps": [snps],
                "merged": [merged],
                "snp_id_current": [snp_id_current],
                "context": [context],
                "intergenic": [intergenic],
                "risk_allele_frequency": [risk_allele_frequency],
                "p_value": [p_value],
                "pvalue_mlog": [pvalue_mlog],
                "p_value_text": [p_value_text],
                "or_or_beta": [or_or_beta],
                "ci_text": [ci_text],
                "platform": [platform],
                "cnv": [cnv],
                "mapped_trait_label": [mapped_trait],
                "mapped_trait":[ normalise.reprefix(&String::from( mapped_trait_uri)) ],
                // "study_accession": study_accession,
                "genotyping_technology": [genotyping_technology]
            }).to_string().as_bytes()).unwrap();

            nodes_writer.write_all("}\n".as_bytes()).unwrap();


            let equiv = serialize_equivalence(("pubmed:".to_owned()+pubmedid).as_bytes(), study_accession.as_bytes());

            if equiv.is_some() {
                equivalences_writer.write_all(equiv.unwrap().as_slice());
            }
        }
    }

}


fn check_headers(got:Vec<&str>, expected:Vec<&str>) {

    if got.len() != expected.len() {
        panic!("Expected {} headers, but found {}", expected.len(), got.len());
    }

    for n in 0..expected.len() {
        if got[n] != expected[n] {
            panic!("Expected header {} to be {}, but found {}", n, expected[n], got[n]);
        }
    }
}
