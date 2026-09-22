//! Offline bulk Expression Atlas TPM -> gene/anatomy expression evidence.
//!
//! Reads a gene-level TPM table and its sibling configuration XML and condensed
//! SDRF. No downloads, differential results, proteins, transcripts or
//! individual cells.

use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::OnceLock;

/// Only identifiers explicitly supplied for `organism part`; do not turn cell
/// types (CL) or developmental stages into anatomical locations.
const ANATOMY_PREFIXES: &[&str] = &["uberon", "po", "fbbt", "wbbt", "zfa", "emapa", "emap", "ma", "bto", "efo", "ncit"];
const NORMAL: &[&str] = &["normal", "healthy", "healthy individual", "normal tissue", "disease free", "disease-free"];
const UNTREATED: &[&str] = &["none", "no treatment", "untreated", "not treated", "not applicable", "control", "mock", "mock treatment", "vehicle control", "pbs control"];
const WILD_TYPE: &[&str] = &["wild type", "wild-type", "wild type genotype", "wild-type genotype", "wt"];
const NO_CELL_LINE: &[&str] = &["none", "not applicable", "not a cell line"];
const NO_INFECTION: &[&str] = &["none", "uninfected", "not infected", "mock", "mock infected", "not applicable"];
const MISSING: &[&str] = &["", "NA", "N/A", "NULL", "NaN"];
/// The sample annotations kept as the context of an observation.
const CONTEXT_KEYS: &[&str] = &["developmental stage", "age", "sex", "disease", "genotype", "cultivar", "strain", "growth condition"];

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).unwrap())
}
fn accession_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^E-[A-Z]+-\d+$") }
// Ensembl/Ensembl Genomes gene IDs, including the model-organism native ones
// Atlas passes through: yeast tRNA genes are named like tA(AGC)D.
fn gene_id_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^[A-Za-z0-9_][A-Za-z0-9_.()-]*$") }
fn ontology_uri_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    re(&R, r"^https?://(?:purl\.obolibrary\.org/obo/|www\.ebi\.ac\.uk/efo/)([A-Za-z]+)_([A-Za-z0-9]+)$")
}
fn wormbase_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^WBGene\d+$") }
fn flybase_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^FBgn\d+$") }
fn tair_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^AT[1-5CM]G\d{5}$") }

fn ontology_id(uri: &str) -> Option<String> {
    ontology_uri_re().captures(uri).map(|c| format!("{}:{}", c[1].to_lowercase(), &c[2]))
}

/// An annotation value with the ontology identifiers given for it, sorted.
type Annotation = (String, Vec<String>);

/// One assay's annotations by lower-cased name, in order of first appearance,
/// which decides the exclusion reason a perturbed sample is counted under.
#[derive(Default)]
struct Sample {
    keys: Vec<(String, BTreeSet<Annotation>)>,
}

impl Sample {
    fn get(&self, key: &str) -> Option<&BTreeSet<Annotation>> {
        self.keys.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    fn entry(&mut self, key: &str) -> &mut BTreeSet<Annotation> {
        if let Some(index) = self.keys.iter().position(|(k, _)| k == key) {
            return &mut self.keys[index].1;
        }
        self.keys.push((key.to_string(), BTreeSet::new()));
        &mut self.keys.last_mut().unwrap().1
    }
}

type Samples = HashMap<String, Sample>;

/// The characteristics and factors of every assay in a condensed SDRF.
fn read_annotations(stream: impl BufRead, accession: &str) -> Result<Samples, String> {
    let mut samples = Samples::new();
    for (index, line) in stream.lines().enumerate() {
        let line = line.map_err(|e| format!("condensed SDRF line {}: {}", index + 1, e))?;
        if line.is_empty() {
            continue;
        }
        // Atlas TSVs are unquoted: a free-text value starting with a double
        // quote is read literally, never as a quoted field swallowing the
        // fields and rows after it.
        let row: Vec<&str> = line.split('\t').collect();
        if row.len() < 6 || row[0] != accession || row[2].is_empty() {
            return Err(format!("Malformed condensed SDRF at line {}", index + 1));
        }
        if row[3] != "characteristic" && row[3] != "factor" {
            continue;
        }
        let identifiers: BTreeSet<String> = row[6..].iter().flat_map(|field| field.split_whitespace()).filter_map(ontology_id).collect();
        samples.entry(row[2].to_string()).or_default().entry(&row[4].trim().to_lowercase())
            .insert((row[5].trim().to_string(), identifiers.into_iter().collect()));
    }
    if samples.is_empty() {
        return Err("Empty condensed SDRF".into());
    }
    Ok(samples)
}

/// Conservative metadata filter; absence is not asserted to prove health.
///
/// Atlas baseline classification supplies the unperturbed-study context. When
/// metadata explicitly describe disease/perturbation, only recognised controls
/// are admitted; missing/unknown values in these fields are not controls.
fn exclusion_reason(sample: &Sample) -> Option<&'static str> {
    for (key, annotations) in &sample.keys {
        let values: BTreeSet<String> = annotations.iter().map(|(value, _)| value.to_lowercase()).collect();
        let only = |allowed: &[&str]| values.iter().all(|value| allowed.contains(&value.as_str()));
        if key.contains("cell line") && !only(NO_CELL_LINE) {
            return Some("cell_line");
        }
        if ["disease", "disease state", "disease status"].contains(&key.as_str()) && !only(NORMAL) {
            return Some("disease");
        }
        if key == "genotype" && !only(WILD_TYPE) {
            return Some("genotype");
        }
        if ["treatment", "compound", "stimulus", "perturbation", "drug"].iter().any(|word| key.contains(word)) && !only(UNTREATED) {
            return Some("treatment");
        }
        if (key.contains("infection") || key.contains("infected")) && !only(NO_INFECTION) {
            return Some("infection");
        }
    }
    None
}

struct AssayGroup {
    id: Option<String>,
    label: Option<String>,
    assays: Vec<String>,
}

struct Configuration {
    experiment_type: String,
    groups: Vec<AssayGroup>,
}

const GROUP_PATH: &[&str] = &["analytics", "assay_groups", "assay_group"];
const ASSAY_PATH: &[&str] = &["analytics", "assay_groups", "assay_group", "assay"];

/// Whether the element path is the root element followed by exactly `names`.
fn at(path: &[String], names: &[&str]) -> bool {
    path.len() == names.len() + 1 && path[1..].iter().zip(names).all(|(a, b)| a == b)
}

/// The experiment type and assay groups of an Atlas configuration XML.
fn read_configuration(xml: &str) -> Result<Configuration, String> {
    let malformed = |e: &dyn std::fmt::Display| format!("Malformed configuration XML: {}", e);
    let mut reader = Reader::from_str(xml);
    let mut path: Vec<String> = Vec::new();
    let mut configuration = Configuration { experiment_type: String::new(), groups: Vec::new() };
    // The text of the current <assay> element, up to its end or first child.
    let mut assay_text: Option<String> = None;
    let mut seen_root = false;
    loop {
        let event = reader.read_event().map_err(|e| malformed(&e))?;
        match &event {
            Event::Eof => break,
            Event::Start(e) | Event::Empty(e) => {
                if let Some(text) = assay_text.take() {
                    configuration.groups.last_mut().unwrap().assays.push(text);
                }
                path.push(String::from_utf8_lossy(e.local_name().as_ref()).into_owned());
                let attribute = |name: &[u8]| -> Result<Option<String>, String> {
                    for attribute in e.attributes() {
                        let attribute = attribute.map_err(|e| malformed(&e))?;
                        if attribute.key.as_ref() == name {
                            return Ok(Some(attribute.unescape_value().map_err(|e| malformed(&e))?.into_owned()));
                        }
                    }
                    Ok(None)
                };
                if path.len() == 1 {
                    seen_root = true;
                    configuration.experiment_type = attribute(b"experimentType")?.unwrap_or_default();
                } else if at(&path, GROUP_PATH) {
                    configuration.groups.push(AssayGroup { id: attribute(b"id")?, label: attribute(b"label")?, assays: Vec::new() });
                } else if at(&path, ASSAY_PATH) {
                    assay_text = Some(String::new());
                }
                if matches!(event, Event::Empty(_)) {
                    if let Some(text) = assay_text.take() {
                        configuration.groups.last_mut().unwrap().assays.push(text);
                    }
                    path.pop();
                }
            }
            Event::End(_) => {
                if let Some(text) = assay_text.take() {
                    configuration.groups.last_mut().unwrap().assays.push(text);
                }
                path.pop();
            }
            Event::Text(t) => {
                if let Some(text) = assay_text.as_mut() {
                    text.push_str(&t.unescape().map_err(|e| malformed(&e))?);
                }
            }
            Event::CData(t) => {
                if let Some(text) = assay_text.as_mut() {
                    text.push_str(&String::from_utf8_lossy(&t[..]));
                }
            }
            _ => {}
        }
    }
    // Like ElementTree, refuse a document that ends before its elements do.
    if !path.is_empty() || !seen_root {
        return Err(malformed(&"no element found"));
    }
    Ok(configuration)
}

/// An assay group that qualifies as baseline expression in one anatomy.
struct Group {
    anatomy: String,
    labels: Vec<String>,
    taxon: String,
    label: String,
    context: BTreeMap<String, Vec<String>>,
}

type Counts = BTreeMap<String, u64>;

fn count(counts: &mut Counts, key: &str) {
    *counts.entry(key.to_string()).or_default() += 1;
}

/// The one identifier every record gives for the annotation, or Err when a
/// record has none, several, or they differ between records.
fn single_identifier<'a>(records: &[&'a Sample], key: &str, accept: impl Fn(&str) -> bool) -> Result<&'a str, ()> {
    let per_record: Vec<BTreeSet<&'a str>> = records.iter().map(|sample| {
        sample.get(key).into_iter().flatten().flat_map(|(_, ids)| ids.iter().map(String::as_str)).filter(|id| accept(id)).collect()
    }).collect();
    if per_record.iter().any(|ids| ids.len() != 1) || per_record.iter().any(|ids| *ids != per_record[0]) {
        return Err(());
    }
    Ok(per_record[0].iter().next().unwrap())
}

/// Every assay group in configuration order, with its metadata when it
/// qualifies, and the counts of why groups were excluded.
fn group_metadata(configuration: &Configuration, samples: &Samples) -> Result<(Vec<(String, Option<Group>)>, Counts), String> {
    if configuration.experiment_type.to_lowercase() != "rnaseq_mrna_baseline" {
        return Err("Only rnaseq_mrna_baseline configurations are supported".into());
    }
    let mut groups: Vec<(String, Option<Group>)> = Vec::new();
    let mut counts = Counts::new();
    for group in &configuration.groups {
        let identifier = group.id.clone().unwrap_or_default();
        if identifier.is_empty() || groups.iter().any(|(id, _)| *id == identifier) {
            return Err(format!("Missing or duplicate assay group: {:?}", group.id));
        }
        groups.push((identifier.clone(), None));
        let assays: Vec<&str> = group.assays.iter().map(|assay| assay.trim()).filter(|assay| !assay.is_empty()).collect();
        if assays.is_empty() || assays.iter().collect::<HashSet<_>>().len() != assays.len() {
            return Err(format!("Missing or duplicate assays in {}", identifier));
        }
        if assays.iter().any(|assay| !samples.contains_key(*assay)) {
            return Err(format!("Assay missing from condensed SDRF in group {}", identifier));
        }
        let records: Vec<&Sample> = assays.iter().map(|assay| &samples[*assay]).collect();
        let reasons: BTreeSet<&str> = records.iter().filter_map(|sample| exclusion_reason(sample)).collect();
        if let Some(reason) = reasons.iter().next() {
            count(&mut counts, &format!("excluded_{}", reason));
            continue;
        }
        // Do not attribute a pooled/mixed group's expression to its components.
        let Ok(anatomy) = single_identifier(&records, "organism part", |id| ANATOMY_PREFIXES.contains(&id.split(':').next().unwrap_or(id))) else {
            count(&mut counts, "excluded_unmapped_or_mixed_anatomy");
            continue;
        };
        let Ok(taxon) = single_identifier(&records, "organism", |id| id.starts_with("ncbitaxon:")) else {
            count(&mut counts, "excluded_unmapped_or_mixed_taxon");
            continue;
        };
        let labels: BTreeSet<String> = records.iter().flat_map(|sample| sample.get("organism part").into_iter().flatten())
            .filter(|(_, ids)| ids.iter().any(|id| id == anatomy)).map(|(value, _)| value.clone()).collect();
        let mut context = BTreeMap::new();
        for key in CONTEXT_KEYS {
            let values: BTreeSet<String> = records.iter().flat_map(|sample| sample.get(key).into_iter().flatten())
                .filter(|(value, _)| !value.is_empty()).map(|(value, _)| value.clone()).collect();
            if !values.is_empty() {
                context.insert(key.to_string(), values.into_iter().collect());
            }
        }
        groups.last_mut().unwrap().1 = Some(Group {
            anatomy: anatomy.to_string(),
            labels: labels.into_iter().collect(),
            taxon: taxon.to_string(),
            label: group.label.clone().unwrap_or(identifier),
            context,
        });
        count(&mut counts, "included_groups");
    }
    if groups.is_empty() {
        return Err("Configuration contains no assay groups".into());
    }
    Ok((groups, counts))
}

/// A number as Python's float() reads it, allowing surrounding whitespace.
fn parse_float(text: &str) -> Result<f64, String> {
    text.trim().parse::<f64>().map_err(|_| format!("could not convert string to float: {:?}", text))
}

/// The median of a scalar or five-number TPM summary; None when it is missing.
fn median_tpm(value: &str) -> Result<Option<f64>, String> {
    if MISSING.contains(&value) {
        return Ok(None);
    }
    let parts: Vec<&str> = value.split(',').collect();
    if parts.len() != 1 && parts.len() != 5 {
        return Err(format!("Expected scalar or five-number TPM summary, got {:?}", value));
    }
    let numbers = parts.iter().map(|part| parse_float(part)).collect::<Result<Vec<f64>, String>>()?;
    if numbers.iter().any(|n| !n.is_finite() || *n < 0.0) {
        return Err(format!("Invalid TPM value: {:?}", value));
    }
    if numbers.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(format!("Unordered TPM summary: {:?}", value));
    }
    Ok(Some(numbers[numbers.len() / 2]))
}

/// A float spelled as Python's repr() spells it, so evidence recorded before
/// this port reads the same: shortest round-trip digits, exponent form below
/// 1e-4 and from 1e16, and always a fractional part otherwise.
fn python_float_repr(value: f64) -> String {
    let scientific = format!("{:e}", value);
    let (mantissa, exponent) = scientific.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    if !(-4..16).contains(&exponent) {
        return format!("{}e{}{:02}", mantissa, if exponent < 0 { '-' } else { '+' }, exponent.abs());
    }
    let fixed = format!("{}", value);
    if fixed.contains('.') { fixed } else { format!("{}.0", fixed) }
}

/// JSON text with every character outside printable ASCII escaped, as Python's
/// json.dumps does by default.
fn ensure_ascii(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        let code = c as u32;
        if code < 0x7f {
            out.push(c);
        } else if code <= 0xffff {
            out.push_str(&format!("\\u{:04x}", code));
        } else {
            let v = code - 0x10000;
            out.push_str(&format!("\\u{:04x}\\u{:04x}", 0xd800 + (v >> 10), 0xdc00 + (v & 0x3ff)));
        }
    }
    out
}

/// One observation as the JSON string kept on the gene node: sorted keys,
/// compact, ASCII only, explicitly naming gene, anatomy and taxon so values and
/// their contexts stay paired when evidence arrays are merged.
fn observation(gene: &str, group_id: &str, group: &Group, study: &str, median: f64) -> String {
    let s = |value: &str| serde_json::to_string(value).unwrap();
    let context: Vec<String> = group.context.iter()
        .map(|(key, values)| format!("{}:[{}]", s(key), values.iter().map(|v| s(v)).collect::<Vec<_>>().join(",")))
        .collect();
    ensure_ascii(&format!(
        "{{\"anatomy\":{},\"assay_group\":{},\"context\":{{{}}},\"gene\":{},\"group_label\":{},\"median_tpm\":{},\"study\":{},\"taxon\":{}}}",
        s(&group.anatomy), s(group_id), context.join(","), s(&format!("ensembl:{}", gene)), s(&group.label),
        python_float_repr(median), s(study), s(&group.taxon)))
}

fn gene_node(identifier: &str, name: &str, taxon: &str) -> Result<Map<String, Value>, String> {
    if !gene_id_re().is_match(identifier) {
        return Err(format!("Invalid gene identifier: {:?}", identifier));
    }
    let mut node = Map::new();
    node.insert("id".into(), Value::String(format!("ensembl:{}", identifier)));
    node.insert("grebi:type".into(), Value::String("biolink:Gene".into()));
    node.insert("biolink:in_taxon".into(), Value::String(taxon.into()));
    if !name.is_empty() {
        node.insert("grebi:name".into(), Value::String(name.into()));
    }
    // Atlas uses the Ensembl/Ensembl Genomes annotation IDs, including model
    // organism IDs. Preserve exact, unambiguous native aliases where known.
    for (pattern, prefix) in [(wormbase_re(), "wormbase"), (flybase_re(), "flybase"), (tair_re(), "tair.locus")] {
        if pattern.is_match(identifier) {
            node.insert("dcterms:identifier".into(), json!([format!("{}:{}", prefix, identifier)]));
        }
    }
    Ok(node)
}

fn emit(output: &mut dyn Write, node: &Value) -> Result<(), String> {
    serde_json::to_writer(&mut *output, node).map_err(|e| e.to_string())?;
    output.write_all(b"\n").map_err(|e| e.to_string())
}

/// Streams the TPM table one gene at a time, writing the study, the anatomy
/// nodes and the genes with evidence, and returns the per-study counts.
fn ingest(table: impl BufRead, configuration: &Configuration, annotations: impl BufRead, accession: &str, threshold: f64, output: &mut dyn Write) -> Result<Counts, String> {
    if !accession_re().is_match(accession) {
        return Err(format!("Invalid experiment accession: {:?}", accession));
    }
    if !threshold.is_finite() || threshold < 0.0 {
        return Err("Minimum median TPM must be finite and non-negative".into());
    }
    let samples = read_annotations(annotations, accession)?;
    let (groups, mut counts) = group_metadata(configuration, &samples)?;
    let taxa: BTreeSet<&str> = groups.iter().filter_map(|(_, group)| group.as_ref()).map(|group| group.taxon.as_str()).collect();
    if taxa.len() > 1 {
        return Err("Multiple species in a baseline experiment".into());
    }
    let mut lines = table.lines();
    let header: Vec<String> = match lines.next() {
        Some(line) => line.map_err(|e| e.to_string())?.split('\t').map(String::from).collect(),
        None => Vec::new(),
    };
    if header.len() < 3 || !(header[0] == "GeneID" || header[0] == "Gene ID") || header[1] != "Gene Name" {
        return Err("Expected a gene-level TPM table header".into());
    }
    let columns = &header[2..];
    let by_id: HashMap<&str, &Option<Group>> = groups.iter().map(|(id, group)| (id.as_str(), group)).collect();
    let unique: HashSet<&str> = columns.iter().map(String::as_str).collect();
    if unique.len() != columns.len() || unique.len() != groups.len() || columns.iter().any(|column| !by_id.contains_key(column.as_str())) {
        return Err("TPM columns do not match configuration assay groups".into());
    }
    let selected: Vec<(usize, &str, &Group)> = columns.iter().enumerate()
        .filter_map(|(offset, column)| by_id[column.as_str()].as_ref().map(|group| (offset + 2, column.as_str(), group)))
        .collect();
    let mut seen_genes: HashSet<String> = HashSet::new();
    let mut emitted_anatomy: HashSet<String> = HashSet::new();
    let study = format!("https://www.ebi.ac.uk/gxa/experiments/{}", accession);

    // Keep provenance even if every group/gene is filtered out. The common
    // ingest process requires a nonempty JSONL stream (split emits no files
    // for empty input). No expression assertions are made for excluded groups.
    emit(output, &json!({
        "id": study, "grebi:type": "biolink:Dataset", "grebi:name": format!("Expression Atlas {}", accession),
        "expression_atlas:experiment_accession": accession,
        "expression_atlas:min_median_tpm": threshold,
        "expression_atlas:included_assay_groups": counts.get("included_groups").copied().unwrap_or(0),
    }))?;

    for (offset, line) in lines.enumerate() {
        let line_number = offset + 2;
        let line = line.map_err(|e| format!("TPM row {}: {}", line_number, e))?;
        let row: Vec<&str> = line.split('\t').collect();
        if row.len() != header.len() {
            return Err(format!("Wrong column count in TPM row {}", line_number));
        }
        if !gene_id_re().is_match(row[0]) || seen_genes.contains(row[0]) {
            return Err(format!("Invalid or duplicate gene in TPM row {}: {:?}", line_number, row[0]));
        }
        seen_genes.insert(row[0].to_string());
        count(&mut counts, "genes_read");
        // Observations by anatomy, in the order the anatomies first appear.
        let mut evidence: Vec<(&str, Vec<String>)> = Vec::new();
        for (index, group_id, group) in &selected {
            // Strictly above the configured cutoff, including when it is zero.
            let Some(value) = median_tpm(row[*index])? else { continue };
            if value <= threshold {
                continue;
            }
            let text = observation(row[0], group_id, group, &study, value);
            match evidence.iter_mut().find(|(anatomy, _)| *anatomy == group.anatomy) {
                Some((_, observations)) => observations.push(text),
                None => evidence.push((group.anatomy.as_str(), vec![text])),
            }
            count(&mut counts, "supporting_group_results");
        }
        if evidence.is_empty() {
            continue;
        }
        let mut node = gene_node(row[0], row[1], taxa.iter().next().unwrap())?;
        evidence.sort_by(|a, b| a.0.cmp(b.0));
        let mut expressed_in = Vec::new();
        let mut observations = Vec::new();
        for (anatomy, texts) in evidence {
            if !emitted_anatomy.contains(anatomy) {
                let labels: BTreeSet<&str> = selected.iter().filter(|(_, _, group)| group.anatomy == anatomy)
                    .flat_map(|(_, _, group)| group.labels.iter().map(String::as_str)).collect();
                emit(output, &json!({"id": anatomy, "grebi:type": "biolink:AnatomicalEntity", "grebi:name": labels}))?;
                emitted_anatomy.insert(anatomy.to_string());
            }
            // GrEBI deduplicates complete property values, including reification
            // metadata. Study-specific edge properties would create parallel
            // edges. Keep the relation identical across studies and put paired
            // observations on the gene, explicitly keyed by anatomy and taxon.
            expressed_in.push(json!({
                "grebi:value": anatomy,
                "grebi:properties": {"expression_atlas:min_median_tpm": [threshold]},
            }));
            observations.extend(texts.into_iter().map(Value::String));
            count(&mut counts, "gene_anatomy_pairs");
        }
        node.insert("biolink:expressed_in".into(), Value::Array(expressed_in));
        node.insert("expression_atlas:evidence".into(), Value::Array(observations));
        emit(output, &Value::Object(node))?;
        count(&mut counts, "genes_emitted");
    }
    if seen_genes.is_empty() {
        return Err("TPM table contains no genes".into());
    }
    Ok(counts)
}

/// The per-study summary line, spelled as the Python script spelled it.
fn summary(accession: &str, threshold: f64, counts: &Counts) -> String {
    let mut fields: BTreeMap<&str, String> = counts.iter().map(|(key, value)| (key.as_str(), value.to_string())).collect();
    fields.insert("experiment", serde_json::to_string(accession).unwrap());
    fields.insert("min_median_tpm", python_float_repr(threshold));
    let fields: Vec<String> = fields.iter().map(|(key, value)| format!("{}: {}", serde_json::to_string(key).unwrap(), value)).collect();
    format!("{{{}}}", fields.join(", "))
}

const USAGE: &str = "usage: grebi_ingest_expression_atlas --min-median-tpm MIN_MEDIAN_TPM [--] FILENAME";

fn usage_error(message: &str) -> ! {
    eprintln!("{}\ngrebi_ingest_expression_atlas: error: {}", USAGE, message);
    std::process::exit(2)
}

fn main() {
    let mut threshold: Option<String> = None;
    let mut filename: Option<String> = None;
    let mut options_ended = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if !options_ended {
            if arg == "--" {
                options_ended = true;
                continue;
            }
            if arg == "-h" || arg == "--help" {
                println!("{}\n\nInclude only medians strictly above the TPM cutoff (no default). The\nconfiguration XML and condensed SDRF are read from beside the *-tpms.tsv file.", USAGE);
                return;
            }
            if arg == "--min-median-tpm" {
                threshold = Some(args.next().unwrap_or_else(|| usage_error("argument --min-median-tpm: expected one argument")));
                continue;
            }
            if let Some(value) = arg.strip_prefix("--min-median-tpm=") {
                threshold = Some(value.to_string());
                continue;
            }
            if arg.starts_with('-') && arg.len() > 1 {
                usage_error(&format!("unrecognized arguments: {}", arg));
            }
        }
        if filename.is_some() {
            usage_error(&format!("unrecognized arguments: {}", arg));
        }
        filename = Some(arg);
    }
    let Some(threshold) = threshold else { usage_error("the following arguments are required: --min-median-tpm") };
    let threshold = parse_float(&threshold).unwrap_or_else(|_| usage_error(&format!("argument --min-median-tpm: invalid float value: {:?}", threshold)));
    let Some(filename) = filename else { usage_error("the following arguments are required: filename") };
    let path = Path::new(&filename);
    let name = path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
    let Some(accession) = name.strip_suffix("-tpms.tsv") else { usage_error("Input must be an Atlas gene-level *-tpms.tsv file") };

    let open = |path: &Path| File::open(path).map(BufReader::new).map_err(|e| format!("{}: {}", path.display(), e));
    let result = (|| -> Result<Counts, String> {
        let configuration_path = path.with_file_name(format!("{}-configuration.xml", accession));
        let xml = std::fs::read_to_string(&configuration_path).map_err(|e| format!("{}: {}", configuration_path.display(), e))?;
        let configuration = read_configuration(&xml)?;
        let table = open(path)?;
        let annotations = open(&path.with_file_name(format!("{}.condensed-sdrf.tsv", accession)))?;
        let mut output = BufWriter::new(io::stdout().lock());
        let counts = ingest(table, &configuration, annotations, accession, threshold, &mut output)?;
        output.flush().map_err(|e| e.to_string())?;
        Ok(counts)
    })();
    match result {
        Ok(counts) => eprintln!("{}", summary(accession, threshold, &counts)),
        Err(message) => {
            eprintln!("Expression Atlas ingest failed: {}", message);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACC: &str = "E-MTAB-513";
    const LIVER: &str = "http://purl.obolibrary.org/obo/UBERON_0002107";
    const LUNG: &str = "http://purl.obolibrary.org/obo/UBERON_0002048";
    const TAXON: &str = "http://purl.obolibrary.org/obo/NCBITaxon_9606";
    const TABLE: &str = "GeneID\tGene Name\tg1\nENSG00000000003\tTSPAN6\t2\n";

    fn sdrf(assay: &str, anatomy: &str, extra: &[(&str, &str, &str)], taxon: &str) -> String {
        let mut entries = vec![("organism", "Homo sapiens", taxon), ("organism part", "liver", anatomy)];
        entries.extend(extra.iter().copied());
        entries.iter().map(|(key, value, iri)| format!("{}\t\t{}\tcharacteristic\t{}\t{}\t{}\n", ACC, assay, key, value, iri)).collect()
    }

    fn liver() -> String {
        sdrf("a1", LIVER, &[], TAXON)
    }

    fn config(groups: &[(&str, &[&str])], experiment_type: &str) -> String {
        let mut xml = format!("<configuration experimentType=\"{}\"><analytics><assay_groups>", experiment_type);
        for (name, assays) in groups {
            xml += &format!("<assay_group id=\"{}\" label=\"{}\">", name, name);
            for assay in *assays {
                xml += &format!("<assay>{}</assay>", assay);
            }
            xml += "</assay_group>";
        }
        xml + "</assay_groups></analytics></configuration>"
    }

    fn baseline() -> String {
        config(&[("g1", &["a1"])], "rnaseq_mrna_baseline")
    }

    fn run(table: &str, xml: &str, metadata: &str, threshold: f64) -> Result<(Vec<Value>, Counts), String> {
        let configuration = read_configuration(xml)?;
        let mut output = Vec::new();
        let counts = ingest(table.as_bytes(), &configuration, metadata.as_bytes(), ACC, threshold, &mut output)?;
        let nodes = String::from_utf8(output).unwrap().lines().map(|line| serde_json::from_str(line).unwrap()).collect();
        Ok((nodes, counts))
    }

    fn genes(nodes: &[Value]) -> Vec<&Value> {
        nodes.iter().filter(|node| node["grebi:type"] == "biolink:Gene").collect()
    }

    fn evidence(gene: &Value) -> Vec<Value> {
        gene["expression_atlas:evidence"].as_array().unwrap().iter().map(|text| serde_json::from_str(text.as_str().unwrap()).unwrap()).collect()
    }

    #[test]
    fn yeast_trna_gene_ids_with_parentheses_are_valid() {
        assert!(gene_id_re().is_match("tA(AGC)D"));
        assert!(gene_id_re().is_match("tY(GUA)M1"));
        assert!(gene_id_re().is_match("ENSG00000000003"));
        assert!(!gene_id_re().is_match("tA(AGC) D"));
        assert!(!gene_id_re().is_match(""));
        assert_eq!(gene_node("tA(AGC)D", "", "NCBITaxon:4932").unwrap()["id"], "ensembl:tA(AGC)D");
    }

    #[test]
    fn gene_anatomy_and_paired_evidence() {
        let (nodes, counts) = run(TABLE, &baseline(), &sdrf("a1", LIVER, &[("developmental stage", "adult", "")], TAXON), 0.5).unwrap();
        let node = genes(&nodes)[0];
        assert_eq!(node["id"], "ensembl:ENSG00000000003");
        assert_eq!(node["biolink:in_taxon"], "ncbitaxon:9606");
        let edge = &node["biolink:expressed_in"][0];
        assert_eq!(edge["grebi:value"], "uberon:0002107");
        assert_eq!(edge["grebi:properties"]["expression_atlas:min_median_tpm"], json!([0.5]));
        let observation = &evidence(node)[0];
        assert_eq!(observation["median_tpm"], 2.0);
        assert_eq!(observation["context"], json!({"developmental stage": ["adult"]}));
        assert_eq!(observation["assay_group"], "g1");
        assert_eq!(observation["study"], "https://www.ebi.ac.uk/gxa/experiments/E-MTAB-513");
        assert_eq!(observation["anatomy"], edge["grebi:value"]);
        assert_eq!(observation["gene"], node["id"]);
        assert_eq!(observation["taxon"], node["biolink:in_taxon"]);
        assert_eq!(counts["gene_anatomy_pairs"], 1);
        assert_eq!(nodes.len(), 3); // provenance, anatomy, gene
    }

    #[test]
    fn threshold_uses_median_strictly_above_cutoff() {
        let table = "GeneID\tGene Name\tg1\nENSG00000000001\tA\t0,0.1,0.5,20,100\nENSG00000000002\tB\t0,0.5,0.6,1,100\nENSG00000000003\tC\t0\nENSG00000000004\tD\tNA\n";
        let (nodes, _) = run(table, &baseline(), &liver(), 0.5).unwrap();
        assert_eq!(genes(&nodes).iter().map(|g| g["id"].as_str().unwrap()).collect::<Vec<_>>(), ["ensembl:ENSG00000000002"]);
        assert!(genes(&run(table, &baseline(), &liver(), 1.0).unwrap().0).is_empty());
        assert_eq!(genes(&run(table, &baseline(), &liver(), 0.0).unwrap().0).len(), 2);
    }

    #[test]
    fn duplicate_anatomy_merges_groups_without_losing_context() {
        let table = "GeneID\tGene Name\tg2\tg1\nENSG00000000003\tTSPAN6\t3\t2\n";
        let xml = config(&[("g1", &["a1"]), ("g2", &["a2"])], "rnaseq_mrna_baseline");
        let metadata = liver() + &sdrf("a2", LIVER, &[("developmental stage", "embryonic", "")], TAXON);
        let (nodes, counts) = run(table, &xml, &metadata, 0.5).unwrap();
        let gene = genes(&nodes)[0];
        assert_eq!(gene["biolink:expressed_in"].as_array().unwrap().len(), 1);
        let medians: BTreeMap<String, f64> = evidence(gene).iter()
            .map(|o| (o["assay_group"].as_str().unwrap().to_string(), o["median_tpm"].as_f64().unwrap())).collect();
        assert_eq!(medians, BTreeMap::from([("g1".to_string(), 2.0), ("g2".to_string(), 3.0)]));
        assert_eq!(counts["supporting_group_results"], 2);
    }

    #[test]
    fn different_observations_have_identical_edge_values_for_merging() {
        let (first, _) = run(TABLE, &baseline(), &liver(), 0.5).unwrap();
        let (second, _) = run("GeneID\tGene Name\tg1\nENSG00000000003\tTSPAN6\t5\n", &baseline(),
                              &sdrf("a1", LIVER, &[("developmental stage", "embryonic", "")], TAXON), 0.5).unwrap();
        let (first, second) = (genes(&first)[0], genes(&second)[0]);
        assert_eq!(first["biolink:expressed_in"], second["biolink:expressed_in"]);
        assert_ne!(first["expression_atlas:evidence"], second["expression_atlas:evidence"]);
    }

    #[test]
    fn excludes_perturbed_unknown_and_cell_line_samples() {
        for (key, value) in [("disease", "cancer"), ("disease", "not available"), ("cell line", "HepG2"), ("progenitor cell line", "iPSC-1"),
                             ("genotype", "knockout"), ("compound", "lipopolysaccharide"), ("treatment", "unknown"), ("infection", "infected")] {
            let (nodes, counts) = run(TABLE, &baseline(), &sdrf("a1", LIVER, &[(key, value, "")], TAXON), 0.5).unwrap();
            assert!(genes(&nodes).is_empty(), "{} = {}", key, value);
            assert_eq!(nodes.len(), 1, "provenance keeps the ingest nonempty");
            assert_eq!(counts.get("included_groups"), None);
        }
    }

    #[test]
    fn controls_are_accepted() {
        for (key, value) in [("disease", "normal"), ("genotype", "wild type genotype"), ("compound", "PBS control"), ("treatment", "untreated")] {
            let (nodes, _) = run(TABLE, &baseline(), &sdrf("a1", LIVER, &[(key, value, "")], TAXON), 0.5).unwrap();
            assert_eq!(genes(&nodes).len(), 1, "{} = {}", key, value);
        }
    }

    #[test]
    fn literal_quotes_do_not_swallow_annotations() {
        // A free-text value starting with '"' must be read literally: it must
        // not absorb the rows after it, or the disease annotation below would
        // never be seen and a perturbed group would pass as baseline.
        let metadata = sdrf("a1", LIVER, &[("clinical information", "\"BMI 30; \"\"obese\"\"", ""), ("disease", "cancer", "")], TAXON);
        let (nodes, counts) = run(TABLE, &baseline(), &metadata, 0.5).unwrap();
        assert!(genes(&nodes).is_empty());
        assert_eq!(counts["excluded_disease"], 1);
        let samples = read_annotations(metadata.as_bytes(), ACC).unwrap();
        let sample = &samples["a1"];
        assert!(sample.get("disease").is_some());
        let values: Vec<&str> = sample.get("clinical information").unwrap().iter().map(|(value, _)| value.as_str()).collect();
        assert_eq!(values, ["\"BMI 30; \"\"obese\"\""]);
    }

    #[test]
    fn one_excluded_replicate_excludes_whole_group() {
        let xml = config(&[("g1", &["a1", "a2"])], "rnaseq_mrna_baseline");
        let metadata = liver() + &sdrf("a2", LIVER, &[("disease", "cancer", "")], TAXON);
        assert!(genes(&run(TABLE, &xml, &metadata, 0.5).unwrap().0).is_empty());
    }

    #[test]
    fn mixed_unmapped_and_cell_type_anatomy_are_not_guessed() {
        for anatomy in ["", "CL:0000236", "http://purl.obolibrary.org/obo/CL_0000236"] {
            assert!(genes(&run(TABLE, &baseline(), &sdrf("a1", anatomy, &[], TAXON), 0.5).unwrap().0).is_empty(), "{}", anatomy);
        }
        let xml = config(&[("g1", &["a1", "a2"])], "rnaseq_mrna_baseline");
        assert!(genes(&run(TABLE, &xml, &(liver() + &sdrf("a2", LUNG, &[], TAXON)), 0.5).unwrap().0).is_empty());
        assert!(genes(&run(TABLE, &baseline(), &sdrf("a1", LIVER, &[], ""), 0.5).unwrap().0).is_empty());
    }

    #[test]
    fn plant_anatomy_and_native_gene_aliases() {
        let table = "GeneID\tGene Name\tg1\nAT1G01010\tNAC001\t4\n";
        let metadata = sdrf("a1", "http://purl.obolibrary.org/obo/PO_0009005", &[], "http://purl.obolibrary.org/obo/NCBITaxon_3702");
        let (nodes, _) = run(table, &baseline(), &metadata, 0.5).unwrap();
        let gene = genes(&nodes)[0];
        assert_eq!(gene["biolink:expressed_in"][0]["grebi:value"], "po:0009005");
        assert_eq!(gene["dcterms:identifier"], json!(["tair.locus:AT1G01010"]));
        assert_eq!(gene_node("WBGene00000001", "", "ncbitaxon:6239").unwrap()["dcterms:identifier"], json!(["wormbase:WBGene00000001"]));
        assert_eq!(gene_node("FBgn0000001", "abn", "ncbitaxon:7227").unwrap()["dcterms:identifier"], json!(["flybase:FBgn0000001"]));
        assert!(gene_node("AT6G00001", "", "ncbitaxon:3702").unwrap().get("dcterms:identifier").is_none());
        assert!(gene_node("-bad", "", "ncbitaxon:3702").is_err());
    }

    #[test]
    fn rejects_invalid_thresholds_and_expression_values() {
        for threshold in [-1.0, f64::INFINITY, f64::NAN] {
            assert!(run(TABLE, &baseline(), &liver(), threshold).is_err(), "{}", threshold);
        }
        for value in ["-1", "inf", "2,1,0,3,4", "1,2", "garbage", "1,2,NaN,4,5"] {
            assert!(median_tpm(value).is_err(), "{}", value);
        }
        assert_eq!(median_tpm("NA").unwrap(), None);
        assert_eq!(median_tpm(" 3 ").unwrap(), Some(3.0));
        assert_eq!(median_tpm("0,0.5,0.6,1,100").unwrap(), Some(0.6));
    }

    #[test]
    fn rejects_schema_mismatch_duplicate_and_truncated_rows() {
        for table in ["", "GeneID\tGene Name\tg1\n", "TranscriptID\tGene Name\tg1\nx\tx\t2\n", "GeneID\tGene Name\tg2\nx\tx\t2\n",
                      "GeneID\tGene Name\tg1\nx\tx\n", "GeneID\tGene Name\tg1\nx\tx\t2\nx\tx\t3\n"] {
            assert!(run(table, &baseline(), &liver(), 0.5).is_err(), "{:?}", table);
        }
        let error = run(TABLE, &config(&[("g1", &["missing"])], "rnaseq_mrna_baseline"), &liver(), 0.5).unwrap_err();
        assert!(error.contains("missing from"), "{}", error);
        let error = run(TABLE, &config(&[("g1", &["a1"])], "rnaseq_mrna_differential"), &liver(), 0.5).unwrap_err();
        assert!(error.contains("baseline"), "{}", error);
        let error = run(TABLE, &baseline(), "wrong\trow\n", 0.5).unwrap_err();
        assert!(error.contains("Malformed"), "{}", error);
        assert!(read_configuration("<configuration experimentType=\"rnaseq_mrna_baseline\"><analytics>").is_err());
        assert!(read_configuration("").is_err());
        assert!(read_configuration("<configuration experimentType=\"rnaseq_mrna_baseline\"/>").is_ok());
    }

    #[test]
    fn floats_are_spelled_as_python_spells_them() {
        for (value, expected) in [(2.0, "2.0"), (0.6, "0.6"), (100.0, "100.0"), (0.0001, "0.0001"), (0.00001, "1e-05"),
                                  (1e15, "1000000000000000.0"), (1.2345678901234568e16, "1.2345678901234568e+16"), (0.0, "0.0")] {
            assert_eq!(python_float_repr(value), expected);
        }
        assert_eq!(ensure_ascii("10 µm 😀 \\\"x\\\""), "10 \\u00b5m \\ud83d\\ude00 \\\"x\\\"");
    }
}
