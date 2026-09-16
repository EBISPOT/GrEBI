//! The PRIDE v3 projects/all JSON array as metadata-only GrEBI JSONL: one
//! node per project with its identifiers, descriptive fields, ontology terms,
//! sample attributes, people, organisations and references. Reads the export
//! (a file argument, or stdin) one project at a time; never fetches anything.

use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::sync::OnceLock;
use struson::reader::{JsonReader, JsonStreamReader};

const CV_FIELDS: &[(&str, &str)] = &[
    ("organisms", "pride:organism"), ("organismParts", "pride:organismPart"), ("diseases", "pride:disease"),
    ("instruments", "pride:instrument"), ("softwares", "pride:software"), ("experimentTypes", "pride:experimentType"),
    ("quantificationMethods", "pride:quantificationMethod"), ("identifiedPTMStrings", "pride:modification"),
];
const SAMPLE_FIELDS: &[(&str, &str)] = &[
    ("organism", "pride:organism"), ("organism part", "pride:organismPart"), ("disease", "pride:disease"),
    ("cell type", "pride:cellType"), ("cell line", "pride:cellLine"), ("biosample accession number", "pride:sample"),
    ("biosample", "pride:sample"), ("biosamples", "pride:sample"),
];
const SCALAR_FIELDS: &[(&str, &str)] = &[
    ("title", "grebi:name"), ("projectDescription", "grebi:description"), ("submissionDate", "dcterms:submitted"),
    ("publicationDate", "dcterms:issued"), ("license", "dcterms:license"), ("submissionType", "pride:submissionType"),
    ("sampleProcessingProtocol", "pride:sampleProcessingProtocol"), ("dataProcessingProtocol", "pride:dataProcessingProtocol"),
];

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).unwrap())
}
fn project_accession() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^(?:PXD|PRD|PAD)\d+$") }
fn curie() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^[A-Za-z][A-Za-z0-9_.-]*:\S+$") }
fn doi() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"(?i)^10\.\d{4,9}/\S+$") }
fn biosample() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"(?i)^SAM(?:EA|N|D)\d+$") }
fn orcid() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$") }

/// A string's trimmed text; anything else is the empty string.
fn text(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(s)) => s.trim().to_string(),
        _ => String::new(),
    }
}

/// The objects in a value: the object itself, or the objects of an array.
fn objects(value: Option<&Value>) -> Vec<&Map<String, Value>> {
    match value {
        Some(Value::Object(o)) => vec![o],
        Some(Value::Array(items)) => items.iter().filter_map(|v| v.as_object()).collect(),
        _ => vec![],
    }
}

/// Python truthiness: null, false, 0, "" and empty containers are false.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// A key-order-independent rendering, for deduplication.
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            format!("{{{}}}", keys.iter().map(|k| format!("{}:{}", json!(k), canonical(&o[*k]))).collect::<Vec<_>>().join(","))
        }
        Value::Array(a) => format!("[{}]", a.iter().map(canonical).collect::<Vec<_>>().join(",")),
        v => v.to_string(),
    }
}

/// The truthy values, first occurrence of each.
fn unique(values: Vec<Value>) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in values {
        if truthy(&value) && seen.insert(canonical(&value)) {
            out.push(value);
        }
    }
    out
}

/// A reference as a CURIE or URL to record, or None for anything else.
fn identifier(value: Option<&Value>) -> Option<String> {
    let mut value = text(value);
    // Some otherOmicsLinks arrive wrapped as "url:https://...". Unwrap them so
    // the URL itself is recorded instead of minting a junk "url:" CURIE node.
    if value.len() >= 4 && value[..4].eq_ignore_ascii_case("url:") {
        value = value[4..].trim().to_string();
    }
    // Only record references, never file-transfer locations.
    if value.starts_with("https://") || value.starts_with("http://") {
        return Some(value);
    }
    if biosample().is_match(&value) {
        return Some(format!("biosample:{}", value.to_uppercase()));
    }
    if !curie().is_match(&value) {
        return None;
    }
    let (prefix, local) = value.split_once(':').unwrap();
    let prefix = prefix.to_lowercase();
    if ["ftp", "sftp", "file", "aspera", "fasp"].contains(&prefix.as_str()) {
        return None;
    }
    if ["newt", "taxon", "taxonomy", "ncbitaxon"].contains(&prefix.as_str()) {
        return Some(format!("ncbitaxon:{}", local));
    }
    Some(value)
}

fn doi_identifier(value: Option<&Value>) -> Option<String> {
    let mut value = text(value);
    for prefix in ["https://doi.org/", "http://doi.org/", "doi:"] {
        if let Some(rest) = value.strip_prefix(prefix) {
            value = rest.to_string();
            break;
        }
    }
    if doi().is_match(&value) { Some(format!("doi:{}", value)) } else { None }
}

fn labelled(id: String, label: &str) -> Value {
    json!({"grebi:value": id, "grebi:properties": {"rdfs:label": [label]}})
}

/// An ontology term: its id with its label, the id alone, or a literal when there is no id.
fn cv_value(value: &Value) -> Value {
    if value.is_string() {
        return Value::String(identifier(Some(value)).unwrap_or_else(|| text(Some(value))));
    }
    let Some(object) = value.as_object() else { return Value::Null };
    let accession = identifier(object.get("accession"));
    let label = text(object.get("name"));
    if let Some(accession) = accession {
        return if !label.is_empty() { labelled(accession, &label) } else { Value::String(accession) };
    }
    let value_id = identifier(object.get("value"));
    Value::String(value_id.or_else(|| Some(text(object.get("value"))).filter(|s| !s.is_empty())).unwrap_or(label))
}

fn parse_project(project: &Map<String, Value>) -> Result<Map<String, Value>, String> {
    let accession = text(project.get("accession"));
    if !project_accession().is_match(&accession) {
        return Err(format!("Missing or invalid PRIDE project accession: {:?}", accession));
    }
    let mut node = Map::new();
    let id = format!("pride.project:{}", accession);
    node.insert("id".into(), Value::String(id.clone()));
    node.insert("grebi:type".into(), Value::String("pride:Project".into()));
    // PXD identifies the same dataset in PRIDE and ProteomeXchange. PRD is a
    // legacy PRIDE accession; PAD is an affinity-proteomics accession. Neither
    // should be assigned a ProteomeXchange alias.
    let mut aliases: Vec<String> = if accession.starts_with("PXD") { vec![format!("px:{}", accession)] } else { vec![] };
    if let Some(project_doi) = doi_identifier(project.get("doi")) {
        aliases.push(project_doi);
    }
    if !aliases.is_empty() {
        node.insert("dcterms:identifier".into(), json!(aliases));
    }
    for (source, target) in SCALAR_FIELDS {
        let value = text(project.get(*source));
        if !value.is_empty() {
            node.insert(target.to_string(), Value::String(value));
        }
    }
    for (source, target) in [("keywords", "pride:keyword"), ("projectTags", "pride:tag")] {
        let values = match project.get(source) {
            Some(Value::Array(items)) => unique(items.iter().map(|v| Value::String(text(Some(v)))).collect()),
            _ => vec![],
        };
        if !values.is_empty() {
            node.insert(target.into(), Value::Array(values));
        }
    }
    for (source, target) in CV_FIELDS {
        let values = unique(objects(project.get(*source)).into_iter().map(|o| cv_value(&Value::Object(o.clone()))).collect());
        if !values.is_empty() {
            node.insert(target.to_string(), Value::Array(values));
        }
    }
    for attribute in objects(project.get("sampleAttributes")) {
        let key_name = attribute.get("key").and_then(|k| k.get("name"));
        let name = text(key_name).to_lowercase();
        let target = SAMPLE_FIELDS.iter().find(|(n, _)| *n == name).map(|(_, t)| *t);
        let raw_values: Vec<Value> = match attribute.get("value") {
            None | Some(Value::Null) => vec![],
            Some(Value::Array(items)) => items.clone(),
            Some(v) if !truthy(v) => vec![],
            Some(v) => vec![v.clone()],
        };
        if let Some(target) = target {
            let mut values: Vec<Value> = node.get(target).and_then(|v| v.as_array()).cloned().unwrap_or_default();
            values.extend(raw_values.iter().map(cv_value));
            node.insert(target.to_string(), Value::Array(unique(values)));
        }
    }
    let mut people = objects(project.get("submitters"));
    people.extend(objects(project.get("labPIs")));
    let mut creators = Vec::new();
    for person in &people {
        let mut name = text(person.get("name"));
        if name.is_empty() {
            name = [text(person.get("firstName")), text(person.get("lastName"))].into_iter()
                .filter(|p| !p.is_empty()).collect::<Vec<_>>().join(" ");
        }
        let orcid_text = text(person.get("orcid"));
        let orcid_id = orcid_text.strip_prefix("https://orcid.org/").unwrap_or(&orcid_text).to_string();
        if orcid().is_match(&orcid_id) {
            let creator = format!("orcid:{}", orcid_id);
            creators.push(if !name.is_empty() { labelled(creator, &name) } else { Value::String(creator) });
        } else if !name.is_empty() {
            creators.push(Value::String(name));
        }
    }
    let creators = unique(creators);
    if !creators.is_empty() {
        node.insert("dcterms:creator".into(), Value::Array(creators));
    }
    let affiliations = unique(people.iter().map(|p| Value::String(text(p.get("affiliation")))).collect());
    if !affiliations.is_empty() {
        node.insert("pride:organisation".into(), Value::Array(affiliations));
    }
    let mut references: Vec<Value> = Vec::new();
    for reference in objects(project.get("references")) {
        let pmid = match reference.get("pubmedID") {
            Some(v) if truthy(v) => match v { Value::String(s) => s.trim().to_string(), v => v.to_string() },
            _ => String::new(),
        };
        if !pmid.is_empty() && pmid.chars().all(|c| c.is_ascii_digit()) && pmid.chars().any(|c| c != '0') {
            references.push(Value::String(format!("pubmed:{}", pmid)));
        }
        if let Some(doi) = doi_identifier(reference.get("doi")) {
            references.push(Value::String(doi));
        }
    }
    if let Some(Value::Array(links)) = project.get("otherOmicsLinks") {
        references.extend(links.iter().filter_map(|v| identifier(Some(v))).map(Value::String));
    }
    let mut own_ids: HashSet<String> = aliases.iter().cloned().collect();
    own_ids.insert(id);
    let references = unique(references.into_iter().filter(|r| !own_ids.contains(r.as_str().unwrap_or(""))).collect());
    if !references.is_empty() {
        node.insert("dcterms:references".into(), Value::Array(references));
    }
    // No file inventories, download counts, or contact email addresses are
    // copied: only the explicit metadata fields above enter the graph.
    Ok(node)
}

fn ingest(input: Box<dyn Read>) -> Result<usize, String> {
    let mut json = JsonStreamReader::new(BufReader::new(input));
    let mut out = BufWriter::new(io::stdout().lock());
    let mut seen: HashSet<String> = HashSet::new();
    json.begin_array().map_err(|e| format!("Expected a PRIDE projects JSON array: {}", e))?;
    while json.has_next().map_err(|e| e.to_string())? {
        let project: Value = json.deserialize_next().map_err(|e| format!("Invalid or truncated PRIDE project JSON: {}", e))?;
        let Some(object) = project.as_object() else { return Err("Expected a project object in PRIDE export".into()) };
        let node = parse_project(object)?;
        let id = node["id"].as_str().unwrap().to_string();
        if !seen.insert(id.clone()) {
            return Err(format!("Duplicate PRIDE project in export: {}", id));
        }
        serde_json::to_writer(&mut out, &Value::Object(node)).unwrap();
        out.write_all(b"\n").unwrap();
    }
    json.end_array().map_err(|e| e.to_string())?;
    json.consume_trailing_whitespace().map_err(|_| "Unexpected content after PRIDE projects array".to_string())?;
    out.flush().unwrap();
    if seen.is_empty() {
        return Err("PRIDE export contains no projects".into());
    }
    Ok(seen.len())
}

fn main() {
    // `pride.py -- FILE` became `grebi_ingest_pride -- FILE`; without a file, stdin
    let args: Vec<String> = std::env::args().skip(1).filter(|a| a != "--").collect();
    let input: Box<dyn Read> = match args.first() {
        Some(path) => Box::new(std::fs::File::open(path).unwrap_or_else(|e| { eprintln!("{}: {}", path, e); std::process::exit(1) })),
        None => Box::new(io::stdin()),
    };
    match ingest(input) {
        Ok(count) => eprintln!("Ingested {} PRIDE projects", count),
        Err(message) => {
            eprintln!("{}", message);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(json: &str) -> Map<String, Value> {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn accessions_are_validated() {
        for json in [r#"{"accession": null}"#, r#"{"accession": ""}"#, r#"{"accession": "PXDnope"}"#,
                     r#"{"accession": "PXF123456"}"#, r#"{"accession": "PXD001357 extra"}"#, "{}"] {
            assert!(parse_project(&project(json)).is_err(), "{}", json);
        }
        assert_eq!(parse_project(&project(r#"{"accession": "PXD001357"}"#)).unwrap()["id"], "pride.project:PXD001357");
    }

    #[test]
    fn doi_spellings_are_normalised() {
        for value in ["10.1234/example", "doi:10.1234/example", "https://doi.org/10.1234/example"] {
            assert_eq!(doi_identifier(Some(&Value::String(value.into()))).as_deref(), Some("doi:10.1234/example"), "{}", value);
        }
        assert_eq!(doi_identifier(Some(&Value::String("not a doi".into()))), None);
        assert_eq!(doi_identifier(None), None);
    }
}
