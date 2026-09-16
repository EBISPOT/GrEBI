//! BioStudies PageTab JSON metadata as GrEBI JSONL.
//!
//! The input may be individual PageTab JSON files, directories containing the
//! BioStudies FTP tree, or a single JSON document on stdin. Directory traversal
//! only selects each submission's accession-level JSON document; it does not
//! enter `Files` directories and excludes the Europe PMC collection.

use regex::Regex;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const EUROPE_PMC_NAMES: &[&str] = &["europepmc", "s-epmc"];

fn re(cell: &'static OnceLock<Regex>, pattern: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pattern).unwrap())
}
fn doi_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"(?i)^10\.\d{4,9}/\S+$") }
fn pmid_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"(?i)^(?:pmid:)?(\d+)$") }
fn pmcid_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"(?i)^(?:pmc:?)?(PMC\d+)$") }
fn ontology_id_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"^(?:https?://\S+|[A-Za-z][A-Za-z0-9_.-]*[:_][A-Za-z0-9][A-Za-z0-9_.:-]*)$") }
fn project_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"(?i)^PRJ[A-Z]+\d+$") }
fn ena_path_re() -> &'static Regex { static R: OnceLock<Regex> = OnceLock::new(); re(&R, r"(?i)^/ena/(?:data|browser)/view/(PRJ[A-Z]+\d+)/?$") }

/// The objects in arbitrarily nested PageTab arrays.
fn as_objects(value: Option<&Value>) -> Vec<&Map<String, Value>> {
    let mut out = Vec::new();
    fn walk<'a>(value: &'a Value, out: &mut Vec<&'a Map<String, Value>>) {
        match value {
            Value::Object(o) => out.push(o),
            Value::Array(items) => items.iter().for_each(|v| walk(v, out)),
            _ => {}
        }
    }
    if let Some(v) = value {
        walk(v, &mut out);
    }
    out
}

/// Python's str() of a JSON scalar, for attribute values that are not strings.
fn stringify(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::Null => String::new(),
        v => v.to_string(),
    }
}

fn attributes(owner: &Map<String, Value>) -> Vec<&Map<String, Value>> {
    as_objects(owner.get("attributes"))
}

/// The trimmed, non-empty values of the attributes with any of the names (case-insensitively).
fn attribute_values(owner: &Map<String, Value>, names: &[&str]) -> Vec<String> {
    let wanted: Vec<String> = names.iter().map(|n| n.to_lowercase()).collect();
    let mut values = Vec::new();
    for attribute in attributes(owner) {
        let name = attribute.get("name").map(stringify).unwrap_or_default().trim().to_lowercase();
        if !wanted.contains(&name) {
            continue;
        }
        match attribute.get("value") {
            None | Some(Value::Null) => {}
            Some(v) => {
                let text = stringify(v);
                if !text.trim().is_empty() {
                    values.push(text.trim().to_string());
                }
            }
        }
    }
    values
}

/// PageTab sections, walked depth first, never file or link objects.
fn sections<'a>(section: Option<&'a Value>, out: &mut Vec<&'a Map<String, Value>>) {
    for current in as_objects(section) {
        out.push(current);
        sections(current.get("subsections"), out);
    }
}

fn deduplicate(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in values {
        let value = value.trim().to_string();
        if !value.is_empty() && seen.insert(value.clone()) {
            out.push(value);
        }
    }
    out
}

fn first(values: &[String]) -> Option<String> {
    values.iter().find(|v| !v.is_empty()).cloned()
}

/// Bare, `doi:`-prefixed and doi.org URL forms of a DOI, normalised.
fn doi_identifier(value: &str) -> Option<String> {
    let mut value = value.to_string();
    let mut stripped = true;
    while stripped {
        stripped = false;
        for prefix in ["doi:", "https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/"] {
            if value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix) {
                value = value[prefix.len()..].to_string();
                stripped = true;
            }
        }
    }
    if doi_re().is_match(&value) { Some(format!("doi:{}", value)) } else { None }
}

/// The parts of a URL Python's urlparse would give: scheme, hostname and path.
fn parse_url(value: &str) -> (String, Option<String>, String) {
    let scheme_end = value.find(':').filter(|&i| {
        let s = &value[..i];
        !s.is_empty() && s.chars().next().unwrap().is_ascii_alphabetic() && s.chars().all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
    });
    let (scheme, rest) = match scheme_end {
        Some(i) => (value[..i].to_lowercase(), &value[i + 1..]),
        None => (String::new(), value),
    };
    if let Some(after) = rest.strip_prefix("//") {
        let end = after.find(|c| c == '/' || c == '?' || c == '#').unwrap_or(after.len());
        let netloc = &after[..end];
        let host = netloc.rsplit('@').next().unwrap_or("");
        let host = host.rsplit_once(':').map(|(h, port)| if port.chars().all(|c| c.is_ascii_digit()) { h } else { host }).unwrap_or(host);
        let path_part = &after[end..];
        let path = path_part.split(|c| c == '?' || c == '#').next().unwrap_or("").to_string();
        let hostname = if host.is_empty() { None } else { Some(host.to_lowercase()) };
        return (scheme, hostname, path);
    }
    let path = rest.split(|c| c == '?' || c == '#').next().unwrap_or("").to_string();
    (scheme, None, path)
}

fn link_reference(link: &Map<String, Value>) -> Option<String> {
    let value = link.get("url").map(stringify).unwrap_or_default().trim().to_string();
    if value.is_empty() {
        return None;
    }
    let (scheme, hostname, path) = parse_url(&value);
    let link_type = attribute_values(link, &["Type"]).join(" ").to_lowercase();
    // Submission payloads are outside GrEBI's scope. In particular, do not
    // turn FIRE/FTP paths or PageTab file links into graph identifiers.
    if link_type.contains("file") || scheme == "ftp" || scheme == "sftp" {
        return None;
    }
    // DOIs appear bare, doi:-prefixed, or as doi.org URLs. A DOI-typed link
    // whose value is not recognisable as a DOI falls through to the URL
    // handling below rather than minting a malformed identifier.
    if let Some(doi) = doi_identifier(&value) {
        return Some(doi);
    }
    let pmid = pmid_re().captures(&value).map(|c| c[1].to_string());
    if link_type.contains("pubmed") || link_type.contains("pmid") {
        return pmid.map(|p| format!("pubmed:{}", p));
    }
    if link_type.contains("pmc") {
        if let Some(c) = pmcid_re().captures(&value) {
            let id = &c[1];
            return Some(format!("pmc:{}", id.strip_prefix("PMC").unwrap_or(id)));
        }
    }
    if link_type.contains("array design") && value.to_uppercase().starts_with("A-") {
        return Some(format!("arrayexpress.platform:{}", value));
    }
    if project_re().is_match(&value) {
        return Some(format!("insdc:{}", value.to_uppercase()));
    }
    // Older PageTab records commonly use the legacy ENA data-view URL. Map both
    // it and the current browser URL to the INSDC project identifier GrEBI's
    // ENA datasource uses, so the reference becomes an edge.
    if matches!(hostname.as_deref(), Some("ebi.ac.uk") | Some("www.ebi.ac.uk")) {
        if let Some(c) = ena_path_re().captures(&path) {
            return Some(format!("insdc:{}", c[1].to_uppercase()));
        }
    }
    if value.to_uppercase().starts_with("EMPIAR-") {
        return Some(format!("empiar:{}", value.strip_prefix("EMPIAR-").unwrap_or(&value)));
    }
    if scheme == "http" || scheme == "https" {
        return Some(value);
    }
    None
}

fn alternative_identifiers(accession: &str, collections: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let upper = accession.to_uppercase();
    if collections.iter().any(|c| c.to_lowercase() == "arrayexpress") && (upper.starts_with("A-") || upper.starts_with("E-")) {
        out.push(format!("arrayexpress:{}", accession));
    }
    if upper.starts_with("EMPIAR-") {
        out.push(format!("empiar:{}", accession.strip_prefix("EMPIAR-").unwrap_or(accession)));
    }
    if upper.starts_with("BIOMD") || upper.starts_with("MODEL") {
        out.push(format!("biomodels.db:{}", accession));
    }
    out
}

fn is_europe_pmc(accession: &str, collections: &[String]) -> bool {
    let lower = accession.to_lowercase();
    lower == "europepmc" || lower.starts_with("s-epmc") || collections.iter().any(|c| EUROPE_PMC_NAMES.contains(&c.to_lowercase().as_str()))
}

/// A metadata-only node for one submission, or None for a Europe PMC one.
fn parse_submission(submission: &Map<String, Value>) -> Result<Option<Map<String, Value>>, String> {
    let accession = submission.get("accno").map(stringify).unwrap_or_default().trim().to_string();
    if accession.is_empty() {
        return Err("BioStudies PageTab document has no accno".into());
    }
    let collections = deduplicate(attribute_values(submission, &["AttachTo"]));
    if is_europe_pmc(&accession, &collections) {
        return Ok(None);
    }
    let root_section = submission.get("section");
    let mut section_objects = Vec::new();
    sections(root_section, &mut section_objects);
    let empty = Map::new();
    let primary_section = section_objects.first().copied().unwrap_or(&empty);
    let mut titles = attribute_values(submission, &["Title"]);
    titles.extend(attribute_values(primary_section, &["Title"]));
    let title = first(&titles);
    let descriptions = deduplicate(attribute_values(primary_section, &["Description"]));
    let release_date = first(&attribute_values(submission, &["ReleaseDate", "Release Date"]));
    let section_type = primary_section.get("type").map(stringify).unwrap_or_default().trim().to_lowercase();
    let node_type = if section_type == "collection" || section_type == "project" { "biostudies:Collection" } else { "biostudies:Study" };

    let mut node = Map::new();
    node.insert("id".into(), Value::String(format!("biostudies:{}", accession)));
    node.insert("grebi:type".into(), Value::String(node_type.into()));
    if let Some(title) = title {
        node.insert("grebi:name".into(), Value::String(title));
    }
    if !descriptions.is_empty() {
        node.insert("grebi:description".into(), if descriptions.len() == 1 { Value::String(descriptions[0].clone()) } else { Value::Array(descriptions.iter().cloned().map(Value::String).collect()) });
    }
    if let Some(date) = release_date {
        node.insert("dcterms:issued".into(), Value::String(date));
    }
    if !collections.is_empty() {
        node.insert("biostudies:collection".into(), Value::Array(collections.iter().map(|c| Value::String(format!("biostudies:{}", c))).collect()));
    }
    let alternative_ids = alternative_identifiers(&accession, &collections);
    if !alternative_ids.is_empty() {
        node.insert("dcterms:identifier".into(), Value::Array(alternative_ids.into_iter().map(Value::String).collect()));
    }

    let (mut study_types, mut organisms, mut authors, mut organisations, mut ontology_terms, mut references) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for section in &section_objects {
        study_types.extend(attribute_values(section, &["Study type", "Study types", "Experimental Design", "Experimental Designs"]));
        organisms.extend(attribute_values(section, &["Organism"]));
        let current_type = section.get("type").map(stringify).unwrap_or_default().trim().to_lowercase();
        if current_type == "author" {
            authors.extend(attribute_values(section, &["Name"]));
        } else if current_type == "organization" || current_type == "organisation" {
            organisations.extend(attribute_values(section, &["Name"]));
        } else if current_type == "publication" {
            let publication_accession = section.get("accno").map(stringify).unwrap_or_default().trim().to_string();
            if let Some(c) = pmid_re().captures(&publication_accession) {
                references.push(format!("pubmed:{}", &c[1]));
            }
            for doi in attribute_values(section, &["DOI"]) {
                if let Some(reference) = doi_identifier(&doi) {
                    references.push(reference);
                }
            }
        }
        for attribute in attributes(section) {
            for qualifier in as_objects(attribute.get("valqual")) {
                if qualifier.get("name").map(stringify).unwrap_or_default().trim().to_lowercase() != "termid" {
                    continue;
                }
                let term_id = qualifier.get("value").map(stringify).unwrap_or_default().trim().to_string();
                if ontology_id_re().is_match(&term_id) {
                    ontology_terms.push(term_id);
                }
            }
        }
    }
    for section in &section_objects {
        for link in as_objects(section.get("links")) {
            if let Some(reference) = link_reference(link) {
                references.push(reference);
            }
        }
    }
    for (key, values) in [
        ("biostudies:studyType", study_types), ("biostudies:organism", organisms), ("dcterms:creator", authors),
        ("biostudies:organisation", organisations), ("biostudies:ontologyTerm", ontology_terms), ("dcterms:references", references),
    ] {
        let unique = deduplicate(values);
        if !unique.is_empty() {
            node.insert(key.into(), Value::Array(unique.into_iter().map(Value::String).collect()));
        }
    }
    Ok(Some(node))
}

/// The accession-level JSON files of a BioStudies FTP/NFS tree, in walk order.
fn page_tab_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else { return };
    let mut dirs: Vec<String> = Vec::new();
    let mut files: HashSet<String> = HashSet::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        match entry.file_type() {
            Ok(t) if t.is_dir() => {
                let lower = name.to_lowercase();
                if !EUROPE_PMC_NAMES.contains(&lower.as_str()) && lower != "files" {
                    dirs.push(name);
                }
            }
            Ok(_) => { files.insert(name); }
            Err(_) => {}
        }
    }
    let expected = format!("{}.json", root.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default());
    if files.contains(&expected) {
        out.push(root.join(expected));
    }
    dirs.sort();
    for dir in dirs {
        page_tab_files(&root.join(dir), out);
    }
}

fn load_document(path: &Path) -> Result<Map<String, Value>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    match serde_json::from_str::<Value>(&text) {
        Ok(Value::Object(o)) => Ok(o),
        Ok(_) => Err(format!("{}: expected a JSON object", path.display())),
        Err(e) => Err(e.to_string()),
    }
}

fn main() {
    let paths: Vec<PathBuf> = std::env::args().skip(1).filter(|a| a != "--").map(PathBuf::from).collect();
    let mut out = BufWriter::new(io::stdout().lock());
    let mut seen: HashSet<String> = HashSet::new();
    let mut emit = |source: &str, document: &Map<String, Value>, out: &mut BufWriter<io::StdoutLock>| {
        match parse_submission(document) {
            Ok(Some(node)) => {
                let id = node["id"].as_str().unwrap().to_string();
                if seen.insert(id) {
                    serde_json::to_writer(&mut *out, &Value::Object(node)).unwrap();
                    out.write_all(b"\n").unwrap();
                }
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("{}: {}", source, e);
                std::process::exit(1);
            }
        }
    };
    if paths.is_empty() {
        let mut text = String::new();
        io::stdin().read_to_string(&mut text).unwrap();
        match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(o)) => emit("stdin", &o, &mut out),
            _ => { eprintln!("stdin: expected a JSON object"); std::process::exit(1); }
        }
    }
    for path in paths {
        if !path.is_dir() {
            match load_document(&path) {
                Ok(document) => emit(&path.display().to_string(), &document, &mut out),
                Err(e) => { eprintln!("{}: {}", path.display(), e); std::process::exit(1); }
            }
            continue;
        }
        let mut candidates = Vec::new();
        page_tab_files(&path, &mut candidates);
        for candidate in candidates {
            // The public tree updates continuously: a submission listed by the
            // walk may be republished or withdrawn before it is read. Skip
            // vanished or half-written files instead of failing a multi-hour
            // whole-tree ingest.
            match load_document(&candidate) {
                Ok(document) => emit(&candidate.display().to_string(), &document, &mut out),
                Err(e) => eprintln!("WARNING: skipping {}: {}", candidate.display(), e),
            }
        }
    }
    out.flush().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("grebi_ingest_biostudies_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_walk_prunes_linked_files_and_europe_pmc() {
        let root = scratch_dir("walk");
        let included = root.join("S-BSST/001/S-BSST1");
        let linked_files = included.join("Files");
        let excluded = root.join("S-EPMC/001/S-EPMC1");
        std::fs::create_dir_all(&linked_files).unwrap();
        std::fs::create_dir_all(&excluded).unwrap();
        std::fs::write(included.join("S-BSST1.json"), "{}").unwrap();
        std::fs::write(linked_files.join("payload.json"), "{}").unwrap();
        std::fs::write(excluded.join("S-EPMC1.json"), "{}").unwrap();
        let mut found = Vec::new();
        page_tab_files(&root, &mut found);
        assert_eq!(found, vec![included.join("S-BSST1.json")]);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn half_written_and_vanished_files_are_errors_for_the_walk_to_skip() {
        let root = scratch_dir("load");
        let corrupt = root.join("S-BSST2.json");
        std::fs::write(&corrupt, "{\"accno\": \"S-BSST2\", \"att").unwrap();
        // A submission withdrawn between listing and reading behaves like a
        // broken symlink: present in the walk, gone on open.
        let vanished = root.join("S-BSST3.json");
        std::os::unix::fs::symlink(root.join("gone.json"), &vanished).unwrap();
        assert!(load_document(&corrupt).is_err());
        assert!(load_document(&vanished).is_err());
        assert!(load_document(&root.join("S-BSST1.json")).is_ok() == false);
        std::fs::write(root.join("S-BSST1.json"), "{\"accno\": \"S-BSST1\"}").unwrap();
        assert_eq!(load_document(&root.join("S-BSST1.json")).unwrap()["accno"], "S-BSST1");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn doi_links_normalise_url_and_prefixed_forms() {
        fn link(url: &str, link_type: Option<&str>) -> Option<String> {
            let mut object = Map::new();
            object.insert("url".into(), Value::String(url.into()));
            if let Some(link_type) = link_type {
                object.insert("attributes".into(), serde_json::json!([{"name": "Type", "value": link_type}]));
            }
            link_reference(&object)
        }
        for value in ["10.1234/abc", "doi:10.1234/abc", "DOI:10.1234/abc", "https://doi.org/10.1234/abc",
                      "http://dx.doi.org/10.1234/abc", "doi:https://doi.org/10.1234/abc"] {
            assert_eq!(link(value, Some("DOI")).as_deref(), Some("doi:10.1234/abc"), "{}", value);
        }
        assert_eq!(link("https://doi.org/10.1234/abc", None).as_deref(), Some("doi:10.1234/abc"));
        // A DOI-typed link whose value is not a DOI falls back to the URL
        // handling instead of minting a malformed doi: identifier.
        assert_eq!(link("https://example.org/paper", Some("DOI")).as_deref(), Some("https://example.org/paper"));
        assert_eq!(link("not a doi", Some("DOI")), None);
    }

    #[test]
    fn europe_pmc_submissions_are_excluded() {
        let submission: Map<String, Value> =
            serde_json::from_str(r#"{"accno": "S-EPMC123", "attributes": [], "section": {"type": "Study"}}"#).unwrap();
        assert!(parse_submission(&submission).unwrap().is_none());
        let submission: Map<String, Value> =
            serde_json::from_str(r#"{"accno": "S-BSST9", "attributes": [{"name": "AttachTo", "value": "EuropePMC"}]}"#).unwrap();
        assert!(parse_submission(&submission).unwrap().is_none());
        let submission: Map<String, Value> = serde_json::from_str(r#"{"section": {"type": "Study"}}"#).unwrap();
        assert!(parse_submission(&submission).is_err());
    }
}
