//! The MetaboLights EBI Search XML (eb-eye_metabolights_complete.xml) on stdin
//! as GrEBI JSONL: one node per <entry>, a study (MTBLS...) or a chemical
//! (MTBLC...), with its name, description, cross references, dates and
//! additional fields. Streams the XML rather than reading it all first.

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use serde_json::{Map, Value};
use std::io::{self, BufReader, BufWriter, Write};

#[derive(Default)]
struct Entry {
    id: String,
    name: Option<String>,
    description: Option<String>,
    refs: Vec<Value>,
    dates: Vec<(String, Value)>,
    fields: Vec<(String, Vec<Value>)>,
}

fn attribute(element: &BytesStart, name: &str) -> Option<String> {
    element.attributes().flatten()
        .find(|a| a.key.as_ref() == name.as_bytes())
        .map(|a| a.unescape_value().unwrap().into_owned())
}

fn text_or_null(text: Option<String>) -> Value {
    text.map(Value::String).unwrap_or(Value::Null)
}

impl Entry {
    fn field(&mut self, name: String, text: Option<String>) {
        let value = text_or_null(text);
        match self.fields.iter_mut().find(|(n, _)| *n == name) {
            Some((_, values)) => values.push(value),
            None => self.fields.push((name, vec![value])),
        }
    }

    fn into_json(self) -> Map<String, Value> {
        let mut node = Map::new();
        node.insert("id".into(), Value::String(self.id.clone()));
        node.insert("grebi:name".into(), text_or_null(self.name));
        node.insert("grebi:description".into(), text_or_null(self.description));
        node.insert("metabolights:ref".into(), Value::Array(self.refs.clone()));
        for (kind, value) in self.dates {
            node.insert(format!("metabolights:{}_date", kind), value);
        }
        for (name, values) in &self.fields {
            node.insert(format!("metabolights:{}", name), Value::Array(values.clone()));
        }
        if self.id.starts_with("MTBLS") {
            node.insert("grebi:type".into(), Value::String("metabolights:Study".into()));
        } else if self.id.starts_with("MTBLC") {
            node.insert("grebi:type".into(), Value::String("metabolights:Chemical".into()));
            // a chemical's identifiers: its cross references, InChI and formula
            let mut chemical = self.refs;
            for key in ["inchi", "formula"] {
                if let Some((_, values)) = self.fields.iter().find(|(n, _)| n == key) {
                    chemical.extend(values.iter().cloned());
                }
            }
            node.insert("metabolights:chemical".into(), Value::Array(chemical));
        } else {
            eprintln!("entry {} is neither a study (MTBLS) nor a chemical (MTBLC)", self.id);
            std::process::exit(1);
        }
        node
    }
}

fn main() {
    let mut reader = Reader::from_reader(BufReader::new(io::stdin().lock()));
    let mut out = BufWriter::new(io::stdout().lock());
    let mut buf = Vec::new();
    let mut entry: Option<Entry> = None;
    let mut entry_depth = 0usize;
    let mut depth = 0usize;
    // the element whose text is being collected: (name, attribute, text so far)
    let mut collecting: Option<(String, Option<String>, Option<String>)> = None;
    loop {
        match reader.read_event_into(&mut buf).unwrap_or_else(|e| panic!("bad XML at byte {}: {}", reader.buffer_position(), e)) {
            Event::Start(e) => {
                depth += 1;
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if name == "entry" {
                    entry = Some(Entry { id: attribute(&e, "id").unwrap_or_default(), ..Default::default() });
                    entry_depth = depth;
                } else if let Some(current) = entry.as_mut() {
                    match name.as_str() {
                        "name" | "description" if depth == entry_depth + 1 => collecting = Some((name, None, None)),
                        "field" => collecting = Some((name, attribute(&e, "name"), None)),
                        "ref" => current.refs.push(text_or_null(attribute(&e, "dbkey"))),
                        "date" => current.dates.push((attribute(&e, "type").unwrap_or_default(), text_or_null(attribute(&e, "value")))),
                        _ => {}
                    }
                }
            }
            Event::Empty(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if let Some(current) = entry.as_mut() {
                    match name.as_str() {
                        "ref" => current.refs.push(text_or_null(attribute(&e, "dbkey"))),
                        "date" => current.dates.push((attribute(&e, "type").unwrap_or_default(), text_or_null(attribute(&e, "value")))),
                        "field" => current.field(attribute(&e, "name").unwrap_or_default(), None),
                        "name" if depth + 1 == entry_depth + 1 => current.name = None,
                        "description" if depth + 1 == entry_depth + 1 => current.description = None,
                        _ => {}
                    }
                }
            }
            Event::Text(t) => {
                if let Some((_, _, text)) = collecting.as_mut() {
                    let piece = t.unescape().unwrap().into_owned();
                    *text = Some(text.take().unwrap_or_default() + &piece);
                }
            }
            Event::CData(t) => {
                if let Some((_, _, text)) = collecting.as_mut() {
                    let piece = String::from_utf8_lossy(&t).into_owned();
                    *text = Some(text.take().unwrap_or_default() + &piece);
                }
            }
            Event::End(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if let Some((collected, attr, text)) = collecting.take() {
                    if collected == name {
                        let current = entry.as_mut().unwrap();
                        match collected.as_str() {
                            "name" => current.name = text,
                            "description" => current.description = text,
                            "field" => current.field(attr.unwrap_or_default(), text),
                            _ => {}
                        }
                    } else {
                        collecting = Some((collected, attr, text));
                    }
                }
                if name == "entry" && depth == entry_depth {
                    let node = entry.take().unwrap().into_json();
                    serde_json::to_writer(&mut out, &Value::Object(node)).unwrap();
                    out.write_all(b"\n").unwrap();
                }
                depth -= 1;
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    out.flush().unwrap();
}
