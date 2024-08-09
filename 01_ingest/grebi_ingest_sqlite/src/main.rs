use rusqlite::{params, Connection, Result};
use serde_json::{json, Value};
use inflector::Inflector;
use std::collections::HashMap;
use clap::Parser;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    datasource_name: String,

    #[arg(long)]
    filename: String,

}

fn main() -> Result<()> {

    let args = Args::parse();
    let conn = Connection::open(&args.filename)?;
    let prefix = args.datasource_name.to_lowercase();

    let schema_info = get_schema_info(&conn)?;
    let foreign_keys = get_foreign_keys(&conn)?;

    for (table, columns) in &schema_info {

        if table.starts_with("sqlite_") {
            continue;
        }

        let grebi_type = format!("{}:{}", prefix, table.to_singular());

        eprintln!("--- Reading table: {} => {}", table, grebi_type);

        let primary_keys = get_primary_keys(&conn, table).unwrap();
        eprintln!("\tcolumns: {:?}", columns);
        eprintln!("\tprimary keys: {:?}", primary_keys);

        let mut stmt = conn.prepare(&format!("SELECT * FROM {}", table)).unwrap();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {

            let mut json_obj = json!({});
            let mut ids = Vec::new();

            for (idx, column) in columns.iter().enumerate() {

                let value:Option<String> = match row.get(idx)? {
                    rusqlite::types::Value::Null => None,
                    rusqlite::types::Value::Integer(i) => Some(i.to_string()),
                    rusqlite::types::Value::Real(r) => Some(r.to_string()),
                    rusqlite::types::Value::Text(t) => Some(t.to_string()),
                    rusqlite::types::Value::Blob(b) => Some(hex::encode(b))
                };

                if value.is_none() {
                    continue;
                }

                let v = value.unwrap();

                let col_name = format!("{}:{}", prefix, column);

                if primary_keys.len() == 1 && &primary_keys[0] == column {
                    ids.push(format!("{}:{}:{}", prefix, table.to_singular(), v.clone()));
                }

                let fk_info = foreign_keys.get(&(table.clone(), column.clone()));

                if fk_info.is_some() {
                    json_obj[&col_name] = json!(format!("{}:{}:{}", prefix, fk_info.unwrap().0.to_singular(), v));
                } else {
                    json_obj[&col_name] = json!(v);
                }
            }

            json_obj["grebi:type"] = json!(grebi_type);
            json_obj["id"] = json!(ids);

            println!("{}", serde_json::to_string(&json_obj).unwrap());
        }
    }
    Ok(())
}

fn get_schema_info(conn: &Connection) -> Result<HashMap<String, Vec<String>>> {
    let mut schema_info = HashMap::new();
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let tables = stmt.query_map(params![], |row| row.get(0))?;

    for table in tables {
        let table: String = table?;
        let mut columns = Vec::new();
        let mut col_stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
        let col_info = col_stmt.query_map(params![], |row| row.get(1))?;

        for col in col_info {
            columns.push(col?);
        }
        schema_info.insert(table, columns);
    }
    Ok(schema_info)
}

fn get_primary_keys(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut primary_keys = Vec::new();
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let col_info = stmt.query_map(params![], |row| {
        let name: String = row.get(1)?;
        let is_pk: bool = row.get(5)?;
        Ok((name, is_pk))
    })?;

    for col in col_info {
        let (name, is_pk) = col?;
        if is_pk {
            primary_keys.push(name);
        }
    }
    Ok(primary_keys)
}

fn get_foreign_keys(conn: &Connection) -> Result<HashMap<(String, String), (String, String)>> {
    let mut foreign_keys = HashMap::new();
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let tables = stmt.query_map(params![], |row| row.get(0))?;

    for table in tables {
        let table: String = table?;
        let mut fk_stmt = conn.prepare(&format!("PRAGMA foreign_key_list({})", table))?;
        let fk_info = fk_stmt.query_map(params![], |row| {
            let from: String = row.get(3)?;
            let to_table: String = row.get(2)?;
            let to: String = row.get(4)?;
            Ok((from, to_table, to))
        })?;

        for fk in fk_info {
            let (from, to_table, to) = fk?;
            foreign_keys.insert((table.clone(), from), (to_table, to));
        }
    }
    Ok(foreign_keys)
}

