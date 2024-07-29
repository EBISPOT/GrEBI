
use std::collections::{HashSet, HashMap, BTreeMap};
use std::{env, io};
use csv;
use rusqlite::Connection;
use bloomfilter::Bloom;
use clap::Parser;
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    add_group: Vec<String>,

    #[arg(long)]
    add_prefix: String // used to prepend the subgraph name like hra_kg:g:
}


fn main() {

	let mut group_to_entities:BTreeMap<u64, HashSet<Vec<u8>>> = BTreeMap::new();
	let mut entity_to_group:BTreeMap<Vec<u8>, u64> = BTreeMap::new();

	let mut next_group_id:u64 = 1;

	let args = Args::parse();
	let add_group:Vec<String> = args.add_group;
	for group in add_group {
		let entries:HashSet<Vec<u8>> = group.split(",").map(|s| s.as_bytes().to_vec()).collect();
		let gid = next_group_id;
		next_group_id = next_group_id + 1;
		for id in &entries {
			entity_to_group.insert(id.to_vec(), gid);
		}
		group_to_entities.insert(gid, entries);
	}

	let start_time = std::time::Instant::now();
	let mut n = 0;

	let stdin = io::stdin();
	let handle = stdin.lock();
	let mut reader = BufReader::new(handle);

	let mut stdout = io::stdout().lock();
	let mut writer = BufWriter::new(stdout);

	loop {
		let mut line: Vec<u8> = Vec::new();
		reader.read_until(b'\n', &mut line).unwrap();

		n = n + 1;
		if n % 1000000 == 0 {
			eprintln!("...{} lines in {} seconds", n, start_time.elapsed().as_secs());
		}


		if line.len() == 0 {
			break;
		}
		if line[line.len() - 1] == b'\n' {
			line.pop();
		}

		let mut ids:Vec<Vec<u8>> = line.split(|&byte| byte == b'\t').map(|id| id.to_vec()).collect();

		let mut target_group:u64 = 0;
		for id in &ids {
			let g = entity_to_group.get(id);
			if g.is_some() {
				target_group = *g.unwrap();
				break;
			}
		}

		if target_group != 0 {
			// at least one of the ids already had a group;
			// put everything else into it
			for id in &ids {
				let g2 = entity_to_group.get(id);
				if g2.is_some() && *g2.unwrap() != target_group {
					// this id already had a group different to ours
					let entities_in_b = group_to_entities.remove(&g2.unwrap()).unwrap();
					for e in entities_in_b.clone() {
						entity_to_group.insert(e, target_group);
					}
					let entities_in_a = group_to_entities.get_mut(&target_group).unwrap();
					entities_in_a.extend(entities_in_b);
				} else {
					// this id didn't have a group
					entity_to_group.insert(id.to_vec(), target_group);
					group_to_entities.get_mut(&target_group).unwrap().insert(id.to_vec());
				}
			}
		} else {

			// none of the ids had a group so we make a new one
			target_group = next_group_id;
			next_group_id = next_group_id + 1;
			for id in &ids {
				entity_to_group.insert(id.to_vec(), target_group);
			} 
			group_to_entities.insert(target_group, ids.iter().map(|id| id.to_vec()).collect::<HashSet<_>>());
		}
	}

	eprintln!("Loaded {} lines in {} seconds", n, start_time.elapsed().as_secs());

	let start_time2 = std::time::Instant::now();
	let mut n2 = 0;


	for group in group_to_entities {

		n2 = n2 + 1;

		// writer.write_all("group_".as_bytes()).unwrap();
		// writer.write_all(group.0.to_string().as_bytes()).unwrap();
		// writer.write_all("\t".as_bytes()).unwrap();

		let mut sorted_ids:Vec<&Vec<u8>> = group.1.iter().collect();
		sorted_ids.sort_unstable_by(|a, b| id_score(a).cmp(&id_score(b)));

		let mut is_first_value = true;

		for entity in sorted_ids {
			if is_first_value {
				writer.write_all(&args.add_prefix.as_bytes()).unwrap();
				writer.write_all(entity.as_slice()).unwrap();
				writer.write_all("\t".as_bytes()).unwrap();
				is_first_value = false;
			} else {
				writer.write_all("\t".as_bytes()).unwrap();
			}
			writer.write_all(entity.as_slice()).unwrap();
		}

		writer.write_all("\n".as_bytes()).unwrap();
	}

	eprintln!("Wrote {} groups in {} seconds", n2, start_time2.elapsed().as_secs());

}


// From the equivalence group, try to pick an ID which will be obvious in Neo4j.
// Prefer:
//      - CURIEs
//      - textual (readable) IDs rather than numeric
//      - "grebi:" IDs always win (used to consolidate names on grebi:name etc.)
// lower score is better
//
fn id_score(id:&[u8]) -> i32 {

	if id.starts_with(b"grebi:") {
		return i32::MIN;
	}

	let mut score = 0;

	if id.contains(&b':') && !id.starts_with(b"http") {
		score = score - 1000; // curie-like
	}

	for c in id {
		if c.is_ascii_alphabetic() {
			score = score + 1;
		}
	}

	return score;
}

