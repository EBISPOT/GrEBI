

use std::collections::{HashSet, HashMap, BTreeMap};
use std::{env, io};
use rocksdb::{DB, Options};
use csv;
use rusqlite::Connection;
use bloomfilter::Bloom;
use std::io::{BufRead, BufReader };
use std::io::{Write, BufWriter};

fn main() {

    let mut group_to_entities:BTreeMap<u64, HashSet<Vec<u8>>> = BTreeMap::new();
    let mut entity_to_group:BTreeMap<Vec<u8>, u64> = BTreeMap::new();

    let mut next_group_id:u64 = 0;

    let start_time = std::time::Instant::now();
    let mut n = 0;

    let stdin = io::stdin();
    let handle = stdin.lock();
    let mut reader = BufReader::new(handle);

    let mut stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    loop {
        let mut subject: Vec<u8> = Vec::new();
        reader.read_until(b'\t', &mut subject).unwrap();

        if subject.len() == 0 {
            break;
        }

        subject.pop(); // remove \t

        let mut object: Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut object).unwrap();

        object.pop(); // remove \n


        n = n + 1;
        if n % 1000000 == 0 {
            eprintln!("...{} equivalences in {} seconds", n, start_time.elapsed().as_secs());
        }

        if subject == object {
            continue;
        }

        let groupA:Option<u64> = entity_to_group.get(&subject).cloned();
        let groupB:Option<u64> = entity_to_group.get(&object).cloned();

        if groupA.is_some() {
            // A has a group
            let gA = groupA.unwrap();
            if groupB.is_some() {
                // B has a group
                let gB = groupB.unwrap();
                if gA == gB {
                    // already in the same group, nothing to do
                    continue
                }
                // A and B are in different groups
                // merge B into A
                let entities_in_b = group_to_entities.remove(&gB).unwrap();
                for e in entities_in_b.clone() {
                    entity_to_group.insert(e, gA);
                }
                let entities_in_a = group_to_entities.get_mut(&gA).unwrap();
                entities_in_a.extend(entities_in_b);

            } else {
                // A has a group and B doesn't
                // Put B in A's group
                entity_to_group.insert(object.clone(), gA);
                group_to_entities.get_mut(&gA).unwrap().insert(object);
            }
        } else {
            // A does not have a group
            if groupB.is_some() {
                let gB = groupB.unwrap();
                // B has a group but A does not
                // Put A in B's group
                entity_to_group.insert(subject.clone(), gB);
                group_to_entities.get_mut(&gB).unwrap().insert(subject);
            } else {
                // Neither A nor B have a group.
                // Put both into a new group.

                let group_id = next_group_id;
                next_group_id = next_group_id + 1;

                entity_to_group.insert(subject.clone(), group_id);
                entity_to_group.insert(object.clone(), group_id);
                group_to_entities.insert(group_id, HashSet::from([subject, object]));
            }
        }
    }

    eprintln!("Loaded {} equivalences in {} seconds", n, start_time.elapsed().as_secs());

    let start_time2 = std::time::Instant::now();
    let mut n2 = 0;


    for group in group_to_entities {

        n2 = n2 + 1;

        writer.write_all("group_".as_bytes()).unwrap();
        writer.write_all(group.0.to_string().as_bytes()).unwrap();
        writer.write_all("\t".as_bytes()).unwrap();

        let mut is_first_value = true;

        for entity in group.1 {
            if is_first_value {
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
