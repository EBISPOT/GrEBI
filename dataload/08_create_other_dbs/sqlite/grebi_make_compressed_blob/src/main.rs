use flate2::write::ZlibEncoder;
use flate2::Compression;
use grebi_shared::get_id;
use std::io::BufReader;
use std::io::BufRead;
use std::io::BufWriter;
use std::io;
use std::io::Write;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    let mut n:i64 = 0;

    let mut line:Vec<u8> = Vec::new();

    loop {

        line.clear();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            eprintln!("saw {} lines", n);
            break;
        }

        n = n + 1;

        let id = get_id(&line);

        writer.write_all(&(id.len() as u32).to_le_bytes()).unwrap();
        writer.write_all(id).unwrap();

        let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(9));

        // The embedding vector is already stored in Neo4j and Solr so we don't also need it in
        // the compressed blob for sqlite.
        //
        let (before, after) = remove_embedding_vector(&line);
        enc.write_all(&before).unwrap();
        enc.write_all(&after).unwrap();

        let compressed = enc.finish().unwrap();

        writer.write_all(&(compressed.len() as u32).to_le_bytes()).unwrap();
        writer.write_all(&compressed).unwrap();
    }

}

fn remove_embedding_vector(line: &Vec<u8>) -> (&[u8], &[u8]) {
    let pattern_start = br#","grebi:embeddingVector":["#;
    
    if let Some(start_idx) = line.windows(pattern_start.len())
                                 .position(|w| w == pattern_start) 
    {
        if let Some(end_idx) = line[start_idx..].iter().position(|&b| b == b']') {
            let before = &line[..start_idx];
            let after = &line[start_idx + end_idx + 1..];
            return (before, after);
        }
    }
    
    (&line[..], &[])
}
