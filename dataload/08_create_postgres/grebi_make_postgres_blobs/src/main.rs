use std::io::{self, BufReader, BufWriter, Read, Write};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Convert a stream of compressed blobs (little-endian u32 framing) into
/// PostgreSQL COPY BINARY format.
///
/// Input format (repeating):
///   [u32 LE id_len][id bytes][u32 LE blob_len][zlib-compressed blob bytes]
///
/// Output format:
///   Header: "PGCOPY\n\xff\r\n\0" (11 bytes) + i32 BE flags (0) + i32 BE ext (0)
///   Per row: i16 BE nfields (2) + i32 BE id_len + id bytes + i32 BE blob_len + blob bytes
///   Trailer: i16 BE -1

fn main() {
    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut writer = BufWriter::new(stdout);

    // Write PGCOPY header
    writer.write_all(b"PGCOPY\n\xff\r\n\0").unwrap();
    writer.write_all(&0i32.to_be_bytes()).unwrap(); // flags
    writer.write_all(&0i32.to_be_bytes()).unwrap(); // header extension area length

    let mut n: u64 = 0;

    loop {
        // Read id_len (u32 LE)
        let mut size_buf = [0u8; 4];
        match reader.read_exact(&mut size_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("read error: {}", e),
        }
        let id_len = u32::from_le_bytes(size_buf) as usize;

        let mut id_buf = vec![0u8; id_len];
        reader.read_exact(&mut id_buf).unwrap();

        // Read blob_len (u32 LE)
        reader.read_exact(&mut size_buf).unwrap();
        let blob_len = u32::from_le_bytes(size_buf) as usize;

        let mut blob_buf = vec![0u8; blob_len];
        reader.read_exact(&mut blob_buf).unwrap();

        // Write row: nfields=2, then (len, data) for each field
        writer.write_all(&2i16.to_be_bytes()).unwrap();

        writer.write_all(&(id_len as i32).to_be_bytes()).unwrap();
        writer.write_all(&id_buf).unwrap();

        writer.write_all(&(blob_len as i32).to_be_bytes()).unwrap();
        writer.write_all(&blob_buf).unwrap();

        n += 1;
    }

    // Write trailer
    writer.write_all(&(-1i16).to_be_bytes()).unwrap();

    eprintln!("Wrote {} rows to PGCOPY binary", n);
}
