use std::io::{self, BufReader, BufWriter, Read};

use grebi_shared::pgcopy::PgCopyWriter;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Convert a stream of compressed blobs (little-endian u32 framing) into
/// PostgreSQL COPY BINARY format.
///
/// Input format (repeating):
///   [u32 LE id_len][id bytes][u32 LE blob_len][zlib-compressed blob bytes]
///
/// Output: PGCOPY binary with 2 bytea columns (id, json).

fn main() {
    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut writer = PgCopyWriter::new(BufWriter::new(stdout));

    loop {
        let mut size_buf = [0u8; 4];
        match reader.read_exact(&mut size_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("read error: {}", e),
        }
        let id_len = u32::from_le_bytes(size_buf) as usize;

        let mut id_buf = vec![0u8; id_len];
        reader.read_exact(&mut id_buf).unwrap();

        reader.read_exact(&mut size_buf).unwrap();
        let blob_len = u32::from_le_bytes(size_buf) as usize;

        let mut blob_buf = vec![0u8; blob_len];
        reader.read_exact(&mut blob_buf).unwrap();

        writer.begin_row(2);
        writer.write_bytes(&id_buf);
        writer.write_bytes(&blob_buf);
    }

    let n = writer.finish();
    eprintln!("Wrote {} rows to PGCOPY binary", n);
}
