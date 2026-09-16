use std::io::Write;

/// OID for PostgreSQL TEXT type (used in array binary encoding).
pub const PG_TEXT_OID: i32 = 25;

/// Writer for PostgreSQL COPY BINARY format.
///
/// Usage:
/// ```ignore
/// let mut w = PgCopyWriter::new(stdout);
/// w.begin_row(2);
/// w.write_text("hello");
/// w.write_int32(42);
/// let n = w.finish();
/// ```
pub struct PgCopyWriter<W: Write> {
    writer: W,
    row_count: u64,
}

impl<W: Write> PgCopyWriter<W> {
    pub fn new(mut writer: W) -> Self {
        writer.write_all(b"PGCOPY\n\xff\r\n\0").unwrap();
        writer.write_all(&0i32.to_be_bytes()).unwrap(); // flags
        writer.write_all(&0i32.to_be_bytes()).unwrap(); // header extension length
        Self { writer, row_count: 0 }
    }

    pub fn begin_row(&mut self, nfields: i16) {
        self.writer.write_all(&nfields.to_be_bytes()).unwrap();
        self.row_count += 1;
    }

    pub fn write_null(&mut self) {
        self.writer.write_all(&(-1i32).to_be_bytes()).unwrap();
    }

    /// Write raw bytes as a field (bytea, or any type where you provide the exact binary encoding).
    pub fn write_bytes(&mut self, data: &[u8]) {
        self.writer.write_all(&(data.len() as i32).to_be_bytes()).unwrap();
        self.writer.write_all(data).unwrap();
    }

    pub fn write_text(&mut self, s: &str) {
        self.write_bytes(s.as_bytes());
    }

    pub fn write_int32(&mut self, v: i32) {
        self.writer.write_all(&4i32.to_be_bytes()).unwrap();
        self.writer.write_all(&v.to_be_bytes()).unwrap();
    }

    pub fn write_int64(&mut self, v: i64) {
        self.writer.write_all(&8i32.to_be_bytes()).unwrap();
        self.writer.write_all(&v.to_be_bytes()).unwrap();
    }

    pub fn write_float64(&mut self, v: f64) {
        self.writer.write_all(&8i32.to_be_bytes()).unwrap();
        self.writer.write_all(&v.to_be_bytes()).unwrap();
    }

    /// Write a JSONB field. PostgreSQL binary JSONB format is: version byte (1) + JSON text.
    pub fn write_jsonb(&mut self, json_str: &str) {
        let len = 1 + json_str.len();
        self.writer.write_all(&(len as i32).to_be_bytes()).unwrap();
        self.writer.write_all(&[1u8]).unwrap();
        self.writer.write_all(json_str.as_bytes()).unwrap();
    }

    /// Write a TEXT[] array field.
    pub fn write_text_array(&mut self, values: &[&str]) {
        let mut buf: Vec<u8> = Vec::with_capacity(128);
        buf.extend_from_slice(&1i32.to_be_bytes());                         // ndim = 1
        buf.extend_from_slice(&0i32.to_be_bytes());                         // has-null flags
        buf.extend_from_slice(&PG_TEXT_OID.to_be_bytes());                  // element OID
        buf.extend_from_slice(&(values.len() as i32).to_be_bytes());        // dim size
        buf.extend_from_slice(&1i32.to_be_bytes());                         // lower bound
        for val in values {
            let bytes = val.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        self.write_bytes(&buf);
    }

    /// Write a pgvector `vector(dim)` field from f32 slice.
    /// pgvector binary send format: u16 dim, u16 unused(0), then dim × f32 big-endian.
    pub fn write_vector_f32(&mut self, values: &[f32]) {
        let len = 4 + values.len() * 4;
        self.writer.write_all(&(len as i32).to_be_bytes()).unwrap();
        self.writer.write_all(&(values.len() as u16).to_be_bytes()).unwrap();
        self.writer.write_all(&0u16.to_be_bytes()).unwrap();
        for &v in values {
            self.writer.write_all(&v.to_be_bytes()).unwrap();
        }
    }

    /// Write trailer and flush. Returns the number of rows written.
    pub fn finish(mut self) -> u64 {
        self.writer.write_all(&(-1i16).to_be_bytes()).unwrap();
        self.writer.flush().unwrap();
        self.row_count
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";

    fn body(w: PgCopyWriter<Vec<u8>>) -> Vec<u8> {
        // the trailer is a -1 field count
        let mut out = w.writer.clone();
        out.extend_from_slice(&(-1i16).to_be_bytes());
        out
    }

    #[test]
    fn writes_the_header_and_the_trailer_and_counts_rows() {
        let mut w = PgCopyWriter::new(Vec::new());
        assert_eq!(w.writer, HEADER);
        w.begin_row(1);
        w.write_null();
        w.begin_row(1);
        w.write_null();
        let expected = [HEADER, &[0, 1, 0xff, 0xff, 0xff, 0xff, 0, 1, 0xff, 0xff, 0xff, 0xff], &[0xff, 0xff]].concat();
        let snapshot = w.writer.clone();
        assert_eq!(w.finish(), 2);
        assert_eq!([snapshot, (-1i16).to_be_bytes().to_vec()].concat(), expected);
    }

    #[test]
    fn scalar_fields_are_length_prefixed_big_endian() {
        let mut w = PgCopyWriter::new(Vec::new());
        w.begin_row(4);
        w.write_text("hi");
        w.write_int32(-2);
        w.write_int64(3);
        w.write_float64(1.5);
        let mut expected = HEADER.to_vec();
        expected.extend_from_slice(&[0, 4]);
        expected.extend_from_slice(&[0, 0, 0, 2, b'h', b'i']);
        expected.extend_from_slice(&[0, 0, 0, 4, 0xff, 0xff, 0xff, 0xfe]);
        expected.extend_from_slice(&[0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3]);
        expected.extend_from_slice(&[0, 0, 0, 8]);
        expected.extend_from_slice(&1.5f64.to_be_bytes());
        expected.extend_from_slice(&[0xff, 0xff]);
        assert_eq!(body(w), expected);
    }

    #[test]
    fn jsonb_is_version_one_plus_the_text() {
        let mut w = PgCopyWriter::new(Vec::new());
        w.begin_row(1);
        w.write_jsonb("{\"a\":1}");
        let mut expected = HEADER.to_vec();
        expected.extend_from_slice(&[0, 1, 0, 0, 0, 8, 1]);
        expected.extend_from_slice(b"{\"a\":1}");
        expected.extend_from_slice(&[0xff, 0xff]);
        assert_eq!(body(w), expected);
    }

    #[test]
    fn text_arrays_use_the_one_dimensional_array_encoding() {
        let mut w = PgCopyWriter::new(Vec::new());
        w.begin_row(1);
        w.write_text_array(&["a", "bc"]);
        let mut elems = Vec::new();
        elems.extend_from_slice(&1i32.to_be_bytes()); // ndim
        elems.extend_from_slice(&0i32.to_be_bytes()); // no null bitmap
        elems.extend_from_slice(&PG_TEXT_OID.to_be_bytes());
        elems.extend_from_slice(&2i32.to_be_bytes()); // length
        elems.extend_from_slice(&1i32.to_be_bytes()); // lower bound
        elems.extend_from_slice(&[0, 0, 0, 1, b'a', 0, 0, 0, 2, b'b', b'c']);
        let mut expected = HEADER.to_vec();
        expected.extend_from_slice(&[0, 1]);
        expected.extend_from_slice(&(elems.len() as i32).to_be_bytes());
        expected.extend_from_slice(&elems);
        expected.extend_from_slice(&[0xff, 0xff]);
        assert_eq!(body(w), expected);

        let mut w = PgCopyWriter::new(Vec::new());
        w.begin_row(1);
        w.write_text_array(&[]);
        assert_eq!(&body(w)[HEADER.len() + 2..HEADER.len() + 6], &20i32.to_be_bytes(), "an empty array is just its 20 header bytes");
    }

    #[test]
    fn vectors_use_the_pgvector_send_format() {
        let mut w = PgCopyWriter::new(Vec::new());
        w.begin_row(1);
        w.write_vector_f32(&[1.0, -0.5]);
        let mut expected = HEADER.to_vec();
        expected.extend_from_slice(&[0, 1, 0, 0, 0, 12, 0, 2, 0, 0]);
        expected.extend_from_slice(&1.0f32.to_be_bytes());
        expected.extend_from_slice(&(-0.5f32).to_be_bytes());
        expected.extend_from_slice(&[0xff, 0xff]);
        assert_eq!(body(w), expected);
    }
}
