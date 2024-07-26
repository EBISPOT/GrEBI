
use std::io::{BufWriter, self, BufReader, Write,BufRead};
use std::io::StdoutLock;

fn main() -> std::io::Result<()> {

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    output_nodes.write_all(b"id:ID,:LABEL\n").unwrap();

    loop {

        let mut line:Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }
        if line[line.len() - 1] == b'\n' {
            line.pop();
        }
        output_nodes.write_all(b"\"").unwrap();
        write_escaped_value(&line, &mut output_nodes);
        output_nodes.write_all(b"\",\"Id\"\n").unwrap();
    }

    Ok(())
}

fn write_escaped_value(buf:&[u8], writer:&mut BufWriter<StdoutLock>) {

    for byte in buf.iter() {
        match byte {
            b'\n' => writer.write_all(b"\\n").unwrap(),
            b'\r' => writer.write_all(b"\\r").unwrap(),
            b'\t' => writer.write_all(b"\\t").unwrap(),
            b'\\' => writer.write_all(b"\\\\").unwrap(),
            b'"' => writer.write_all(b"\"\"").unwrap(),
            b => writer.write_all(&[*b]).unwrap(),
        }
    }
}

