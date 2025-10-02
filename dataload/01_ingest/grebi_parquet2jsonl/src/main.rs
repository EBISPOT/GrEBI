use std::io::{stdin, stdout, BufWriter, Read};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_json::writer::LineDelimitedWriter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut inbuf = Vec::new();
    stdin().lock().read_to_end(&mut inbuf)?;
    let bytes = Bytes::from(inbuf);

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?
        .with_batch_size(1024)
        .build()?;

    let mut out = BufWriter::new(stdout().lock());
    let mut writer = LineDelimitedWriter::new(&mut out);

    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
    }

    writer.finish()?;
    Ok(())
}

