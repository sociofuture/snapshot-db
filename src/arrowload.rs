//! `LOAD DATA ... FORMAT ARROW`: the "file" is an Arrow IPC stream (schema message, record
//! batches, end marker), produced in memory by the client, e.g. with Arrow Java.
//! Bytes arrive in arbitrary chunks; the stream decoder reassembles messages across chunks.

use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::ipc::reader::StreamDecoder;

use crate::arena::Arena;
use crate::table::Table;

pub struct ArrowLoader {
    decoder: StreamDecoder,
}

impl ArrowLoader {
    pub fn new() -> Self {
        ArrowLoader { decoder: StreamDecoder::new() }
    }

    pub fn feed(&mut self, chunk: &[u8], table: &mut Table, arena: &mut Arena) -> Result<(), String> {
        let mut buf = Buffer::from(chunk.to_vec());
        while !buf.is_empty() {
            let before = buf.len();
            let batch = self.decoder.decode(&mut buf).map_err(|e| format!("invalid Arrow stream: {e}"))?;
            if let Some(batch) = batch {
                table.push_batch(arena, &batch)?;
            } else if buf.len() == before {
                break; // needs more bytes
            }
        }
        Ok(())
    }

    /// The stream must have been complete (end-of-stream marker seen).
    pub fn finish(&mut self) -> Result<(), String> {
        self.decoder.finish().map_err(|e| format!("incomplete Arrow stream: {e}"))
    }
}
