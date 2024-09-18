use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
};

use serde_json::{Map, Value};

use crate::Readable;

use super::utils::byte_utils::cast_bytes;

#[derive(Debug)]
pub struct NativeAdapter {}

impl Readable for NativeAdapter {
    fn read(
        &self,
        file_path: &String,
        config: &crate::Config,
        from: Option<u64>,
        len: u64,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>> {
        // Create file reader and BufReader
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);

        // Get last col to calculate packet_size
        // It is calculated by last_offset + length
        let last_col = config
            .native_columns
            .iter()
            .max_by_key(|i| i.offset)
            .ok_or("Empty data")?;

        // Calculate packet_size
        let packet_size = last_col.offset.unwrap_or(0) + last_col.length;
        let mut buf = [0; 1024];

        // Get column details from config
        let mut native_columns = config.native_columns.clone();

        // Sort columns by offset in ascending order
        native_columns.sort_by_key(|i| i.offset);

        // Set from and to
        let from = from.unwrap_or(0);

        // Seek till n packets, where n = form
        // Which is calculated by (from * packet_size)
        // Seek takes n bytes
        buf_reader.seek(SeekFrom::Start(from * packet_size as u64))?;

        let mut data = vec![];
        let mut pos = 0;

        // Read into buf for packet size
        while let Ok(_) = buf_reader.read_exact(&mut buf[0..packet_size]) {
            // Break if pos is GE than to
            if pos >= len {
                break;
            }

            let mut hashmap = Map::new();

            // Cast for each column
            for col in &native_columns {
                // Get slice from buf
                let buf = &buf[col.offset.unwrap_or(0)..(col.offset.unwrap_or(0) + col.length)];

                // Convert byte array to required type
                let val = cast_bytes(buf, &col.dtype)?;

                hashmap.insert(col.name.clone(), Value::from(val));
            }

            data.push(hashmap);

            pos += 1;
        }

        Ok(data)
    }
}
