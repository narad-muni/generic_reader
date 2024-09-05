use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
};

use serde_json::Value;

use crate::{DType, Readable};

#[derive(Debug)]
pub struct NativeAdapter {}

fn cast_bytes(buf: &[u8], dtype: &DType) -> Result<Value, Box<dyn Error>> {
    Ok(match dtype {
        DType::Char => Value::String(String::from_utf8(buf.to_vec())?),
        DType::Bool => Value::Bool(buf[0] != 0),
        DType::UInt => Value::Number(serde_json::Number::from(u32::from_be_bytes(
            buf.try_into()?,
        ))),
        DType::SInt => Value::Number(serde_json::Number::from(i32::from_be_bytes(
            buf.try_into()?,
        ))),
        DType::Float => Value::Number(
            serde_json::Number::from_f64(f64::from_be_bytes(buf.try_into()?)).ok_or("NaN")?,
        ),
    })
}

impl Readable for NativeAdapter {
    fn read(
        &self,
        file_path: &String,
        config: &crate::Config,
        from: Option<usize>,
        to: Option<usize>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>), Box<dyn Error>> {
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
        let packet_size = last_col.offset + last_col.length;
        let mut buf = [0; 1024];

        // Get column names from config
        let columns = config.native_columns.iter().map(|i| i.name.clone());

        // Get column details from config
        let mut native_columns = config.native_columns.clone();

        // Sort columns by offset in ascending order
        native_columns.sort_by_key(|i| i.offset);

        // Set from and to
        let from = from.unwrap_or(0);
        let to = to.unwrap_or(usize::MAX);

        // Seek till n packets, where n = form
        // Which is calculated by (from * packet_size)
        // Seek takes n bytes
        buf_reader.seek(SeekFrom::Start((from * packet_size) as u64))?;

        let mut data = vec![];
        let mut pos = 0;

        // Read into buf for packet size
        while let Ok(_) = buf_reader.read_exact(&mut buf[0..packet_size]) {
            // Break if pos is GE than to
            if pos >= (to-from) {
                break;
            }

            let mut arr = vec![];

            // Cast for each column
            for col in &native_columns {
                // Get slice from buf
                let buf = &buf[col.offset..(col.offset + col.length)];

                // Convert byte array to required type
                let val = cast_bytes(buf, &col.dtype)?;

                arr.push(val);
            }

            data.push(arr);

            pos += 1;
        }

        Ok((columns.collect(), data))
    }
}
