use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom},
};

use serde_json::{Map, Value};

use crate::Readable;

#[derive(Debug)]
pub struct JsonLineAdapter {}

impl Readable for JsonLineAdapter {
    fn read(
        &self,
        file_path: &String,
        _config: &crate::Config,
        from: Option<u64>,
        len: u64,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>> {
        // Create file reader with BufReader
        let file = File::open(&file_path)?;
        let mut buf_reader = BufReader::new(file);

        // Create buffer reader lines iterator
        let mut buf = String::new();
        buf_reader.read_line(&mut buf)?;

        // Reset reader to 0 after reading first line
        buf_reader.seek(SeekFrom::Start(0))?;

        // Set from and to
        let from = from.unwrap_or(0);

        // Create iter of lines
        let lines = buf_reader.lines();

        // Iterate through lines and collect data
        // Skip from lines and take (to - from) lines
        let mut data = vec![];

        for line in lines.skip(from as usize).take(len as usize) {
            if let Ok(line) = line {
                // Decode each line as json
                let json_obj = serde_json::from_str::<Map<String, Value>>(&line)?;

                data.push(json_obj);
            }
        }

        Ok(data)
    }
}
