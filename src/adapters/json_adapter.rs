use std::{error::Error, fs::File, io::BufReader};

use serde_json::{Map, Value};

use crate::Readable;

#[derive(Debug)]
pub struct JsonAdapter {}

impl Readable for JsonAdapter {
    fn read(
        &self,
        file_path: &String,
        _config: &crate::Config,
        from: Option<u64>,
        len: u64,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>> {
        // Create file reader
        let file = File::open(file_path)?;
        let buf_reader = BufReader::new(file);

        // Create key value pair from json data
        let data: Vec<Map<String, Value>> = serde_json::from_reader(buf_reader)?;

        // Set from and to
        // min ensures it is within bounds
        let length = data.len() as u64;
        let from = from.unwrap_or(0).min(length);
        let to = (from + len).min(length);

        let data = data[from as usize..to as usize].iter().map(|i| i.clone()).collect::<Vec<_>>();

        Ok(data)
    }
}
