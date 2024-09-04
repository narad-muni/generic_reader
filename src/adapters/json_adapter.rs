use std::{fs::File, io::BufReader};

use serde_json::{Map, Value};

use crate::Readable;

#[derive(Debug)]
pub struct JsonAdapter {}

impl Readable for JsonAdapter {
    fn read(
        &self,
        file_path: &String,
        _config: &crate::Config,
        from: Option<usize>,
        to: Option<usize>,
    ) -> (Vec<String>, Vec<Vec<Value>>) {
        // Create file reader
        let file = File::open(file_path).unwrap();
        let buf_reader = BufReader::new(file);

        // Create key value pair from json data
        let data: Vec<Map<String, Value>> = serde_json::from_reader(buf_reader).unwrap();

        // Get first object to extract columns
        let first = data.get(0).unwrap();
        let columns: Vec<String> = first.keys().map(|i| i.clone().to_string()).collect();

        // Set from and to
        // min ensures it is within bounds
        let length = data.len();
        let from = from.unwrap_or(0).min(length);
        let to = to.unwrap_or(usize::MAX).min(length);

        // Iter through slice of data and collect values
        let data = data[from..to]
            .iter()
            .map(|i| i.values().into_iter().map(|j| j.clone()).collect())
            .collect();

        (columns, data)
    }
}
