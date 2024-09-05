use std::{fs::File, io::BufReader};

use serde_json::Value;

use crate::Readable;

#[derive(Debug)]
pub struct JsonArrayAdapter {}

impl Readable for JsonArrayAdapter {
    fn read(
        &self,
        file_path: &String,
        config: &crate::Config,
        from: Option<usize>,
        to: Option<usize>,
    ) -> (Vec<String>, Vec<Vec<Value>>) {
        // Create file reader
        let file = File::open(file_path).unwrap();
        let buf_reader = BufReader::new(file);

        // Read data using buffer reader
        let values: Vec<Vec<Value>> = serde_json::from_reader(buf_reader).unwrap();

        // Set from and to
        // min ensures it is within bounds
        let length = values.len();
        let mut from = from.unwrap_or(0).min(length);
        let mut to = to.unwrap_or(usize::MAX).min(length);

        // Get first object
        let columns: Vec<String> = if config.use_default_columns {
            config.default_columns.clone()
        } else {
            // Increment from & to because first val is header
            from += 1;
            from = from.min(length);

            to += 1;
            to = to.min(length);

            values
                .get(0)
                .unwrap()
                .iter()
                .map(|i| i.as_str().unwrap().to_string())
                .collect()
        };

        // Collect data from slice
        let data = values[from..to]
            .into_iter()
            .map(|i| i.clone())
            .collect();

        (columns, data)
    }
}
