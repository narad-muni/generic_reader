use std::{error::Error, fs::File, io::BufReader};

use serde_json::{Map, Value};

use crate::Readable;

#[derive(Debug)]
pub struct JsonArrayAdapter {}

impl Readable for JsonArrayAdapter {
    fn read(
        &self,
        file_path: &String,
        config: &crate::Config,
        from: Option<usize>,
        len: usize,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>> {
        // Create file reader
        let file = File::open(file_path)?;
        let buf_reader = BufReader::new(file);

        // Read data using buffer reader
        let values: Vec<Vec<Value>> = serde_json::from_reader(buf_reader)?;

        // Set from and to
        // min ensures it is within bounds
        let length = values.len();
        let mut from = from.unwrap_or(0).min(length);
        let mut to = (from + len).min(length);

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
                .ok_or("Empty data")?
                .iter()
                .map(|i| i.as_str().unwrap().to_string())
                .collect()
        };

        let mut data = vec![];

        // Collect data from slice
        values[from..to].into_iter().for_each(|val| {
            let mut hashmap = Map::new();

            for i in 0..val.len() {
                hashmap.insert(columns[i].clone(), Value::from(val[i].clone()));
            }

            data.push(hashmap);
        });

        Ok(data)
    }
}
