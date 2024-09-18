use std::{error::Error, fs::File, io::BufReader};

use csv::StringRecord;
use serde_json::{Map, Value};

use crate::Readable;

#[derive(Debug)]
pub struct CsvAdapter {}

impl Readable for CsvAdapter {
    fn read(
        &self,
        file_path: &String,
        config: &crate::Config,
        from: Option<u64>,
        len: u64,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>> {
        // Create file reader
        let file = File::open(file_path)?;
        let buf_reader = BufReader::new(file);

        // Create csv reader
        let mut reader = csv::Reader::from_reader(buf_reader);

        // Set columns depending on config
        // Either from config file defaults or from csv file
        if config.use_default_columns {
            // Set headers to default
            // This marks first entry as data rather than header
            reader.set_headers(StringRecord::from(config.default_columns.clone()));
        }

        let columns = reader.headers()?.clone();

        // Set from and to
        // min ensures it is within bounds
        let from = from.unwrap_or(0);

        let mut data = vec![];

        // Iter through slice of data and collect values
        for record in reader.records().skip(from as usize).take(len as usize) {
            let record = record?;

            let mut hashmap = Map::new();

            for (key, value) in columns.iter().zip(record.iter()) {
                hashmap.insert(key.to_string(), Value::from(value));
            }

            data.push(hashmap);
        }

        Ok(data)
    }
}
