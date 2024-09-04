use std::{fs::File, io::BufReader};

use csv::StringRecord;
use serde_json::Value;

use crate::Readable;

#[derive(Debug)]
pub struct CsvAdapter {}

impl Readable for CsvAdapter {
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

        // Create csv reader
        let mut reader = csv::Reader::from_reader(buf_reader);

        // Set columns depending on config
        // Either from config file defaults or from csv file
        let columns = if config.use_default_columns {
            // Set headers to default
            // This marks first entry as data rather than header
            reader.set_headers(StringRecord::from(config.default_columns.clone()));

            config.default_columns.clone()
        } else {
            reader
                .byte_headers()
                .unwrap()
                .iter()
                .map(|i| String::from_utf8(i.to_vec()).unwrap())
                .collect()
        };

        // Set from and to
        // min ensures it is within bounds
        let from = from.unwrap_or(0);
        let to = to.unwrap_or(usize::MAX);

        let mut data = vec![];

        // Iter through slice of data and collect values
        for result in reader.byte_records().skip(from).take(to - from) {
            if let Ok(record) = result {
                // Convert record to vector of values
                data.push(
                    record
                        .iter()
                        .map(|i| Value::String(String::from_utf8_lossy(i).to_string()))
                        .collect(),
                );
            }
        }

        (columns, data)
    }
}
