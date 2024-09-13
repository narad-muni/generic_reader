use std::{collections::HashMap, error::Error, fmt::Debug, fs};

use adapters::{
    csv_adapter::CsvAdapter, json_lines_adapter::JsonLineAdapter,
    multi_native_adapter::MultiNative, native_adapter::NativeAdapter,
};
use serde::Deserialize;
use serde_json::{Map, Value};

mod adapters;

use crate::adapters::{json_adapter::JsonAdapter, json_array_adapter::JsonArrayAdapter};

pub struct Reader {
    pub config: Config,
    pub file_path: String,
    pub _type: Type,
}

/// Used to define a value in buffer block
/// e.g. in buffer of 512 bytes
/// username starts at 3rd byte, has length of 7, is of type char array
#[derive(Debug, Default, Deserialize, Clone)]
pub struct BufferValue {
    #[serde(default)]
    name: String,
    #[serde(default)]
    dtype: DType,
    offset: Option<usize>,
    length: usize,
    #[serde(default)]
    default: bool,
}

#[derive(Debug, Default, Deserialize)]
pub struct PacketHeader {
    packet_size: BufferValue,
    timestamp: BufferValue,
}

#[derive(Debug, Default, Deserialize)]
pub struct PacketColumns {
    expected_size: u32,
    columns: Vec<BufferValue>,
}

#[derive(Debug, Default, Deserialize,PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    #[default]
    Lzo,
}

#[derive(Debug, Default, Deserialize)]
pub struct PacketInfo {
    no_of_packets: BufferValue,
    compressed_packet_size: BufferValue,
    compresseion_type: CompressionType,
    packet_size: BufferValue,
    packet_identifier: BufferValue,
    column_details: HashMap<u64, PacketColumns>,
}

#[derive(Debug, Default, Deserialize)]
pub struct NativeSettings {
    packet_header: PacketHeader,
    packet_info: PacketInfo,
}

/// Register adapter mappings here
fn get_adapter(_type: &Type) -> Box<dyn Readable> {
    match _type {
        Type::Json => Box::new(JsonAdapter {}),
        Type::JsonArray => Box::new(JsonArrayAdapter {}),
        Type::JsonLines => Box::new(JsonLineAdapter {}),
        Type::Native => Box::new(NativeAdapter {}),
        Type::Csv => Box::new(CsvAdapter {}),
        Type::MultiNative => Box::new(MultiNative {}),
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Json,
    JsonArray,
    JsonLines,
    Csv,
    Native,
    MultiNative,
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DType {
    Char,
    UInt,
    Short,
    SInt,
    Float,
    Bool,
    Byte,
    #[default]
    None,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    /// flag to select all columns
    pub all_columns: bool,

    /// Selected columns to display
    pub selected_columns: Vec<String>,

    /// column name with size for native file format
    /// First is column name
    /// Second is datatype of column
    /// Third is (offset, length) if buffer which contains data
    /// We need to caste the buffer to defined type
    #[serde(default)]
    pub native_columns: Vec<BufferValue>,

    /// Used for native files.
    /// can be configured to read any number of columns
    #[serde(default)]
    pub native: NativeSettings,

    /// If true, it will use default columns
    #[serde(default)]
    pub use_default_columns: bool,

    /// List of columns to display
    /// Can be used in case column names are not available
    #[serde(default)]
    pub default_columns: Vec<String>,
}

impl Reader {
    pub fn new(
        config_path: String,
        file_path: String,
        _type: Type,
    ) -> Result<Reader, Box<dyn Error>> {
        // Load config to struct
        let config_file = fs::read_to_string(&config_path)?;
        let config = serde_json::from_str(&config_file)?;

        Ok(Reader {
            config,
            file_path,
            _type,
        })
    }

    pub fn new_with_config(config: Config, file_path: String, _type: Type) -> Reader {
        Reader {
            config,
            file_path,
            _type,
        }
    }

    pub fn all_columns(&mut self, all_columns: bool) {
        self.config.all_columns = all_columns;
    }

    pub fn read(
        &self,
        from: Option<usize>,
        len: Option<usize>,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>> {
        // Get adapter from mapping
        let adapter = get_adapter(&self._type);

        let len = len.unwrap_or(10);

        adapter.read(&self.file_path, &self.config, from, len)
    }

    pub fn get_columns(config: Config, _type: Type) -> Map<String, Value> {
        let mut columns = Map::new();

        // For csv like structures
        if (_type == Type::Csv || _type == Type::JsonArray) && config.use_default_columns {
            config.default_columns.iter().for_each(|c| {
                columns.insert(c.to_string(), Value::Bool(
                    // If column is in selected columns
                    config.selected_columns.contains(&c.to_string())
                ));
            });
        }else if _type == Type::Native {
            config.native_columns.iter().for_each(|c| {
                columns.insert(c.name.to_string(), Value::Bool(c.default));
            });
        }else if _type == Type::MultiNative {
            config.native.packet_info.column_details.values().for_each(|c| {
                c.columns.iter().for_each(|c| {
                    if columns.get(&c.name).is_none() || columns.get(&c.name).unwrap() == false {
                        columns.insert(c.name.to_string(), Value::Bool(c.default));
                    }
                });
            })
        }

        columns
    }
}

pub trait Readable: Send + Sync + Debug {
    /// read method should read from file_path
    /// It should parse according to it's implementation and config file
    /// If from is set, it should read from that index
    /// If to is set, it should read till that index excluding it
    /// Returns (columns, values)
    fn read(
        &self,
        file_path: &String,
        config: &Config,
        from: Option<usize>,
        len: usize,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>>;
}
