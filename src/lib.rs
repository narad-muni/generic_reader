use std::{fmt::Debug, fs};

use adapters::{
    csv_adapter::CsvAdapter, json_lines_adapter::JsonLineAdapter, native_adapter::NativeAdapter,
};
use serde::Deserialize;
use serde_json::Value;

mod adapters;

use crate::adapters::{json_adapter::JsonAdapter, json_array_adapter::JsonArrayAdapter};

pub struct Reader {
    pub config: Config,
    pub settings: Settings,
}

/// Register adapter mappings here
fn get_adapter(_type: &Type) -> Box<dyn Readable> {
    match _type {
        Type::Json => Box::new(JsonAdapter {}),
        Type::JsonArray => Box::new(JsonArrayAdapter {}),
        Type::JsonLines => Box::new(JsonLineAdapter {}),
        Type::Native => Box::new(NativeAdapter {}),
        Type::Csv => Box::new(CsvAdapter {}),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Json,
    JsonArray,
    JsonLines,
    Csv,
    Native,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DType {
    Char,
    UInt,
    SInt,
    Float,
    Bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NativeColumn {
    name: String,
    dtype: DType,
    offset: usize,
    length: usize,
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
    pub native_columns: Vec<NativeColumn>,

    /// If true, it will use default columns
    #[serde(default)]
    pub use_default_columns: bool,

    /// List of columns to display
    /// Can be used in case column names are not available
    #[serde(default)]
    pub default_columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    file_path: String,
    #[serde(rename = "type")]
    _type: Type,
    config_path: String,
}

impl Reader {
    pub fn new(settings_path: String) -> Reader {
        let settings_file = fs::read_to_string(&settings_path).unwrap();
        let settings: Settings = serde_json::from_str(&settings_file).unwrap();

        let mut config_path = settings.config_path.clone();
        // make config path absolute from settings path
        if !config_path.starts_with("/") {
            let settings_dir = settings_path.split("/").collect::<Vec<&str>>()[0..2].join("/");
            config_path = format!("{settings_dir}/{config_path}");
        }

        let config_file = fs::read_to_string(&config_path).unwrap();
        let config = serde_json::from_str(&config_file).unwrap();

        Reader { config, settings }
    }

    pub fn all_columns(&mut self, all_columns: bool) {
        self.config.all_columns = all_columns;
    }

    pub fn read(&self, from: Option<usize>, to: Option<usize>) -> (Vec<String>, Vec<Vec<Value>>) {
        // Get adapter from mapping
        let adapter = get_adapter(&self.settings._type);

        adapter.read(&self.settings.file_path, &self.config, from, to)
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
        to: Option<usize>,
    ) -> (Vec<String>, Vec<Vec<Value>>);
}
