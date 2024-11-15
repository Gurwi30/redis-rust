use crate::parser::Value;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::time::Instant;

pub struct Storage {
    storage: HashMap<String, DataContainer>
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            storage: HashMap::new()
        }
    }

    pub fn load_from_rdb(path: String) -> Storage {
        RDBFile::from(path).expect("An error!!!!!!");

        Storage {
            storage: HashMap::new()
        }
    }

    pub fn set(&mut self, key: &str, value: Value, expire_in_mills: Option<u128>) -> Value {
        self.storage.insert(key.to_string(), DataContainer::create(value, expire_in_mills));
        Value::SimpleString("OK".to_string())
    }

    pub fn get(&mut self, key: &str) -> Value {
        match self.storage.get(key) {
            Some(container) => if !container.is_expired() {
                container.get_value()
            } else {
                self.storage.remove(&key.to_string());
                Value::NullBulkString
            }

            None => Value::NullBulkString
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<Value> {
        self.storage.remove(key);
        Ok(Value::SimpleString("OK".to_string()))
    }

    pub fn keys(&self) -> Vec<String> {
        self.storage.keys().map(|k| k.to_string()).collect()
    }
}

pub struct DataContainer {
    value: Value,
    creation_date: Instant,
    expire_in_mills: Option<u128>
}

impl DataContainer {
    pub fn create(value: Value, expire_in_mills: Option<u128>) -> DataContainer {
        DataContainer {
            value,
            creation_date: Instant::now(),
            expire_in_mills
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expire_in_mills {
            Some(expire_in_mills) => Instant::now().duration_since(self.creation_date).as_millis() >= expire_in_mills,
            None => false
        }
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }
}

struct RDBFile {
    version: String,
    metadata: HashMap<String, String>,
}

impl RDBFile {
    pub fn from(file_path: String) -> Result<RDBFile> {
        if !file_path.ends_with(".rdb") {
            return Err(anyhow!("File does not end with '.rdb'"));

        }

        let contents = fs::read(file_path);
        println!("contents: {:?}", contents);

        let version = read_from_until(&contents?, 0, b"FA").map(|bytes| String::from_utf8(Vec::from(bytes)).unwrap()).unwrap_or("0.0.0.0".to_string());

        println!("RBD Header File Version: {:?}", version);

        Ok(
            RDBFile {
                version,
                metadata: HashMap::new(),
            }
        )
    }
}

fn read_from_until<'a>(data: &'a Vec<u8>, start: usize, until: &[u8; 2]) -> Option<&'a[u8]> {
    for i in start..data.len() {
        let current_byte = data[i];
        let previous_byte = data[i - 1];

        if previous_byte == until[0] && current_byte == until[1] {
            return Some(&data[0..(i - 1)]);
        }
    }

    None
}