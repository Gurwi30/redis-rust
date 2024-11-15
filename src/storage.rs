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
    redis_version_number: String,
    metadata: HashMap<String, String>,
}

impl RDBFile {
    pub fn from(file_path: String) -> Result<RDBFile> {
        if !file_path.ends_with(".rdb") {
            return Err(anyhow!("File does not end with '.rdb'"));
        }

        let contents = fs::read(file_path).unwrap();
        //println!("contents: {:?}", contents);

        let (redis_version_number, read_bytes) = read_from_until(&contents, 0, 0xFA).map(|data| (String::from_utf8(Vec::from(data.0)).unwrap(), data.1)).unwrap();
        println!("RBD File Header Version: {:?}", redis_version_number);


        // Skip the 0xFA byte and read the metadata
        let metadata = &contents[read_bytes + 1..];

        // Now, we need to read the attribute name and value
        // if let Some(attribute_name_end) = metadata.iter().position(|&x| x == 0x2D) {
        //     let attribute_name = &metadata[0..attribute_name_end];
        //     let attribute_value = &metadata[attribute_name_end + 1..];
        //
        //     // Convert bytes to string
        //     let attribute_name_str = String::from_utf8_lossy(attribute_name);
        //     let attribute_value_str = String::from_utf8_lossy(attribute_value);
        //
        //     println!("Attribute Name: {}", attribute_name_str);
        //     println!("Attribute Value: {}", attribute_value_str);
        // }

        println!("Metadata: {:?}", metadata);

        //
        // let metadata = read_from_until(&contents, read_bytes, 0xFE).map(|data| (data.0, data.1)).unwrap();
        // println!("RBD Metadata: {}", String::from_utf8_lossy(metadata.0));

        Ok(
            RDBFile {
                redis_version_number,
                metadata: HashMap::new(),
            }
        )
    }
}

fn read_from_until(data: &[u8], start: usize, until: i32) -> Option<(&[u8], usize)> {
    for i in (start + 1)..data.len() {
        let current_byte = data[i];

        if current_byte == until as u8 {
            return Some((&data[start..(i - 1)], i + 1));
        }
    }

    None
}