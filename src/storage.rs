use std::collections::HashMap;
use anyhow::Result;
use crate::parser::Value;

pub struct DataContainer {
    data: HashMap<String, Value>
}

impl DataContainer {
    pub fn new() -> DataContainer {
        DataContainer {
            data: HashMap::new()
        }
    }

    pub fn set(&mut self, key: &str, value: Value) -> Value {
        self.data.insert(key.to_string(), value);
        Value::SimpleString("OK".to_string())
    }

    pub fn get(&self, key: &str) -> Value {
        self.data.get(key).cloned().unwrap_or_else(|| Value::Null)
    }

    pub fn remove(&mut self, key: &str) -> Result<Value> {
        self.data.remove(key);
        Ok(Value::SimpleString("OK".to_string()))
    }
}