use std::collections::HashMap;
use std::time::{Instant, UNIX_EPOCH};
use anyhow::Result;
use crate::parser::Value;

pub struct Storage {
    storage: HashMap<String, DataContainer>
}

pub struct DataContainer {
    value: Value,
    creation_date: Instant,
    expire_in_mills: Option<u128>
}

impl Storage {
    pub fn new() -> Storage {
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