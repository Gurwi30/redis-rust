use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum ConfigKey {
    Dir,
    DbFilename,
}

impl ConfigKey {
    pub fn get_def_value(self) -> String {
        match self {
            ConfigKey::Dir => "DIR".into(),
            ConfigKey::DbFilename => "DB_FILENAME".into(),
        }
    }
}

pub struct Configuration {
    options: HashMap<ConfigKey, String>
}

impl Configuration {
    pub fn new() -> Configuration {
        Configuration {
            options: HashMap::new()
        }
    }

    pub fn set(&mut self, key: ConfigKey, value: &str) {
        self.options.insert(key, value.to_string());
        println!("Config Value {:?} set to {}", key, value)
    }

    pub fn get(&mut self, key: ConfigKey) -> String {
        self.options.get(&key).cloned().unwrap_or(key.get_def_value())
    }

    pub fn delete(&mut self, key: ConfigKey) {
        self.options.remove(&key);
    }

}