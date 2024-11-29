use crate::parser::{Type, Value};
use anyhow::{anyhow, Result};
use bytes::Buf;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct Storage {
    values: HashMap<String, DataContainer>
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            values: HashMap::new()
        }
    }

    pub fn load_from_rdb(path: String) -> Result<Storage> {
        let rdb_file = RDBFile::from(path)?;

        Ok(
            Storage {
                values: rdb_file.data
            }
        )
    }

    pub fn import_data(&mut self, rdb_file: RDBFile) {
        self.values.extend(rdb_file.data);
    }

    pub fn set(&mut self, key: &str, value: Value, expire: Option<SystemTime>) -> Value {
        self.values.insert(key.to_string(), DataContainer::create(value, expire));
        Value::SimpleString("OK".to_string())
    }

    pub fn add_all(&mut self, values: HashMap<String, DataContainer>) {
        self.values.extend(values)
    }

    pub fn get(&mut self, key: &str) -> Option<Value> {
        match self.values.get(key) {
            Some(container) => if !container.is_expired() {
                Some(container.get_value())
            } else {
                self.values.remove(&key.to_string());
                None
            }

            None => None
        }
    }

    pub fn get_specific(&mut self, value_type: Type) -> Vec<DataContainer> {
        self.values
            .values()
            .filter(move |data_container| data_container.value.get_type() == value_type)
            .cloned()
            .collect()
    }

    pub fn remove(&mut self, key: &str) -> Result<Value> {
        self.values.remove(key);
        Ok(Value::SimpleString("OK".to_string()))
    }

    pub fn keys(&self) -> Vec<String> {
        self.values.keys().map(|k| k.to_string()).collect()
    }

    pub fn get_all(self) -> HashMap<String, DataContainer> {
        self.values
    }
}

#[derive(Clone, Debug)]
pub struct DataContainer {
    value: Value,
    expire: Option<SystemTime>
}

impl DataContainer {
    pub fn create(value: Value, expire: Option<SystemTime>) -> DataContainer {
        DataContainer {
            value,
            expire
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expire {
            Some(expire) => SystemTime::now() > expire,
            None => false
        }
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }
}

pub enum RDBValidationResult {
    Valid,
    TooShort,
    MissingRedisMagicString
}

pub struct RDBFile {
    data: HashMap<String, DataContainer>,
}

impl RDBFile {
    pub fn from(file_path: String) -> Result<RDBFile> {
        if !file_path.ends_with(".rdb") {
            return Err(anyhow!("File does not end with '.rdb'"));
        }

        let mut file = File::open(file_path)?;
        let mut data: HashMap<String, DataContainer> = HashMap::new();
        let mut buffer = Vec::new();
        let mut cursor = 5;

        file.read_to_end(&mut buffer)?;

        match Self::is_valid(&buffer) {
            RDBValidationResult::TooShort => return Err(anyhow!("Invalid RDB file! File is too short")),
            RDBValidationResult::MissingRedisMagicString => return Err(anyhow!("Missing RDB file! Missing Redis Magic String")),
            RDBValidationResult::Valid => {
                while buffer[cursor] != 0xFF {
                    if buffer[cursor] != 0xFB {
                        cursor += 1;
                        continue;
                    }

                    cursor += 1;
                    let hash_table_size = buffer[cursor] as usize;
                    cursor += 2; // ADDED 1 TO SKIP EXPIRE HASH TABLE SIZE

                    println!("Hash table size: {}", hash_table_size);

                    for _ in 0..hash_table_size {
                        println!("1) Current byte to read: {:?}", buffer[cursor]);

                        let expire: Option<SystemTime> = match buffer[cursor] {
                            0xFD => {
                                let slice: [u8; 4] = buffer[cursor + 1..cursor + 5].try_into()?;
                                cursor += 5;
                                println!("Reading from FD");
                                Some(UNIX_EPOCH + Duration::from_secs(u32::from_le_bytes(slice) as u64))
                            }

                            0xFC => {
                                let slice: [u8; 8] = buffer[cursor + 1..cursor + 9].try_into()?;
                                cursor += 9;
                                println!("Reading from FC");
                                Some(UNIX_EPOCH + Duration::from_millis(u64::from_le_bytes(slice)))
                            }

                            _ => None
                        };

                        cursor += 1; // ADDED 1 TO SKIP VALUE TYPE

                        println!("2) Current byte to read: {:?}", buffer[cursor]);

                        let (key, key_length) = read_length_encoded_string(&buffer[cursor..])?;
                        cursor += key_length;

                        let (value, value_length) = read_length_encoded_string(&buffer[cursor..])?;
                        cursor += value_length;

                        println!("Key: {}, Value: {}, Expiration: {:?}", key, value, expire);

                        let data_container = DataContainer::create(Value::BulkString(value), expire);

                        if data_container.is_expired() {
                            continue;
                        }

                        data.insert(key, data_container);
                    }
                }
            }
        }

         Ok(
            RDBFile {
                data
            }
        )
    }

    fn is_valid(buffer: &Vec<u8>) -> RDBValidationResult {
        if buffer.len() < 5 {
            return RDBValidationResult::TooShort;
        }

        if String::from_utf8(buffer[..5].to_vec()).unwrap().as_str() != "REDIS" {
            return RDBValidationResult::MissingRedisMagicString;
        }

        RDBValidationResult::Valid
    }
}

fn read_length_encoded_int(bytes: &[u8]) -> Result<u64> {
    let first_byte = bytes[0];

    match first_byte {
        0xFD => Ok(u64::from(first_byte)),
        0xFC => {
            let second_byte = bytes[1];
            Ok(((first_byte as u64 & 0x3F) << 8) | second_byte as u64)
        }

        0xFF => {
            let mut buff: [u8; 4] = [0; 4];
            bytes.reader().read_exact(&mut buff)?;
            Ok(u32::from_be_bytes(buff) as u64)
        }

        0xD0 => Ok(u64::from_be_bytes(bytes.try_into()?)),

        _ => Err(anyhow!("Error reading length encoded integer!"))
    }
}

fn read_length_encoded_string(bytes: &[u8]) -> Result<(String, usize)> {
    if bytes.is_empty() {
        return Err(anyhow!("Input is empty! cannot read length."));
    }

    let str_len = bytes[0] as usize;
    if bytes.len() < str_len + 1 {
        return Err(anyhow!("Not enough bytes to read the full string. Expected {}, got {}.", str_len, bytes.len() - 1));
    }

    let string_slice = &bytes[1..=str_len];
    let string = String::from_utf8(string_slice.to_vec())
        .map_err(|_| anyhow!("Invalid UTF-8 sequence in string"))?
        .to_string();

    Ok((string, str_len + 1))
}