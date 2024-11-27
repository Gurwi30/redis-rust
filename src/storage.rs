use crate::parser::Value;
use anyhow::{anyhow, Result};
use bytes::Buf;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::time::Duration;
use tokio::time::Instant;

pub struct Storage {
    storage: HashMap<String, DataContainer>
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            storage: HashMap::new()
        }
    }

    pub fn load_from_rdb(path: String) -> Result<Storage> {
        let rdb_file = RDBFile::from(path)?;

        Ok(
            Storage {
                storage: rdb_file.data
            }
        )
    }

    pub fn import_data(&mut self, rdb_file: RDBFile) {
        self.storage.extend(rdb_file.data);
    }

    pub fn set(&mut self, key: &str, value: Value, expire: Option<u128>) -> Value {
        self.storage.insert(key.to_string(), DataContainer::create(value, expire));
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
    created: Instant,
    expire_in_mills: Option<u128>
}

impl DataContainer {
    pub fn create(value: Value, expire_in_mills: Option<u128>) -> DataContainer {
        DataContainer {
            value,
            created: Instant::now(),
            expire_in_mills
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expire_in_mills {
            Some(expire_in_mills) => {
                Instant::now()
                    .duration_since(self.created)
                    .as_millis()
                    >= expire_in_mills
            }
            None => false,
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
                        let expire: Option<u128> = match buffer[cursor] {
                            0xFD => {
                                let slice: [u8; 4] = buffer[cursor + 1..cursor + 5].try_into()?;
                                cursor += 4;
                                Some(Duration::from_secs(u32::from_le_bytes(slice) as u64).as_millis())
                            }

                            0xFC => {
                                let slice: [u8; 8] = buffer[cursor + 1..cursor + 9].try_into()?;
                                cursor += 8;
                                Some(Duration::from_millis(u64::from_le_bytes(slice)).as_millis())
                            }

                            _ => None
                        };

                        cursor += 2; // ADDED 1 TO SKIP VALUE TYPE

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

// fn read_from_until(data: &[u8], start: usize, until: i32) -> Option<(&[u8], usize)> {
//     for i in (start + 1)..data.len() {
//         let current_byte = data[i];
//
//         if current_byte == until as u8 {
//             return Some((&data[start..(i - 1)], i));
//         }
//     }
//
//     None
// }

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

// Skip the 0xFA byte and read the metadata
// let metadata = &contents[read_bytes + 1..];

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

// println!("Metadata: {:?}", metadata);
// println!("Metadata 1 Length: {:?}", metadata[0] as usize);
//
// //
// // let metadata = read_from_until(&contents, read_bytes, 0xFE).map(|data| (data.0, data.1)).unwrap();
// // println!("RBD Metadata: {}", String::from_utf8_lossy(metadata.0));