use crate::commands::{Command, CommandContext};
use crate::parser::{StreamEntry, Value};
use crate::storage::DataContainer;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct StorageXAddCommand;
impl Command for StorageXAddCommand {
    fn name(&self) -> &str {
        "xadd"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage XADD <key> <id> [<key>] [<value>]...".to_string()));
        }

        let key = args.first().unwrap().clone().unpack_as_string().unwrap();
        let id = args[1].clone().unpack_as_string().unwrap();

        let mut values: HashMap<String, DataContainer> = HashMap::new();

        for i in 2..args.len() - 1 {
            let entry_key = args[i].clone().unpack_as_string().unwrap();
            let entry_value = args[i + 1].clone();

            values.insert(entry_key, DataContainer::create(entry_value, None));
        }

        match context.storage.get(&key) {
            Some(value) => {
                if let Value::Stream(mut entries) = value {
                    let last_entry = entries.last().unwrap();
                    let (millis, sequence) = match generate_stream_id(id, &entries) {
                        Ok(values) => values,
                        _ => return Ok(Value::SimpleError("The ID must have both values as integers! Example: 1-1".to_string(), ))
                    };

                    println!("Adding to stream {}-{}", millis, sequence);

                    if millis == 0 && sequence == 0 {
                        return Ok(Value::SimpleError("ERR The ID specified in XADD must be greater than 0-0".to_string()));
                    }

                    if (millis == last_entry.millis_time && sequence == last_entry.sequence_number) || millis < last_entry.millis_time {
                        return Ok(Value::SimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()));
                    }

                    let mut entry = StreamEntry::new(millis, sequence);
                    entry.storage.add_all(values);
                    entries.push(entry);

                    context.storage.set(&key, Value::Stream(entries), None);

                    Ok(Value::BulkString(format!("{}-{}", millis, sequence)))
                } else {
                    Ok(Value::SimpleError("Not a stream!".to_string()))
                }
            },

            None => {
                let (millis, sequence) = generate_stream_id(id, &vec![])?;

                println!("Creating stream {}-{}", millis, sequence);

                if millis == 0 && sequence == 0 {
                    return Ok(Value::SimpleError("ERR The ID specified in XADD must be greater than 0-0".to_string()));
                }

                let mut entry = StreamEntry::new(millis, sequence);
                entry.storage.add_all(values);

                context.storage.set(&key, Value::Stream(vec![entry]), None);
                Ok(Value::BulkString(format!("{}-{}", millis, sequence)))
            }
        }

    }
}

pub struct StorageXRangeCommand;
impl Command for StorageXRangeCommand {
    fn name(&self) -> &str {
        "xrange"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 3 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage XRANGE <key> <start> <end>".to_string()))
        }

        let key = args.first().unwrap().clone().unpack_as_string().unwrap();
        let min_arg = args[1].clone().unpack_as_string().unwrap();
        let max_arg = args[2].clone().unpack_as_string().unwrap();

        let min: Option<Vec<i128>> = if min_arg.len() == 1 && min_arg.chars().next().unwrap() == '-' {
            None
        } else {
            Some(
                min_arg.split('-')
                    .map(|v| v.parse::<i128>().unwrap())
                    .collect()
            )
        };

        let max: Option<Vec<i128>> = if max_arg.len() == 1 && max_arg.chars().next().unwrap() == '+' {
            None
        } else {
            Some(
                max_arg.split('-')
                    .map(|v| v.parse::<i128>().unwrap())
                    .collect()
            )
        };

        match context.storage.get(key.as_str()) {
            Some(value) => {
                if let Value::Stream(stream_entries) = value {
                    let res = Value::Array(
                        stream_entries.iter()
                            .filter(|entry|
                                min.as_ref().map(|m| entry.millis_time >= m[0] && entry.sequence_number >= m[1] as i64).unwrap_or(true) &&
                                    max.as_ref().map(|m| entry.millis_time <= m[0] && entry.sequence_number <= m[1] as i64).unwrap_or(true)
                            ) // ((min != None && entry.millis_time >= min.unwrap()[0]) && (max != None && entry.millis_time <= max.unwrap()[0])) && (entry.sequence_number >= min.unwrap()[1] as i64 && entry.sequence_number <= max.unwrap()[1] as i64)
                            .map(|entry| entry.as_array_value())
                            .collect()
                    );

                    Ok(res)
                } else {
                    Ok(Value::SimpleError("Not a stream".to_string()))
                }
            }

            _ => Ok(Value::NullBulkString)
        }
    }
}

pub struct StorageXReadCommand;
impl Command for StorageXReadCommand {
    fn name(&self) -> &str {
        "xread"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 3 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage XREAD <type> <key> <id>".to_string()));
        }

        let read_type = args.first().unwrap().clone().unpack_as_string().unwrap().to_lowercase();
        let key = args[1].clone().unpack_as_string().unwrap();
        let id = args[2].clone().unpack_as_string().unwrap();

        let (millis_time, sequence_number) = parse_stream_id(id)?;

        match context.storage.get(&key) {
            Some(value) => {
                match read_type.as_str() {
                    "streams" => {
                        if let Value::Stream(stream_entries) = value {
                            match stream_entries.iter()
                                .filter(|entry| entry.millis_time == millis_time && entry.sequence_number == sequence_number)
                                .map(|entry| entry.as_array_value())
                                .collect::<Vec<Value>>()
                                .first() {
                                    Some(entry) => {
                                        Ok(entry.clone())
                                    }

                                    None => {
                                        Ok(Value::NullBulkString)
                                    }
                                }
                        } else {
                            Ok(Value::SimpleError("Not a stream!".to_string()))
                        }
                    }

                    _ => Ok(Value::SimpleError("Invalid type!".to_string()))
                }
            }

            None => {
                println!("No value found for key {}", key);
                Ok(Value::NullBulkString)
            },
        }
    }
}

fn parse_stream_id(id: String) -> Result<(i128, i64)> {
    let splitted_id: Vec<&str> = id.split("-").collect();

    if splitted_id.len() < 2 {
        return Err(anyhow!("Invalid stream id length: {}", id));
    }

    let millis_time = splitted_id[0].parse::<i128>()?;
    let sequence_number = splitted_id[1].parse::<i64>()?;

    Ok((millis_time, sequence_number))
}

fn generate_stream_id(id: String, entries: &Vec<StreamEntry>) -> Result<(i128, i64)> {
    let splitted_id: Vec<&str> = id.split("-").collect();

    if splitted_id.len() > 1 {
        let milliseconds_time: i128 = splitted_id[0].parse()?;
        let def_sequence_value: i64 = if milliseconds_time <= 0 {
            1
        } else {
            0
        };

        let sequence_number: i64 = if splitted_id[1].starts_with('*') {
            entries
                .last()
                .and_then(|entry| {
                    if entry.millis_time == milliseconds_time {
                        Some(entry.sequence_number + 1)
                    } else {
                        None
                    }
                })
                .unwrap_or(def_sequence_value)
        } else {
            splitted_id[1].parse()?
        };

        return Ok((milliseconds_time, sequence_number))
    }

    Ok((SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i128, 0))
}