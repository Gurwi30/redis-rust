use crate::config::{ConfigKey, Configuration};
use crate::parser::{StreamEntry, Value};
use crate::storage::{DataContainer, Storage};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

trait Command: Send + Sync {
    fn name(&self) -> &str;
    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value>;
}

pub struct CommandExecutor {
    commands: HashMap<String, Box<dyn Command>>,
}

pub struct CommandContext {
    storage: Storage,
    config: Configuration
}

impl CommandContext {
    pub fn new(storage: Storage, config: Configuration) -> CommandContext {
        CommandContext {
            storage,
            config
        }
    }
}

impl CommandExecutor {
    pub fn new() -> CommandExecutor {
        let mut executor = CommandExecutor {
            commands: HashMap::new(),
        };

        executor.init_def();
        executor
    }

    pub fn try_exec(&self, command_name: String, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        match self.commands.get(&command_name) {
            Some(command) => command.exec(args, context),
            None => panic!("Unable to handle command {}!", command_name.to_uppercase()) // TODO -> SEND ERROR
        }
    }

    fn register(&mut self, command: Box<dyn Command>) {
        self.commands.insert(command.name().to_string(), command);
    }

    fn init_def(&mut self) {
        self.register(Box::new(PingCommand));
        self.register(Box::new(EchoCommand));

        self.register(Box::new(StorageSetCommand));

        self.register(Box::new(StorageXAddCommand));
        self.register(Box::new(StorageXRangeCommand));

        self.register(Box::new(StorageGetCommand));
        self.register(Box::new(StorageKeysCommand));
        self.register(Box::new(StorageValueTypeCommand));

        self.register(Box::new(ConfigCommand))
    }
}

struct PingCommand;
impl Command for PingCommand {
    fn name(&self) -> &str {
        "ping"
    }

    fn exec(&self, _args: Vec<Value>, _context: &mut CommandContext) -> Result<Value> {
        Ok(Value::SimpleString("PONG".to_string()))
    }
}

struct EchoCommand;
impl Command for EchoCommand {
    fn name(&self) -> &str {
        "echo"
    }

    fn exec(&self, args: Vec<Value>, _context: &mut CommandContext) -> Result<Value> {
        Ok(args.first().unwrap().clone())
    }
}

struct StorageSetCommand;
impl Command for StorageSetCommand {
    fn name(&self) -> &str {
        "set"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage SET <key> <value> [<expiration-in-millis>]".to_string()));
        }

        let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
        let value: Value = args[1].clone();
        let mut expiration: Option<SystemTime> = None;

        if args.len() > 2 {
            let option = args[2].clone().unpack_as_string().unwrap().to_lowercase();

            match option.as_str() {
                "px" => expiration = Some(SystemTime::now() + Duration::from_millis(args[3].clone().unpack_as_string().unwrap().parse::<u64>()?)),
                _ => println!("{} is an invalid option!", option)
            }
        }

        Ok(context.storage.set(key.as_str(), value, expiration))
    }
}

struct StorageXAddCommand;
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

        for i in 2..args.len() - 3 {
            let entry_key = args[i].clone().unpack_as_string().unwrap();
            let entry_value = args[i + 1].clone();

            values.insert(entry_key, DataContainer::create(entry_value, None));
        }

        match context.storage.get(&key) {
            Some(value) => {
                if let Value::Stream(entries) = value {
                    let last_entry = entries.last().unwrap();
                    let (millis, sequence) = match parse_stream_id(id, &entries) {
                        Ok(values) => values,
                        _ => return Ok(Value::SimpleError("The ID must have both values as integers! Example: 1-1".to_string(), ))
                    };

                    if millis == 0 && sequence == 0 {
                        return Ok(Value::SimpleError("ERR The ID specified in XADD must be greater than 0-0".to_string()));
                    }

                    if (millis == last_entry.millis_time && sequence == last_entry.sequence_number) || millis < last_entry.millis_time {
                        return Ok(Value::SimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()));
                    }

                    let mut entry = StreamEntry::new(millis, sequence);
                    entry.storage.add_all(values);

                    Ok(Value::BulkString(format!("{}-{}", millis, sequence)))
                } else {
                    Ok(Value::SimpleError("Not a stream!".to_string()))
                }
            },

            _ => {
                let (millis, sequence) = parse_stream_id(id,&vec![])?;

                if millis == 0 && sequence == 0 {
                    return Ok(Value::SimpleError("ERR The ID specified in XADD must be greater than 0-0".to_string()));
                }

                let mut entry = StreamEntry::new(millis, sequence);
                entry.storage.add_all(values);

                context.storage.set(&key, Value::Stream(vec![entry]), None);
                Ok(Value::BulkString(format!("{}-{}", millis, sequence)))
            }
        }

        // let split_id = &id.split("-").collect::<Vec<&str>>();
        // let (cur_id_mills_time, cur_id_sequence_number) = match (split_id[0].parse::<i128>(), split_id[1].parse::<i64>()) {
        //     (Ok(mills_time), Ok(sequence_number)) => (mills_time, sequence_number),
        //     _ => return Ok(Value::SimpleError("The ID must have both values as integers! Example: 1-1".to_string(), ))
        // };
    }
}

struct StorageXRangeCommand;
impl Command for StorageXRangeCommand {
    fn name(&self) -> &str {
        "xrange"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 3 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage XRANGE <key> <start> <end>".to_string()))
        }

        let key = args.first().unwrap().clone().unpack_as_string().unwrap();
        // let min = args[1].clone().unpack_as_string().unwrap().parse::<i128>()?; // TODO -> DO BETTER ERROR HANDLING
        // let max = args[2].clone().unpack_as_string().unwrap().parse::<i128>()?; // TODO -> DO BETTER ERROR HANDLING

        let min: Vec<i128> = args[1].clone().unpack_as_string().unwrap().split('-')
            .map(|v| v.parse::<i128>().unwrap())
            .collect();

        let max: Vec<i128> = args[2].clone().unpack_as_string().unwrap().split('-')
            .map(|v| v.parse::<i128>().unwrap())
            .collect();

        match context.storage.get(key.as_str()) {
            Some(value) => {
                if let Value::Stream(stream_entries) = value {
                    let res = Value::Array(
                        stream_entries.iter()
                            .filter(|entry| (entry.millis_time > min[0] && entry.millis_time < max[0]) || (entry.sequence_number > min[1] as i64 && entry.sequence_number < max[1] as i64))
                            .map(|entry| {
                                Value::Array(vec![
                                    Value::BulkString(format!("{}-{}", entry.millis_time, entry.sequence_number)),
                                    Value::Array(
                                                  entry.clone()
                                                      .storage
                                                      .get_all()
                                                      .iter()
                                                      .flat_map(|(key, data)| {
                                                          vec![
                                                              Value::BulkString(key.to_string()),
                                                              data.get_value(),
                                                          ]
                                                      })
                                                      .collect()
                                    ),
                                ])
                            })
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

struct StorageGetCommand;
impl Command for StorageGetCommand {
    fn name(&self) -> &str {
        "get"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 1 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage GET <key>".to_string()));
        }

        let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();

        match context.storage.get(key.as_str()) {
            Some(value) => Ok(value.clone()),
            _ => Ok(Value::NullBulkString)
        }
    }
}

struct StorageKeysCommand;
impl Command for StorageKeysCommand {
    fn name(&self) -> &str {
        "keys"
    }

    fn exec(&self, _args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        Ok(Value::Array(context.storage.keys().iter().map(|k| Value::BulkString(k.clone())).collect::<Vec<Value>>()))
    }
}

struct StorageValueTypeCommand;
impl Command for StorageValueTypeCommand {
    fn name(&self) -> &str {
        "type"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 1 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage TYPE <key>".to_string()));
        }

        let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();

        match context.storage.get(key.as_str()) {
            Some(value) => Ok(Value::SimpleString(value.get_type().to_string().to_lowercase())),
            _ => Ok(Value::SimpleString("none".to_string())),
        }
    }
}

struct ConfigCommand;
impl Command for ConfigCommand {
    fn name(&self) -> &str {
        "config"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
        if args.len() < 1 {
            return Ok(Value::SimpleError("Missing arguments! Correct usage CONFIG <sub-command>".to_string()));
        }

        let sub_command = args[0].clone().unpack_as_string().unwrap().to_lowercase();

        match sub_command.as_str() {
            "get" => {
                if args.len() < 2 {
                    return Ok(Value::SimpleError("Missing arguments! Correct usage CONFIG GET <option>".to_string()));
                }

                let get_option = args[1].clone().unpack_as_string().unwrap();

                match get_option.as_str() {
                    "dir" => Ok(Value::Array(vec![Value::SimpleString("dir".to_string()), Value::SimpleString(context.config.get(ConfigKey::Dir))])),
                    "dbfilename" => Ok(Value::SimpleString(context.config.get(ConfigKey::DbFilename))),
                    _ => Err(anyhow!("Invalid config get option {}", get_option))
                }
            }

            _ => Err(anyhow!("Invalid config sub command {}", sub_command))
        }
    }
}

fn parse_stream_id(id: String, entries: &Vec<StreamEntry>) -> Result<(i128, i64)> {
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

        // let sequence_number: i64 = if splitted_id[1].starts_with('*') {
        //     storage
        //         .get_specific(Type::Stream)
        //         .iter()
        //         .filter_map(|data| {
        //             if let Value::Stream(mills, sequence, _) = data.get_value() {
        //                 if mills == milliseconds_time {
        //                     return Some(sequence + 1);
        //                 }
        //             }
        //
        //             None
        //         })
        //         .last()
        //         .unwrap_or(def_sequence_value)
        // } else {
        //     splitted_id[1].parse()?
        // };

        return Ok((milliseconds_time, sequence_number))
    }

    Ok((SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i128, 0))
}

// let last_stream_entry
//
// let (cur_id_mills_time, cur_id_sequence_number) = match parse_stream_id(id, &mut context.storage) {
//     Ok(values) => values,
//     _ => return Ok(Value::SimpleError("The ID must have both values as integers! Example: 1-1".to_string(), ))
// };
//
// match last_stream_entry {
//     Some(data_container) => {
//         if let Value::Stream(mills_time, sequence_number, _entries) = data_container.get_value() {
//             if cur_id_mills_time == 0 && cur_id_sequence_number == 0 {
//                 return Ok(Value::SimpleError("ERR The ID specified in XADD must be greater than 0-0".to_string()));
//             }
//
//             if (cur_id_mills_time == mills_time && cur_id_sequence_number == sequence_number) || cur_id_mills_time < mills_time {
//                 return Ok(Value::SimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()));
//             }
//         }
//     }
//     _ => {}
// }
//
// let mut entries: HashMap<String, DataContainer> = HashMap::new();
//
// for i in 2..args.len() - 3 {
//     let entry_key = args[i].clone().unpack_as_string().unwrap();
//     let entry_value = args[i + 1].clone();
//
//     entries.insert(entry_key, DataContainer::create(entry_value, None));
// }
//
// let mut new_entry = StreamEntry::new(cur_id_mills_time, cur_id_sequence_number);
// new_entry.storage.add_all(entries);
//
// match context.storage.get(key.as_str()) {
//     Some(value) => {
//         if let Value::Stream(mut stream_entries) = value {
//             stream_entries.push(new_entry);
//         }
//     },
//
//     _ => {
//         context.storage.set(key.as_str(), Value::Stream(vec![new_entry]), None);
//     }
// }
//
// Ok(Value::BulkString(format!("{}-{}", cur_id_mills_time, cur_id_sequence_number)))