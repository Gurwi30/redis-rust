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
        self.register(Box::new(StorageXReadCommand));

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

        for i in 2..args.len() - 1 {
            let entry_key = args[i].clone().unpack_as_string().unwrap();
            let entry_value = args[i + 1].clone();

            values.insert(entry_key, DataContainer::create(entry_value, None));
        }

        match context.storage.get(&key) {
            Some(value) => {
                if let Value::Stream(mut entries) = value {
                    let last_entry = entries.last().unwrap();
                    let (millis, sequence) = match parse_stream_id(id, &entries) {
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
                let (millis, sequence) = parse_stream_id(id,&vec![])?;

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
                            .map(|entry| entry.as_value())
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

struct StorageXReadCommand;
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

        match context.storage.get(&key) {
            Some(value) => {
                match read_type.as_str() {
                    "streams" => {
                        if let Value::Stream(stream_entries) = value {
                            match stream_entries.iter()
                                .filter(|entry| format!("{}-{}", entry.millis_time, entry.sequence_number) == id)
                                .map(|entry| entry.as_value())
                                .next() {

                                    Some(entry) => {
                                        Ok(entry)
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

            None => Ok(Value::NullBulkString),
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

        return Ok((milliseconds_time, sequence_number))
    }

    Ok((SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i128, 0))
}