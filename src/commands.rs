use crate::config::{ConfigKey, Configuration};
use crate::parser::Value;
use crate::storage::Storage;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

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
            None => panic!("Unable to handle command {}!", command_name.to_uppercase())
        }
    }

    fn register(&mut self, command: Box<dyn Command>) {
        self.commands.insert(command.name().to_string(), command);
    }

    fn init_def(&mut self) {
        self.register(Box::new(PingCommand));
        self.register(Box::new(EchoCommand));

        self.register(Box::new(StorageSetCommand));
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

struct StorageGetCommand;
impl Command for StorageGetCommand {
    fn name(&self) -> &str {
        "get"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> Result<Value> {
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
        let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
        match context.storage.get(key.as_str()) {
            Some(value) => Ok(Value::SimpleString(value.get_type().to_string())),
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
        let option = args[0].clone().unpack_as_string().unwrap().to_lowercase();

        match option.as_str() {
            "get" => {
                if args.len() < 2 {
                    return Ok(Value::SimpleString("PONG".to_string()));
                }

                let get_option = args[1].clone().unpack_as_string().unwrap();

                match get_option.as_str() {
                    "dir" => Ok(Value::Array(vec![Value::SimpleString("dir".to_string()), Value::SimpleString(context.config.get(ConfigKey::Dir))])),
                    "dbfilename" => Ok(Value::SimpleString(context.config.get(ConfigKey::DbFilename))),
                    _ => Err(anyhow!("Invalid config get option {}", get_option))
                }
            }

            _ => Err(anyhow!("Invalid config option {}", option))
        }
    }
}