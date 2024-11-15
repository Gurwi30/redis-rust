use crate::parser::Value;
use crate::storage::Storage;
use anyhow::Result;
use std::collections::HashMap;

trait Command: Send + Sync {
    fn name(&self) -> &str;
    fn exec(&self, args: Vec<Value>, storage: &mut Storage) -> Result<Value>;
}

pub struct CommandExecutor {
    commands: HashMap<String, Box<dyn Command>>,
}

impl CommandExecutor {
    pub fn new() -> CommandExecutor {
        let mut executor = CommandExecutor {
            commands: HashMap::new(),
        };

        executor.init_def();
        executor
    }

    pub fn try_exec(&self, command_name: String, args: Vec<Value>, storage: &mut Storage) -> Result<Value> {
        match self.commands.get(&command_name) {
            Some(command) => command.exec(args, storage),
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
    }
}

struct PingCommand;
impl Command for PingCommand {
    fn name(&self) -> &str {
        "ping"
    }

    fn exec(&self, _: Vec<Value>, _: &mut Storage) -> Result<Value> {
        Ok(Value::SimpleString("PONG".to_string()))
    }
}

struct EchoCommand;
impl Command for EchoCommand {
    fn name(&self) -> &str {
        "echo"
    }

    fn exec(&self, args: Vec<Value>, _: &mut Storage) -> Result<Value> {
        Ok(args.first().unwrap().clone())
    }
}

struct StorageSetCommand;
impl Command for StorageSetCommand {
    fn name(&self) -> &str {
        "set"
    }

    fn exec(&self, args: Vec<Value>, storage: &mut Storage) -> Result<Value> {
        let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
        let value: Value = args[1].clone();
        let mut expiration: Option<u128> = None;

        if args.len() > 2 {
            let option = args[2].clone().unpack_as_string().unwrap().to_lowercase();

            match option.as_str() {
                "px" => expiration = Some(args[3].clone().unpack_as_string().unwrap().parse::<u128>()?), // TODO -> HANDLE ERRORS
                _ => println!("{} is an invalid option!", option)
            }
        }

        Ok(storage.set(key.as_str(), value, expiration))
    }
}

struct StorageGetCommand;
impl Command for StorageGetCommand {
    fn name(&self) -> &str {
        "get"
    }

    fn exec(&self, args: Vec<Value>, storage: &mut Storage) -> Result<Value> {
        let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
        Ok(storage.get(key.as_str()))
    }
}

struct ConfigCommand;
impl Command for ConfigCommand {
    fn name(&self) -> &str {
        "config"
    }

    fn exec(&self, args: Vec<Value>, storage: &mut Storage) -> Result<Value> {
        Ok(Value::SimpleString("OK".to_string()))
    }
}