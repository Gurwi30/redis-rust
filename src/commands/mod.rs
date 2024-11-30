mod x_commands;
mod base_commands;
mod config_commands;
mod storage_commands;

use crate::commands::base_commands::{EchoCommand, PingCommand};
use crate::commands::config_commands::ConfigCommand;
use crate::commands::storage_commands::{StorageGetCommand, StorageKeysCommand, StorageSetCommand, StorageValueTypeCommand};
use crate::commands::x_commands::{StorageXAddCommand, StorageXRangeCommand, StorageXReadCommand};
use crate::config::Configuration;
use crate::parser::Value;
use crate::storage::Storage;
use anyhow::Result;
use std::collections::HashMap;

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
        self.register(Box::new(StorageGetCommand));
        self.register(Box::new(StorageKeysCommand));
        self.register(Box::new(StorageValueTypeCommand));

        self.register(Box::new(StorageXAddCommand));
        self.register(Box::new(StorageXRangeCommand));
        self.register(Box::new(StorageXReadCommand));

        self.register(Box::new(ConfigCommand))
    }
}