use std::time::{Duration, SystemTime};
use crate::commands::{Command, CommandContext};
use crate::parser::Value;

pub struct StorageSetCommand;
impl Command for StorageSetCommand {
    fn name(&self) -> &str {
        "set"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> anyhow::Result<Value> {
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

pub struct StorageGetCommand;
impl Command for StorageGetCommand {
    fn name(&self) -> &str {
        "get"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> anyhow::Result<Value> {
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

pub struct StorageKeysCommand;
impl Command for StorageKeysCommand {
    fn name(&self) -> &str {
        "keys"
    }

    fn exec(&self, _args: Vec<Value>, context: &mut CommandContext) -> anyhow::Result<Value> {
        Ok(Value::Array(context.storage.keys().iter().map(|k| Value::BulkString(k.clone())).collect::<Vec<Value>>()))
    }
}

pub struct StorageValueTypeCommand;
impl Command for StorageValueTypeCommand {
    fn name(&self) -> &str {
        "type"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> anyhow::Result<Value> {
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