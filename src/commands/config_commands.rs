use anyhow::anyhow;
use crate::commands::{Command, CommandContext};
use crate::config::ConfigKey;
use crate::parser::Value;

pub struct ConfigCommand;
impl Command for ConfigCommand {
    fn name(&self) -> &str {
        "config"
    }

    fn exec(&self, args: Vec<Value>, context: &mut CommandContext) -> anyhow::Result<Value> {
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