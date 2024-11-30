use crate::commands::{Command, CommandContext};
use crate::parser::Value;

pub struct PingCommand;
impl Command for PingCommand {
    fn name(&self) -> &str {
        "ping"
    }

    fn exec(&self, _args: Vec<Value>, _context: &mut CommandContext) -> anyhow::Result<Value> {
        Ok(Value::SimpleString("PONG".to_string()))
    }
}

pub struct EchoCommand;
impl Command for EchoCommand {
    fn name(&self) -> &str {
        "echo"
    }

    fn exec(&self, args: Vec<Value>, _context: &mut CommandContext) -> anyhow::Result<Value> {
        Ok(args.first().unwrap().clone())
    }
}