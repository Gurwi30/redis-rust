mod parser;
mod response;
mod storage;
mod config;
mod commands;

use crate::commands::{CommandContext, CommandExecutor};
use crate::config::{ConfigKey, Configuration};
use crate::parser::Value;
use crate::storage::{RDBFile, Storage};
use anyhow::Result;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{DEFAULT_PORT}")).await?;
    let args = std::env::args().collect::<Vec<_>>();

    let mut storage = Storage::new();
    let mut config = Configuration::new();

    if args.len() > 1 {
        let mut cur_index = 1;

        while cur_index < args.len() {
            if args[cur_index].starts_with("--") {
                let arg = &args[cur_index];

                match arg.as_str() {
                    "--dir" => {
                        let value = args[cur_index + 1].as_str();
                        config.set(ConfigKey::Dir, value);
                    },

                    "--dbfilename" => {
                        let value = args[cur_index + 1].as_str();
                        config.set(ConfigKey::DbFilename, value);
                    }

                    _ => println!("Invalid Argument! {}", args[1]),
                }

                cur_index += 2;
            } // TODO -> Check if the argument value is present, if not throw an error, just handle this fucking errors and don't be lazy.
        }

        match RDBFile::from(format!("{}/{}", config.get(ConfigKey::Dir), config.get(ConfigKey::DbFilename))) {
            Ok(rdb_file) => {
                storage.import_data(rdb_file);
                println!("Imported data from RDB file");
            }

            Err(e) => println!("Unable to import data from RDB file! {}", e),
        }

    }

    let shared_executor = Arc::new(CommandExecutor::new());
    let context = Arc::new(Mutex::new(CommandContext::new(storage, config)));

    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                println!("Connection established! {addr}...");
                let executor = Arc::clone(&shared_executor);
                let context = Arc::clone(&context);

                tokio::spawn(async move {
                    handle_client(_socket, executor, context).await;
                });
            }

            Err(e) => {
                println!("An error occurred: {:?}", e);
            }
        }
    }
}

async fn handle_client(socket: TcpStream, command_executor: Arc<CommandExecutor>, context: Arc<Mutex<CommandContext>>) {
    let mut handler = response::RespHandler::new(socket);

    loop {
        let value = handler.read_value().await.unwrap();

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            let mut context = context.lock().unwrap();

            command_executor.try_exec(command.to_lowercase(), args, &mut *context).unwrap()
        } else {
            break;
        };

        handler.write_value(response).await.unwrap()
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(arr) => Ok((arr.first().unwrap().clone().unpack_as_string().unwrap().to_lowercase(), arr.into_iter().skip(1).collect())),
        _ => Err(anyhow::anyhow!("Invalid command format!"))
    }
}