mod parser;
mod response;
mod commands;
mod storage;
mod config;

use crate::commands::{CommandContext, CommandExecutor};
use crate::config::{ConfigKey, Configuration};
use crate::parser::Value;
use crate::storage::Storage;
use anyhow::Result;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{DEFAULT_PORT}")).await?;
    let args = std::env::args().collect::<Vec<_>>();

    let storage = Storage::new();
    let mut config = Configuration::new();

    if args.len() > 1 {
        let mut cur_index = 1;

        while cur_index < args.len() {
            if args[cur_index].starts_with("--") {
                let arg = &args[cur_index];

                match args[1].as_str() {
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
            }
        }
    }

    if args.len() > 2 {
        match args[1].as_str() {
            "--dir" => {
                let value = args[2].as_str();
                config.set(ConfigKey::Dir, value);
            },

            "--dbfilename" => {
                let value = args[2].as_str();
                config.set(ConfigKey::DbFilename, value);
            }

            _ => println!("Invalid Argument! {}", args[1]),
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
        Value::Array(arr) => {
            Ok((arr.first().unwrap().clone().unpack_as_string().unwrap().to_lowercase(), arr.into_iter().skip(1).collect()))
        },

        _ => Err(anyhow::anyhow!("Invalid command format!"))
    }
}


// match command.as_str() {
//     "ping" => Value::SimpleString("PONG".to_string()),
//     "echo" => args.first().unwrap().clone(),
//     "set" => {
//         let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
//         let value: Value = args[1].clone();
//         let mut expiration: Option<u128> = None;
//
//         if args.len() > 2 {
//             let option = args[2].clone().unpack_as_string().unwrap().to_lowercase();
//
//             match option.as_str() {
//                 "px" => expiration = Some(args[3].clone().unpack_as_string().unwrap().parse::<u128>().unwrap()), // TODO -> HANDLE ERRORS
//                 _ => println!("{} is an invalid option!", option)
//             }
//         }
//
//         data_container.set(key.as_str(), value, expiration)
//     },
//
//     "get" => {
//         let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
//         data_container.get(key.as_str())
//     }
//
//     invalid_command => panic!("Unable to handle command {}!", invalid_command)
// }