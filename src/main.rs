mod parser;
mod response;
mod commands;
mod storage;

use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;
use crate::commands::CommandExecutor;
use crate::storage::Storage;
use crate::parser::Value;

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{DEFAULT_PORT}")).await?;
    let shared_storage = Arc::new(Mutex::new(Storage::new()));
    let shared_executor = Arc::new(CommandExecutor::new());

    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                println!("Connection established! {addr}...");
                let storage = Arc::clone(&shared_storage);
                let executor = Arc::clone(&shared_executor);

                tokio::spawn(async move {
                    handle_client(_socket, storage, executor).await;
                });
            }

            Err(e) => {
                println!("An error occurred: {:?}", e);
            }
        }

    }
}

async fn handle_client(socket: TcpStream, storage: Arc<Mutex<Storage>>, command_executor: Arc<CommandExecutor>) {
    let mut handler = response::RespHandler::new(socket);

    loop {
        let value = handler.read_value().await.unwrap();

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            let mut storage = storage.lock().unwrap();
            command_executor.try_exec(command.to_lowercase(), args, &mut *storage).unwrap()
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