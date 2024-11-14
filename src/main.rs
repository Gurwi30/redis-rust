mod parser;
mod response;
mod commands;
mod storage;

use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;
use crate::storage::DataContainer;
use crate::parser::Value;

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{DEFAULT_PORT}")).await?;

    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                println!("Connection established! {addr}...");

                tokio::spawn(async move {
                    handle_client(_socket).await;
                });
            }

            Err(e) => {
                println!("An error occurred: {:?}", e);
            }
        }

    }
}

async fn handle_client(socket: TcpStream) {
    let mut handler = response::RespHandler::new(socket);
    let mut data_container: DataContainer = DataContainer::new();

    loop {
        let value = handler.read_value().await.unwrap();

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();

            match command.as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                "set" => {
                    let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
                    let value: Value = args[1].clone();

                    data_container.set(key.as_str(), value)
                },

                "get" => {
                    let key: String = args.first().unwrap().clone().unpack_as_string().unwrap();
                    data_container.get(key.as_str())
                }

                invalid_command => panic!("Unable to handle command {}!", invalid_command)
            }
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