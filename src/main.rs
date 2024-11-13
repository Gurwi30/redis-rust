mod parser;
mod response;

use crate::parser::Value;

use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{DEFAULT_PORT}")).await?;

    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                println!("Connection established! {addr}...");
                handle_client(_socket).await;
            }

            Err(e) => {
                println!("An error occurred: {:?}", e);
            }
        }

    }
}

async fn handle_client(socket: TcpStream) {
    let mut handler = response::RespHandler::new(socket);

    loop {
        let value = handler.read_value().await.unwrap();

        let response = if let Some(value) = value {
            let (command, args) = extract_command(value).unwrap();
            match command.as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                invalid_command => panic!("Unable to handle command {}!", invalid_command)
            }
        } else {
            break;
        };

        println!("{:?}", response);

        handler.write_value(response).await.unwrap()
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            ))
        },
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}
fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}

// async fn handle_client(mut stream: TcpStream) -> JoinHandle<()> {
//     tokio::spawn(async move {
//         loop {
//             let reads = stream.read(buffer).await.unwrap();
//             if reads == 0 {
//                 break;
//             }
//
//             //println!("{:?}", String::from_utf8(buffer[..reads].to_vec()).unwrap()); GET INPUTS
//             stream.write(b"+PONG\r\n").await.unwrap();
//         }
//     })
// }
