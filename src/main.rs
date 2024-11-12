#![allow(unused_imports)]

use std::fmt::format;
use std::io::{Read, Write};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{DEFAULT_PORT}")).await?;

    // for stream in listener.accept().await {
    //     match stream {
    //         Ok(stream) => {
    //             println!("Connection established! Responding to ping...");
    //             handle_client(stream);
    //         }
    //
    //         Err(e) => {
    //             println!("An error occurred: {:?}", e);
    //         }
    //     }
    // }


    match listener.accept().await? {
        Ok((_socket, addr)) => {
            println!("Connection established! {addr}...");
            handle_client(_socket).await;
        }

        Err(e) => {
            println!("An error occurred: {:?}", e);
            Err(e);
        }
    }

    Ok(())
}

async fn handle_client(mut stream: TcpStream) -> JoinHandle<()> {
    tokio::spawn(async move {
            loop {
                let reads = stream.read(&mut [0; 256]).await.unwrap();
                if reads == 0 {
                    break;
                }

                stream.write(b"+PONG\r\n").await.unwrap();
            }
    })
}

// fn handle_client(mut stream: TcpStream) {
//     loop {
//         let reads = stream.read(&mut [0; 256]).unwrap();
//         if reads == 0 {
//             break;
//         }
//
//         stream.write(b"+PONG\r\n").unwrap();
//     }
// }
