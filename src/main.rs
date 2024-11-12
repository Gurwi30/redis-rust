#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::TcpListener;

const DEFAULT_PORT: u16 = 6379;

fn main() {
    let listener = TcpListener::bind(("127.0.0.1", DEFAULT_PORT)).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("Connection established! Responding to ping...");

                loop {
                    let reads = stream.read(&mut [0; 256]).unwrap();
                    if reads == 0 {
                        break;
                    }
                }

                stream.write(b"+PONG\r\n").unwrap();
            }

            Err(e) => {
                println!("An error occurred: {:?}", e);
            }
        }
    }

}
