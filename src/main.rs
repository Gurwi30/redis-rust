#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;

const DEFAULT_PORT: u16 = 6379;

fn main() {
    let listener = TcpListener::bind(("127.0.0.1", DEFAULT_PORT)).unwrap();

    loop {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    println!("Connection established! Responding to ping...");

                    stream.write_all(b"+PONG\r\n").unwrap()
                }

                Err(e) => {
                    println!("An error occurred: {:?}", e);
                }
            }
        }
    }

}
