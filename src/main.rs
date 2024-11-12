#![allow(unused_imports)]
use std::net::TcpListener;

const DEFAULT_PORT: u16 = 6379;

fn main() {

    let listener = TcpListener::bind(("0.0.0.0", DEFAULT_PORT)).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("Connection established!");
            }

            Err(e) => {
                println!("An error occurred: {:?}", e);
            }
        }
    }

}
