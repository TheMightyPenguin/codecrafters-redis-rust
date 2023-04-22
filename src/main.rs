// Uncomment this block to pass the first stagE
use std::{
    io::{Read, Write},
    net::TcpListener,
};

enum MessageType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
}

fn format_message(kind: MessageType, body: String) -> String {
    let kind_string = match kind {
        MessageType::SimpleString => "+".to_string(),
        MessageType::Error => "-".to_string(),
        MessageType::Integer => ":".to_string(),
        MessageType::BulkString => "$".to_string(),
        MessageType::Array => "*".to_string(),
    };
    format!("{}{}\r\n", kind_string, body)
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut incoming_stream) => {
                println!("accepted new connection");
                incoming_stream
                    .write(format_message(MessageType::SimpleString, "PONG".to_string()).as_bytes())
                    .unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
