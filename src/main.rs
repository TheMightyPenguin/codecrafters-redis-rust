// Uncomment this block to pass the first stagE
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
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
                handle_stream(incoming_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(mut stream: TcpStream) {
    loop {
        let mut buffer = [0 as u8; 512];
        match stream.read(&mut buffer) {
            Ok(read_bytes) => {
                println!("read {} bytes", read_bytes);
                println!("as string: {}", String::from_utf8_lossy(&buffer));
                if read_bytes == 0 {
                    break;
                }
                stream
                    .write(format_message(MessageType::SimpleString, "PONG".to_string()).as_bytes())
                    .unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }
}
