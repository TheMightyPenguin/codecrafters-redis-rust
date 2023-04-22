// Uncomment this block to pass the first stagE
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

#[derive(PartialEq)]
enum MessageType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
}

enum Command {
    Echo(String),
    Command(String),
    Ping,
}

const SEPARATOR: &str = "\r\n";

fn format_message(kind: MessageType, body: String) -> String {
    let message = match kind {
        MessageType::SimpleString => format!("+{}", body),
        MessageType::Error => format!("-{}", body),
        MessageType::Integer => ":".to_string(),
        MessageType::BulkString => format!("${}{}{}", body.len(), SEPARATOR, body),
        MessageType::Array => "*".to_string(),
    };
    format!("{}{}", message, SEPARATOR)
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(incoming_stream) => {
                thread::spawn(move || {
                    handle_stream(incoming_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(mut stream: TcpStream) {
    loop {
        let mut buffer = [0 as u8; 1024];
        match stream.read(&mut buffer) {
            Ok(read_bytes) => {
                let message = String::from_utf8(buffer.to_vec())
                    .unwrap()
                    // removes null bytes https://stackoverflow.com/a/49406848
                    .trim_end_matches(char::from(0))
                    .to_string();
                if read_bytes == 0 {
                    break;
                }

                println!("message: {:?}", message.clone().chars().collect::<Vec<_>>());
                let instructions = handle_client_message(message);

                if instructions.len() == 0 {
                    stream
                        .write(
                            format_message(
                                MessageType::Error,
                                "Error processing message".to_string(),
                            )
                            .as_bytes(),
                        )
                        .unwrap();
                }

                for instruction in instructions {
                    let message_to_send = match instruction {
                        Command::Echo(message) => format_message(MessageType::BulkString, message),

                        Command::Command(command) => match command.as_str() {
                            _ => {
                                format_message(MessageType::SimpleString, "no docs yet".to_string())
                            }
                        },

                        Command::Ping => {
                            format_message(MessageType::SimpleString, "PONG".to_string())
                        }
                    };
                    println!(
                        "sending-----> {:?}",
                        message_to_send.chars().collect::<Vec<_>>()
                    );
                    stream.write(message_to_send.as_bytes()).unwrap();
                }
                println!();
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }
}

enum State {
    ReadingArray,
    ReadingBulkStringLength,
    ReadingBulkStringContent,
}

fn handle_client_message(message: String) -> Vec<Command> {
    let mut state = State::ReadingArray;
    let mut roller = CharRoller::from_string(message);
    let mut command_name = "".to_string();
    let mut instructions: Vec<Command> = vec![];
    let mut processing_command = false;

    while let Some(raw_word) = roller.next_word() {
        let word = raw_word.trim();
        match state {
            State::ReadingArray => {
                let instruction_type = get_instruction_type(word.chars().nth(0).unwrap());
                if instruction_type != MessageType::Array {
                    panic!("expected array");
                }
                // let array_length = word[1..].parse::<usize>().unwrap();
                // println!("array length: {}", array_length);
                state = State::ReadingBulkStringLength;
            }

            State::ReadingBulkStringLength => {
                let instruction_type = get_instruction_type(word.chars().nth(0).unwrap());
                if instruction_type != MessageType::BulkString {
                    panic!("expected bulk string");
                }
                // let string_length = word[1..].parse::<usize>().unwrap();
                // println!("string length: {}", string_length);
                state = State::ReadingBulkStringContent;
            }

            State::ReadingBulkStringContent => {
                println!("content: {}", word);

                if command_name.is_empty() {
                    command_name = word.to_string();
                }

                match (command_name.to_lowercase().as_str(), word) {
                    ("ping", _) => {
                        instructions.push(Command::Ping);
                        command_name = "".to_string();
                    }

                    ("echo", arg) => {
                        if processing_command {
                            instructions.push(Command::Echo(arg.to_string()));
                            command_name = "".to_string();
                            processing_command = false;
                        } else {
                            processing_command = true;
                        }
                    }

                    ("command", arg) => {
                        if processing_command {
                            instructions.push(Command::Command(arg.to_string()));
                            command_name = "".to_string();
                            processing_command = false;
                        } else {
                            processing_command = true;
                        }
                    }

                    (other, _) => {
                        println!("unknown command: {}", other);
                    }
                }
                state = State::ReadingBulkStringLength;
            }
        }
    }

    instructions
}

fn get_instruction_type(c: char) -> MessageType {
    match c {
        '+' => MessageType::SimpleString,
        '-' => MessageType::Error,
        ':' => MessageType::Integer,
        '$' => MessageType::BulkString,
        '*' => MessageType::Array,
        _ => panic!("unknown type: {}", c),
    }
}

pub struct CharRoller {
    chars: Vec<char>,
    index: usize,
}

impl CharRoller {
    pub fn from_string(phrase: String) -> CharRoller {
        let chars: Vec<_> = phrase.chars().collect();
        CharRoller { chars, index: 0 }
    }

    pub fn next_word(&mut self) -> Option<String> {
        let mut word = String::new();
        if self.index == self.chars.len() {
            return None;
        }
        while self.index < self.chars.len() {
            let c = self.chars[self.index];
            if c == '\r' {
                self.index += 1;
                continue;
            }
            if c == '\n' {
                self.index += 1;
                break;
            }
            word.push(c);
            self.index += 1;
        }
        return if word.len() == 0 { None } else { Some(word) };
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_char_roller() {
        let mut roller = CharRoller::from_string("hello\r\nworld\r\n".to_string());
        let word = roller.next_word();
        assert_eq!(word, Some("hello".to_string()));
        let word = roller.next_word();
        assert_eq!(word, Some("world".to_string()));
        let word = roller.next_word();
        assert_eq!(word, None);
    }
}
