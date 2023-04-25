use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
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
    Get(String),
    Set(String, String, Option<u64>),
    Ping,
}

struct StorageEntry {
    expire_timestamp: Option<Instant>,
    value: String,
}

impl StorageEntry {
    fn new(value: String, expire_timestamp: Option<Instant>) -> StorageEntry {
        StorageEntry {
            expire_timestamp,
            value,
        }
    }
}

const SEPARATOR: &str = "\r\n";
const NULL_BULK_STRING: &str = "$-1\r\n";

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
    let storage = Arc::new(Mutex::new(HashMap::<String, StorageEntry>::new()));

    for stream in listener.incoming() {
        let mut storage_for_thread = storage.clone();
        match stream {
            Ok(incoming_stream) => {
                thread::spawn(move || {
                    handle_stream(incoming_stream, &mut storage_for_thread);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(
    mut stream: TcpStream,
    storage_ref: &mut Arc<Mutex<HashMap<String, StorageEntry>>>,
) {
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

                // println!("message: {:?}", message.clone().chars().collect::<Vec<_>>());
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
                            _ => format_message(
                                MessageType::SimpleString,
                                "not supported yet".to_string(),
                            ),
                        },

                        Command::Get(key) => {
                            let mut storage = storage_ref.lock().unwrap();
                            match storage.get(&key) {
                                Some(entry) => {
                                    let now = Instant::now();
                                    let expiry = entry
                                        .expire_timestamp
                                        .unwrap_or(now + Duration::from_secs(1));
                                    if entry.expire_timestamp.is_some() && now > expiry {
                                        storage.remove(&key);
                                        NULL_BULK_STRING.to_string()
                                    } else {
                                        format_message(
                                            MessageType::BulkString,
                                            entry.value.to_string(),
                                        )
                                    }
                                }
                                None => NULL_BULK_STRING.to_string(),
                            }
                        }

                        Command::Set(key, value, expiry) => {
                            let mut storage = storage_ref.lock().unwrap();
                            let entry = StorageEntry::new(
                                value,
                                match expiry {
                                    Some(ms) => Some(Instant::now() + Duration::from_millis(ms)),
                                    None => None,
                                },
                            );
                            storage.insert(key, entry);
                            format_message(MessageType::SimpleString, "OK".to_string())
                        }

                        Command::Ping => {
                            format_message(MessageType::SimpleString, "PONG".to_string())
                        }
                    };
                    // println!(
                    //     "sending-----> {:?}",
                    //     message_to_send.chars().collect::<Vec<_>>()
                    // );
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
    let mut args: Vec<String> = vec![];
    let mut instructions: Vec<Command> = vec![];
    let mut items_left_count = 0;

    while let Some(raw_word) = roller.next_word() {
        let word = raw_word.trim();
        match state {
            State::ReadingArray => {
                let instruction_type = get_instruction_type(word.chars().nth(0).unwrap());
                if instruction_type != MessageType::Array {
                    panic!("expected array");
                }
                let array_length = word[1..].parse::<usize>().unwrap();
                items_left_count = array_length;
                state = State::ReadingBulkStringLength;
            }

            State::ReadingBulkStringLength => {
                let instruction_type = get_instruction_type(word.chars().nth(0).unwrap());
                if instruction_type != MessageType::BulkString {
                    panic!("expected bulk string");
                }
                state = State::ReadingBulkStringContent;
            }

            State::ReadingBulkStringContent => {
                if command_name.is_empty() {
                    command_name = word.to_string();
                } else {
                    args.push(word.to_string());
                }

                if items_left_count != 1 {
                    state = State::ReadingBulkStringLength;
                    items_left_count = items_left_count - 1;
                    continue;
                }

                match command_name.to_lowercase().as_str() {
                    "ping" => {
                        instructions.push(Command::Ping);
                        command_name = "".to_string();
                    }

                    "echo" => {
                        instructions.push(Command::Echo(args.join(" ").to_string()));
                        command_name = "".to_string();
                    }

                    "get" => {
                        instructions.push(Command::Get(args[0].to_string()));
                        command_name = "".to_string();
                    }

                    "set" => {
                        let mut expiry: Option<u64> = None;
                        if args.len() == 4 {
                            expiry = match args[3].parse::<u64>() {
                                Ok(expiry) => {
                                    if expiry > 0 {
                                        Some(expiry)
                                    } else {
                                        None
                                    }
                                }
                                Err(_e) => None,
                            }
                        }
                        instructions.push(Command::Set(
                            args[0].to_string(),
                            args[1].to_string(),
                            expiry,
                        ));
                        command_name = "".to_string();
                    }

                    "command" => {
                        instructions.push(Command::Command(args.join(" ").to_string()));
                        command_name = "".to_string();
                    }

                    other => {
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
