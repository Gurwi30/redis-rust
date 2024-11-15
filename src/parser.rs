use anyhow::anyhow;
use anyhow::Result;
use bytes::BytesMut;

#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Boolean(bool),
    Integer(i64),
    Array(Vec<Value>),
    NullBulkString,
    Null
}

impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::Boolean(b) => format!("#{}\r\n", b.to_string().chars().next().unwrap()),
            Value::Integer(i) => format!(":{}\r\n", i),
            Value::Array(arr) => format!("*{}\r\n{}\r\n", arr.len(), arr.iter().map(|v| v.clone().serialize()).collect::<Vec<_>>().join("\r\n")),
            Value::NullBulkString => "$-1\r\n".to_string(),
            Value::Null => "_\r\n".to_string()
            //_ => panic!("Tried to serialize unserializable value!")
        }
    }

    pub fn unpack_as_string(self) -> Option<String> {
        match self {
            Value::SimpleString(s) => Some(s),
            Value::BulkString(s) => Some(s),
            Value::Boolean(b) => Some(b.to_string()),
            Value::Integer(i) => Some(i.to_string()),
            Value::Null => Some(String::new()),
            _ => None
        }
    }

}

pub(crate) fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '$' => parse_bulk_string(buffer),
        '*' => parse_array(buffer),
        _ => Err(anyhow!("{:?} is an invalid value type!", buffer)),
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, parsed)) = read_until_end(&buffer[1..]) {
        let buffer_str: String = buffer_to_string(line);
        return Ok((Value::SimpleString(buffer_str), parsed + 1));
    }

    Err(anyhow!("{:?} is an invalid SimpleString!", buffer))
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {
    let (str_length, bytes_consumed) = if let Some((line, parsed)) = read_until_end(&buffer[1..]) {
        let str_length = parse_int(line)?;

        (str_length, parsed + 1)
    } else {
        return Err(anyhow!("{:?} is an invalid Bulk String Format!", buffer))
    };

    let end_of_str = bytes_consumed + str_length;
    let total_parsed = end_of_str + 2;

    Ok((Value::SimpleString(buffer_to_string(&buffer[bytes_consumed..end_of_str])), total_parsed))
}

fn parse_integer(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, parsed)) = read_until_end(&buffer[1..]) {
        let buffer_str: String = buffer_to_string(line);
        return Ok((Value::Integer(buffer_str.parse::<i64>()?), parsed + 1));
    }

    Err(anyhow!("{:?} is an invalid Integer!", buffer))
}

fn parse_boolean(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, parsed)) = read_until_end(&buffer[1..]) {
        let buffer_str: String = buffer_to_string(line);
        return Ok((Value::Boolean(buffer_str.parse::<bool>()?), parsed + 1));
    }

    Err(anyhow!("{:?} is an invalid Integer!", buffer))
}

fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {
    let (array_length, mut bytes_consumed) = if let Some((line, parsed)) = read_until_end(&buffer[1..]) {
        let array_size = parse_int(line)?;

        (array_size, parsed + 1)
    } else {
        return Err(anyhow!("{:?} is an invalid Array Format!", buffer))
    };

    let mut array_items: Vec<Value> = Vec::new();

    for _ in 0..array_length {
        let (item, parsed) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;

        array_items.push(item);
        bytes_consumed += parsed;
    }

    Ok((Value::Array(array_items), bytes_consumed))
}

fn read_until_end(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        let previous_char = buffer[i - 1] as char;
        let cur_char: char = buffer[i] as char;

        if cur_char == '\n' && previous_char == '\r' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    None
}

fn buffer_to_string(buffer: &[u8]) -> String {
    String::from_utf8(buffer.to_vec()).expect("Invalid UTF-8")
}

fn parse_int(buffer: &[u8]) -> Result<usize> {
    Ok(String::from_utf8(buffer.to_vec()).expect("Invalid UTF-8").parse::<usize>()?)
}