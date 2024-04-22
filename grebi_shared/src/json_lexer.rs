
#[derive(PartialEq)]
#[derive(Copy, Clone)]

#[derive(Debug)]
pub enum JsonTokenType {
    StartObject,
    EndObject,
    StartArray,
    EndArray,
    StartString,
    EndString,
    Colon,
    Comma,
    StartNumber,
    EndNumber,
    True,
    False,
    Null,
}


#[derive(Clone)]
pub struct JsonToken {
    pub kind: JsonTokenType,
    pub index: usize,
}

pub fn lex(buf: &[u8]) -> Vec<JsonToken> {

    let mut tokens: Vec<JsonToken> = Vec::new();

    let mut index: usize = 0;

    while index < buf.len() {
        let c = buf[index];

        match c {
            b' ' => { index += 1 }
            b'\t' => { index += 1 }
            b'\r' => { index += 1 }
            b'\n' => { index += 1 }
            b'{' => { tokens.push(JsonToken { kind: JsonTokenType::StartObject, index }); index += 1 }
            b'}' => { tokens.push(JsonToken { kind: JsonTokenType::EndObject, index }); index += 1 } 
            b'[' => { tokens.push(JsonToken { kind: JsonTokenType::StartArray, index }); index += 1 } 
            b']' => { tokens.push(JsonToken { kind: JsonTokenType::EndArray, index }); index += 1 } 
            b':' => { tokens.push(JsonToken { kind: JsonTokenType::Colon, index }); index += 1 } 
            b',' => { tokens.push(JsonToken { kind: JsonTokenType::Comma, index }); index += 1 }
            b'"' => {
                tokens.push(JsonToken { kind: JsonTokenType::StartString, index });
                index += 1;
                while buf[index] != b'"' {
                    if buf[index] == b'\\' {
                        index += 1;
                        match buf[index] {
                            b'"' => index += 1,
                            b'\\' => index += 1,
                            b'/' => index += 1,
                            b'b' => index += 1,
                            b'f' => index += 1,
                            b'n' => index += 1,
                            b'r' => index += 1,
                            b't' => index += 1,
                            b'u' => index += 5,
                            _ => panic!("unknown escape sequence in string: {}. json was: {}", buf[index], String::from_utf8(buf.to_vec()).unwrap() )
                        }
                    } else {
                        index += 1;
                    }
                }
                tokens.push(JsonToken { kind: JsonTokenType::EndString, index });
                index += 1 ;
            }
            b'0'|b'1'|b'2'|b'3'|b'4'|b'5'|b'6'|b'7'|b'8'|b'9'|b'-' => {
                tokens.push(JsonToken { kind: JsonTokenType::StartNumber, index });
                while index < buf.len() && buf[index] != b',' && buf[index] != b' ' && buf[index] != b'\t' && buf[index] != b'}' && buf[index] != b']' {
                    index += 1;
                }
                tokens.push(JsonToken { kind: JsonTokenType::EndNumber, index: index-1 });
            }
            b't' => {
                index += 1;
                if buf[index] == b'r' {
                    index += 1;
                    if buf[index] == b'u' {
                        index += 1;
                        if buf[index] == b'e' {
                            tokens.push(JsonToken { kind: JsonTokenType::True, index });
                            index += 1;
                        }
                    }
                }
            }
            b'f' => {
                index += 1;
                if buf[index] == b'a' {
                    index += 1;
                    if buf[index] == b'l' {
                        index += 1;
                        if buf[index] == b's' {
                            index += 1;
                            if buf[index] == b'e' {
                                tokens.push(JsonToken { kind: JsonTokenType::False, index });
                                index += 1;
                            }
                        }
                    }
                }
            }
            b'n' => {
                index += 1;
                if buf[index] == b'u' {
                    index += 1;
                    if buf[index] == b'l' {
                        index += 1;
                        if buf[index] == b'l' {
                            tokens.push(JsonToken { kind: JsonTokenType::Null, index });
                            index += 1;
                        }
                    }
                }
            },
            _ => panic!("unknown character: {} ({}) at index {} in JSON {}", c as char, c as u8, index, String::from_utf8(buf.to_vec()).unwrap())
        }
    }

    return tokens;
}
