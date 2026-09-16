
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


#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(json: &str) -> Vec<JsonTokenType> {
        lex(json.as_bytes()).into_iter().map(|t| t.kind).collect()
    }

    fn token(json: &str, n: usize) -> JsonToken {
        lex(json.as_bytes())[n].clone()
    }

    #[test]
    fn structural_tokens_carry_their_byte_index() {
        let tokens = lex(b"{\"a\":[1,true,null,false]}");
        use JsonTokenType::*;
        let expected = vec![
            (StartObject, 0), (StartString, 1), (EndString, 3), (Colon, 4), (StartArray, 5),
            (StartNumber, 6), (EndNumber, 6), (Comma, 7), (True, 11), (Comma, 12), (Null, 16), (Comma, 17),
            (False, 22), (EndArray, 23), (EndObject, 24),
        ];
        assert_eq!(tokens.len(), expected.len());
        for (t, (kind, index)) in tokens.iter().zip(expected) {
            assert_eq!(t.kind, kind);
            assert_eq!(t.index, index, "index of {:?}", kind);
        }
    }

    #[test]
    fn whitespace_between_tokens_is_skipped() {
        assert_eq!(kinds("{ \"a\" :\t[ 1 ,\r\n 2 ] }"), kinds("{\"a\":[1,2]}"));
    }

    #[test]
    fn string_tokens_span_the_quotes_and_skip_escapes() {
        // the escaped quote and the unicode escape do not end the string
        let json = "\"a\\\"b\\u0041c\\\\\"";
        let tokens = lex(json.as_bytes());
        assert_eq!(tokens[0].kind, JsonTokenType::StartString);
        assert_eq!(tokens[0].index, 0);
        assert_eq!(tokens[1].kind, JsonTokenType::EndString);
        assert_eq!(tokens[1].index, json.len() - 1);
        assert_eq!(&json.as_bytes()[tokens[0].index + 1..tokens[1].index], b"a\\\"b\\u0041c\\\\");
        // every simple escape is accepted
        assert_eq!(kinds("\"\\/\\b\\f\\n\\r\\t\""), vec![JsonTokenType::StartString, JsonTokenType::EndString]);
    }

    #[test]
    fn number_tokens_span_the_number() {
        let json = "[-1.5e10,0]";
        let tokens = lex(json.as_bytes());
        assert_eq!(&json.as_bytes()[tokens[1].index..tokens[2].index + 1], b"-1.5e10");
        assert_eq!(&json.as_bytes()[tokens[4].index..tokens[5].index + 1], b"0");
        // a number is also ended by a closing brace
        let json = "{\"n\":42}";
        let tokens = lex(json.as_bytes());
        assert_eq!(&json.as_bytes()[tokens[4].index..tokens[5].index + 1], b"42");
        assert_eq!(tokens[6].kind, JsonTokenType::EndObject);
    }

    #[test]
    fn literals_are_single_tokens_indexed_at_their_last_byte() {
        assert_eq!(token("true", 0).index, 3);
        assert_eq!(token("false", 0).index, 4);
        assert_eq!(token("null", 0).index, 3);
    }

    #[test]
    fn empty_input_lexes_to_nothing() {
        assert!(lex(b"").is_empty());
    }

    #[test]
    #[should_panic(expected = "unknown escape sequence")]
    fn an_unknown_escape_is_rejected() {
        lex(b"\"\\x\"");
    }

    #[test]
    #[should_panic(expected = "unknown character")]
    fn an_unexpected_character_is_rejected() {
        lex(b"{\"a\":@}");
    }
}
