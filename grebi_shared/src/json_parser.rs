use crate::json_lexer::{JsonToken, JsonTokenType};



#[derive(PartialEq)]
enum NestItem {
    Object,
    Array
}

pub struct JsonParser {
    tokens:Vec<JsonToken>,
    stack:Vec<NestItem>,
    index:usize
}

impl JsonParser {

    pub fn from_lexed(tokens:Vec<JsonToken>) -> JsonParser {
        return JsonParser { tokens, stack:Vec::new(), index: 0 };
    }

    // hack
    fn skip_comma_if_present(&mut self) {
        if self.peek().kind == JsonTokenType::Comma {
            self.next();
        }
    }


    pub fn begin_object(&mut self) {
        self.skip_comma_if_present();
        let token = self.next();
        if token.kind != JsonTokenType::StartObject {
            panic!();
        }
        self.stack.push(NestItem::Object);
    }

    pub fn end_object(&mut self) {
        let token = self.next();
        if token.kind != JsonTokenType::EndObject {
            panic!();
        }
        if self.stack[self.stack.len() - 1] != NestItem::Object {
            panic!();
        }
        self.stack.pop();
    }

    pub fn name<'a>(&mut self, buf:&'a Vec<u8>) -> &'a [u8] {
        self.skip_comma_if_present();
        if self.stack[self.stack.len() - 1] != NestItem::Object {
            panic!();
        }
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartString {
            panic!("Expected StartString, found {:?} in {}", start_token.kind, String::from_utf8(buf.to_vec()).unwrap());
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndString {
            panic!();
        }
        let colon = self.next();
        if colon.kind != JsonTokenType::Colon {
            panic!();
        }
        return &buf[start_token.index + 1..end_token.index];
    }

    pub fn string<'a>(&mut self, buf:&'a Vec<u8>) -> &'a [u8] {
        self.skip_comma_if_present();
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartString {
            panic!();
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndString {
            panic!();
        }
        return &buf[start_token.index + 1..end_token.index];
    }

    pub fn number<'a>(&mut self, buf:&'a Vec<u8>) -> &'a [u8] {
        self.skip_comma_if_present();
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartNumber {
            panic!();
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndNumber {
            panic!();
        }
        return &buf[start_token.index + 1..end_token.index];
    }

    pub fn begin_array(&mut self) {
        self.skip_comma_if_present();
        let token = self.next();
        if token.kind != JsonTokenType::StartArray {
            panic!("Expected StartArray, found {:?}", token.kind);
        }
        self.stack.push(NestItem::Array);
    }

    pub fn end_array(&mut self) {
        let token = self.next();
        if token.kind != JsonTokenType::EndArray {
            panic!();
        }
        if self.stack[self.stack.len() - 1] != NestItem::Array {
            panic!();
        }
        self.stack.pop();
    }

    pub fn value<'a>(&mut self, buf:&'a Vec<u8>) -> &'a [u8] {

        self.skip_comma_if_present();

        let token = self.peek();

        match token.kind {
            JsonTokenType::StartObject => {
                let begin_tok = self.peek();
                self.begin_object();
                while self.peek().kind != JsonTokenType::EndObject {
                    self.name(&buf);
                    self.value(&buf);
                }
                self.end_object();
                let end_tok = self.peek();
                return &buf[begin_tok.index..end_tok.index];
            },
            JsonTokenType::StartArray => {
                let begin_tok = self.peek();
                self.begin_array();
                while self.peek().kind != JsonTokenType::EndArray {
                    self.value(&buf);
                }
                self.end_array();
                let end_tok = self.peek();
                return &buf[begin_tok.index..end_tok.index];
            },
            JsonTokenType::StartString => {
                let begin_tok = self.peek();
                self.string(&buf);
                let end_tok = self.peek();
                return &buf[begin_tok.index..end_tok.index];
            },
            JsonTokenType::StartNumber => {
                let begin_tok = self.peek();
                self.number(buf);
                let end_tok = self.peek();
                return &buf[begin_tok.index..end_tok.index];
            },
            JsonTokenType::True => {
                return b"true";
            },
            JsonTokenType::False => {
                return b"false";
            },
            JsonTokenType::Null => {
                return b"null";
            },
            _ => {
                panic!("unknown json token type");
            }
        }


    }



    pub fn peek(&mut self) -> JsonToken {
        return self.tokens[self.index].clone();
    }

    pub fn next(&mut self) -> JsonToken {
        let token = self.peek();
        self.index += 1;
        return token;
    }

    pub fn get_index(&mut self) -> usize {
        return self.tokens[self.index].index;
    }


}
