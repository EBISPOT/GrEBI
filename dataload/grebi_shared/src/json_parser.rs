use crate::json_lexer::{JsonToken, JsonTokenType, lex};


#[derive(PartialEq)]
#[derive(Clone, Copy)]
enum NestItem {
    Object,
    Array
}

pub struct JsonParser<'a> {
    slice:&'a [u8],
    tokens:Vec<JsonToken>,
    stack:Vec<NestItem>,
    index:usize,
    saved_index:usize,
    saved_stack:Vec<NestItem>
}

impl<'a> JsonParser<'a> {

    pub fn parse(slice:&'a [u8]) -> JsonParser<'a> {
        return JsonParser { slice, tokens: lex(slice), stack:Vec::new(), index: 0, saved_index: 0, saved_stack:Vec::new() };
    }

    // hack
    fn skip_comma_if_present(&mut self) {
        if self.index < self.tokens.len() && self.tokens[self.index].kind == JsonTokenType::Comma {
            self.index += 1;
        }
    }


    pub fn begin_object(&mut self) -> JsonToken {
        let token = self.next();
        if token.kind != JsonTokenType::StartObject {
            panic!("expected object but found {:?}", token.kind);
        }
        self.stack.push(NestItem::Object);
        return token;
    }

    pub fn end_object(&mut self) -> JsonToken {
        let token = self.next();
        if token.kind != JsonTokenType::EndObject {
            panic!("Expected EndObject, found {:?}", token.kind);
        }
        if self.stack[self.stack.len() - 1] != NestItem::Object {
            panic!("EndObject outside of an object");
        }
        self.stack.pop();
        self.skip_comma_if_present();
        return token;
    }

    pub fn name(&mut self) -> &'a [u8] {
        self.skip_comma_if_present();
        if self.stack[self.stack.len() - 1] != NestItem::Object {
            panic!();
        }
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartString {
            panic!("Expected StartString for object entry name, found {:?} in {}", start_token.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndString {
            panic!("Expected EndString for object entry name, found {:?}", end_token.kind);
        }
        let colon = self.next();
        if colon.kind != JsonTokenType::Colon {
            panic!("Expected Colon, found {:?} in {}", colon.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        return &self.slice[start_token.index + 1..end_token.index];
    }
    pub fn quoted_name(&mut self) -> &'a [u8] {
        self.skip_comma_if_present();
        if self.stack[self.stack.len() - 1] != NestItem::Object {
            panic!();
        }
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartString {
            panic!("Expected StartString for object entry name, found {:?} in {}", start_token.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndString {
            panic!("Expected EndString for object entry name, found {:?} in {}", end_token.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        let colon = self.next();
        if colon.kind != JsonTokenType::Colon {
            panic!("Expected Colon, found {:?}", colon.kind);
        }
        return &self.slice[start_token.index..end_token.index+1];
    }

    pub fn string(&mut self) -> &'a [u8] {
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartString {
            panic!("Expected StartString, found {:?} in {}", start_token.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndString {
            panic!("Expected EndString, found {:?} in {}", end_token.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        self.skip_comma_if_present();
        return &self.slice[start_token.index + 1..end_token.index];
    }

    pub fn quoted_string(&mut self) -> &'a [u8] {
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartString {
            panic!();
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndString {
            panic!();
        }
        self.skip_comma_if_present();
        return &self.slice[start_token.index..end_token.index+1];
    }

    pub fn number(&mut self) -> &'a [u8] {
        let start_token = self.next();
        if start_token.kind != JsonTokenType::StartNumber {
            panic!();
        }
        let end_token = self.next();
        if end_token.kind != JsonTokenType::EndNumber {
            panic!();
        }
        self.skip_comma_if_present();
        return &self.slice[start_token.index..end_token.index+1];
    }

    pub fn begin_array(&mut self) -> JsonToken {
        let token = self.next();
        if token.kind != JsonTokenType::StartArray {
            panic!("Expected StartArray, found {:?} in {}", token.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        self.stack.push(NestItem::Array);
        return token;
    }

    pub fn end_array(&mut self) -> JsonToken {
        let token = self.next();
        if token.kind != JsonTokenType::EndArray {
            panic!("Expected EndArray, found {:?} in {}", token.kind, String::from_utf8(self.slice.to_vec()).unwrap());
        }
        if self.stack[self.stack.len() - 1] != NestItem::Array {
            panic!("EndArray called outside of an array");
        }
        self.skip_comma_if_present();
        self.stack.pop();
        return token;
    }

    pub fn value(&mut self) -> &'a [u8] {

        let token = self.peek();

        match token.kind {
            JsonTokenType::StartObject => {
                let begin_tok = self.begin_object();
                while self.peek().kind != JsonTokenType::EndObject {
                    self.name();
                    self.value();
                }
                let end_tok = self.end_object();
                return &self.slice[begin_tok.index..end_tok.index+1];
            },
            JsonTokenType::StartArray => {
                let begin_tok = self.begin_array();
                while self.peek().kind != JsonTokenType::EndArray {
                    self.value();
                }
                let end_tok = self.end_array();
                return &self.slice[begin_tok.index..end_tok.index+1];
            },
            JsonTokenType::StartString => {
                return self.quoted_string();
            },
            JsonTokenType::StartNumber => {
                return self.number();
            },
            JsonTokenType::True => {
                let _ = self.next();
                self.skip_comma_if_present();
                return b"true";
            },
            JsonTokenType::False => {
                let _ = self.next();
                self.skip_comma_if_present();
                return b"false";
            },
            JsonTokenType::Null => {
                let _ = self.next();
                self.skip_comma_if_present();
                return b"null";
            },
            JsonTokenType::EndObject => panic!("unexpected end object"),
            JsonTokenType::EndArray => panic!("unexpected end array"),
            JsonTokenType::EndString => panic!("unexpected end string"),
            JsonTokenType::Colon => panic!("unexpected colon"),
            JsonTokenType::Comma => panic!("unexpected comma"),
            JsonTokenType::EndNumber => panic!("unexpected end number"),
        }


    }



    pub fn peek(&self) -> JsonToken {
        return self.tokens[self.index].clone();
    }

    pub fn next(&mut self) -> JsonToken {
        let token = self.peek();
        self.index += 1;
        return token;
    }

    // pub fn get_index(&mut self) -> usize {
    //     return self.tokens[self.index].index;
    // }

    pub fn mark(&mut self) {
        self.saved_index = self.index;
        self.saved_stack = self.stack.clone();
    }

    pub fn rewind(&mut self) {
        self.index = self.saved_index;
        self.stack = self.saved_stack.clone();
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_an_object_field_by_field() {
        let json = br#"{"id":"x","n":-2.5,"flags":[true,false,null],"nested":{"k":"v"}}"#;
        let mut p = JsonParser::parse(json);
        p.begin_object();
        assert_eq!(p.name(), b"id");
        assert_eq!(p.string(), b"x");
        assert_eq!(p.name(), b"n");
        assert_eq!(p.number(), b"-2.5");
        assert_eq!(p.name(), b"flags");
        p.begin_array();
        assert_eq!(p.value(), b"true");
        assert_eq!(p.value(), b"false");
        assert_eq!(p.value(), b"null");
        assert_eq!(p.peek().kind, JsonTokenType::EndArray);
        p.end_array();
        assert_eq!(p.name(), b"nested");
        assert_eq!(p.value(), br#"{"k":"v"}"#, "a nested value is returned as its exact slice");
        assert_eq!(p.peek().kind, JsonTokenType::EndObject);
        p.end_object();
    }

    #[test]
    fn value_returns_the_exact_slice_of_any_kind() {
        let json = br#"["s",1,{"a":[1,{"b":2}]},[[]],true]"#;
        let mut p = JsonParser::parse(json);
        p.begin_array();
        assert_eq!(p.value(), br#""s""#, "strings keep their quotes");
        assert_eq!(p.value(), b"1");
        assert_eq!(p.value(), br#"{"a":[1,{"b":2}]}"#);
        assert_eq!(p.value(), b"[[]]");
        assert_eq!(p.value(), b"true");
        p.end_array();
    }

    #[test]
    fn quoted_variants_keep_the_quotes_and_plain_ones_drop_them() {
        let mut p = JsonParser::parse(br#"{"k":"v"}"#);
        p.begin_object();
        assert_eq!(p.quoted_name(), br#""k""#);
        assert_eq!(p.quoted_string(), br#""v""#);
        p.end_object();

        let mut p = JsonParser::parse(br#"{"k":"v"}"#);
        p.begin_object();
        assert_eq!(p.name(), b"k");
        assert_eq!(p.string(), b"v");
        p.end_object();
    }

    #[test]
    fn escapes_are_left_as_they_are_in_the_source() {
        let mut p = JsonParser::parse(br#"{"a\"b":"c\\d\u00e9"}"#);
        p.begin_object();
        assert_eq!(p.name(), br#"a\"b"#);
        assert_eq!(p.string(), br#"c\\d\u00e9"#);
        p.end_object();
    }

    #[test]
    fn mark_and_rewind_replay_from_the_saved_position() {
        let mut p = JsonParser::parse(br#"{"a":[1,2],"b":3}"#);
        p.begin_object();
        assert_eq!(p.name(), b"a");
        p.mark();
        assert_eq!(p.value(), b"[1,2]");
        assert_eq!(p.name(), b"b");
        p.rewind();
        assert_eq!(p.value(), b"[1,2]", "the array is read again after rewinding");
        assert_eq!(p.name(), b"b");
        assert_eq!(p.number(), b"3");
        p.end_object();
    }

    #[test]
    fn empty_containers() {
        let mut p = JsonParser::parse(br#"{"a":{},"b":[]}"#);
        p.begin_object();
        assert_eq!(p.name(), b"a");
        assert_eq!(p.value(), b"{}");
        assert_eq!(p.name(), b"b");
        p.begin_array();
        p.end_array();
        p.end_object();
    }

    #[test]
    #[should_panic(expected = "expected object")]
    fn begin_object_on_an_array_panics() {
        JsonParser::parse(b"[]").begin_object();
    }

    #[test]
    #[should_panic(expected = "Expected EndObject")]
    fn end_object_before_the_end_panics() {
        let mut p = JsonParser::parse(br#"{"a":1}"#);
        p.begin_object();
        p.end_object();
    }

    #[test]
    #[should_panic(expected = "Expected StartString for object entry name")]
    fn a_name_must_be_a_string() {
        let mut p = JsonParser::parse(b"{1:2}");
        p.begin_object();
        p.name();
    }
}
