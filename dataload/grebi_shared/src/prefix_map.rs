

use std::collections::HashMap;
use std::mem::size_of;

struct BuilderNode {
    to_prefix:String,
    children:HashMap<u8, BuilderNode>
}

impl BuilderNode {

    fn to_buf(&self) -> Vec<u8> {

        let mut inner_buf:Vec<u8> = Vec::new();
        inner_buf.push(self.to_prefix.len() as u8);
        inner_buf.extend(self.to_prefix.as_bytes());
        for (k, v) in &self.children {
            inner_buf.push(*k);
            inner_buf.extend(v.to_buf());
        }

        let mut outer_buf:Vec<u8> = Vec::new();
        outer_buf.extend((inner_buf.len() as usize).to_ne_bytes().iter());
        outer_buf.extend(inner_buf);
        return outer_buf;
    }

}

pub struct PrefixMapBuilder {
    root_node:BuilderNode
}

impl PrefixMapBuilder {

    pub fn new() -> PrefixMapBuilder {
        return PrefixMapBuilder { root_node: BuilderNode { to_prefix:String::new(), children: HashMap::new() } };
    }

    pub fn add_mapping(&mut self, from_prefix:String, to_prefix:String) {
        self.get_or_create_node(from_prefix).to_prefix = to_prefix;
    }

    fn get_or_create_node(&mut self, from_prefix:String) -> &mut BuilderNode {

        let mut cur_node = &mut self.root_node;

        let bytes = from_prefix.as_bytes();

        for n in 0..bytes.len() {
            let b = bytes[n].to_ascii_lowercase();
            if !cur_node.children.contains_key(&b) {
                let new_node = BuilderNode { to_prefix:String::new(), children: HashMap::new() };
                cur_node.children.insert(b, new_node);
            }
            cur_node = cur_node.children.get_mut(&b).unwrap();
        }

        return cur_node;
    }

    pub fn build(&self) -> PrefixMap {
        let mut buf = self.root_node.to_buf();
        buf.shrink_to_fit();
        return PrefixMap { buf }
    }

}






pub struct PrefixMap {
    pub buf:Vec<u8>
}

impl PrefixMap {

    // pub fn compact(&self, subject:&[u8]) -> Option<Vec<u8>> {
    //     return compact_impl(subject, &self.buf);
    // }
    pub fn reprefix(&self, subject:&String) -> String {
        let res = self.reprefix_bytes(subject.as_bytes());
        if res.is_some() {
            return String::from_utf8(res.unwrap()).unwrap();
        } else {
            return subject.clone();
        }
    }

    pub fn maybe_reprefix(&self, subject:&String) -> Option<String> {
        let res = self.reprefix_bytes(subject.as_bytes());
        if res.is_some() {
            return Some(String::from_utf8(res.unwrap()).unwrap());
        } else {
            return None;
        }
    }

    pub fn reprefix_bytes(&self, subject:&[u8]) -> Option<Vec<u8>> {
        return reprefix_impl(subject, &self.buf[
            /* skip unused inner_size and curie_len of root node */
            (size_of::<usize>() + 1)..(self.buf.len())
            ]);
    }


}





/* compacted tree structure
Node {
    letter:u8
    inner_size:usize

    Inner {
        to_prefix_size:u8
        to_prefix:[u8]
        children:Node[...]
    }
}
*/
#[inline(always)]
fn reprefix_impl<'a>(subject:&[u8], buf:&[u8]) -> Option<Vec<u8>> {

    if subject.len() == 0 || buf.len() == 0 {
        return None;
    }

    let mut buf_index:usize = 0;

    loop {

        let buf_b = buf[buf_index];
        buf_index += 1; // letter

        let inner_size = usize::from_ne_bytes(
            buf[buf_index..buf_index+size_of::<usize>()].try_into().unwrap());
        buf_index += size_of::<usize>(); // inner_size

        let inner_buf = &buf[buf_index..buf_index+inner_size];
        buf_index += inner_size;

        // println!("subject {}", String::from_utf8( subject.to_vec()).unwrap() );
        //println!("letter {}, inner size {}, buf index {}, subject index {}", String::from_utf8([ buf_b ].to_vec()).unwrap(), inner_size, buf_index, 0);

        let subject_b = subject[0];

        if buf_b.to_ascii_lowercase() == subject_b.to_ascii_lowercase() {
            // the node at the beginning of buf is a match
            // see if we can get a longer match from its children
            //
            let to_prefix_size = inner_buf[0];
            let children_buf = &inner_buf[(1+to_prefix_size as usize)..inner_buf.len()];

            if children_buf.len() > 0 {

                let longer_match = reprefix_impl(&subject[1..subject.len()], &children_buf);
                    
                if longer_match.is_some() {
                    return longer_match;
                }
            }

            // there are no matching children (no longer prefixes than this that match)
            // do we have a to prefix? if so we have found our best match
            //
            if to_prefix_size > 0 {
                let to_prefix:&[u8] = &inner_buf[1..1+to_prefix_size as usize];
                return Some([to_prefix, &subject[1..subject.len()]].concat());
            }


            // there are no matching children and there is no curie assigned to this node
            // = we are on an intermediate node but nothing else has the same next char as our subject
            // = no match
            return None;

        } else {
            // the node at the beginning of buf does not match
            // continue to the next node?

            if buf_index == buf.len() {
                // reached the end of the buffer
                return None;
            }
        }
    }


}


#[cfg(test)]
mod tests {
    use super::*;

    fn map(mappings: &[(&str, &str)]) -> PrefixMap {
        let mut b = PrefixMapBuilder::new();
        for (from, to) in mappings {
            b.add_mapping(from.to_string(), to.to_string());
        }
        b.build()
    }

    #[test]
    fn replaces_a_matching_prefix_and_keeps_the_rest() {
        let m = map(&[("MONDO_", "mondo:"), ("http://purl.obolibrary.org/obo/", "obo:")]);
        assert_eq!(m.reprefix(&"MONDO_0005083".to_string()), "mondo:0005083");
        assert_eq!(m.reprefix(&"http://purl.obolibrary.org/obo/HP_0000001".to_string()), "obo:HP_0000001");
        assert_eq!(m.maybe_reprefix(&"MONDO_1".to_string()), Some("mondo:1".to_string()));
        assert_eq!(m.reprefix_bytes(b"MONDO_1"), Some(b"mondo:1".to_vec()));
    }

    #[test]
    fn the_longest_matching_prefix_wins() {
        let m = map(&[("http://purl.obolibrary.org/obo/", "obo:"), ("http://purl.obolibrary.org/obo/MONDO_", "mondo:")]);
        assert_eq!(m.reprefix(&"http://purl.obolibrary.org/obo/MONDO_0005083".to_string()), "mondo:0005083");
        assert_eq!(m.reprefix(&"http://purl.obolibrary.org/obo/HP_1".to_string()), "obo:HP_1");
    }

    #[test]
    fn matching_ignores_case_but_the_replacement_is_as_given() {
        let m = map(&[("mondo:", "MONDO:")]);
        assert_eq!(m.reprefix(&"Mondo:1".to_string()), "MONDO:1");
        assert_eq!(m.reprefix(&"MONDO:1".to_string()), "MONDO:1");
        let m = map(&[("HP:", "hp:")]);
        assert_eq!(m.reprefix(&"hp:1".to_string()), "hp:1");
    }

    #[test]
    fn subjects_without_a_mapping_are_left_alone() {
        let m = map(&[("mondo:", "mondo:"), ("efo:", "efo:")]);
        assert_eq!(m.maybe_reprefix(&"doid:1".to_string()), None);
        assert_eq!(m.reprefix(&"doid:1".to_string()), "doid:1");
        // a subject that stops on an intermediate node of the trie has no mapping either
        assert_eq!(m.maybe_reprefix(&"mon".to_string()), None);
        assert_eq!(m.maybe_reprefix(&"mondx:1".to_string()), None);
        assert_eq!(m.maybe_reprefix(&"".to_string()), None);
        assert_eq!(map(&[]).maybe_reprefix(&"mondo:1".to_string()), None);
    }

    #[test]
    fn a_mapping_to_the_empty_prefix_is_no_mapping() {
        let m = map(&[("obo:", "")]);
        assert_eq!(m.maybe_reprefix(&"obo:1".to_string()), None);
    }

    #[test]
    fn a_shorter_prefix_still_applies_when_the_longer_one_does_not_match() {
        let m = map(&[("a", "A:"), ("ab", "AB:"), ("abc", "ABC:")]);
        assert_eq!(m.reprefix(&"abcd".to_string()), "ABC:d");
        assert_eq!(m.reprefix(&"abd".to_string()), "AB:d");
        assert_eq!(m.reprefix(&"ad".to_string()), "A:d");
        // exact matches with nothing left over
        assert_eq!(m.reprefix(&"abc".to_string()), "ABC:");
    }

    #[test]
    fn many_siblings_resolve_whatever_order_the_trie_stores_them_in() {
        let mappings: Vec<(String, String)> = (0..40).map(|i| (format!("p{:02}:", i), format!("q{:02}:", i))).collect();
        let mut b = PrefixMapBuilder::new();
        for (from, to) in &mappings {
            b.add_mapping(from.clone(), to.clone());
        }
        let m = b.build();
        for (from, to) in &mappings {
            assert_eq!(m.reprefix(&format!("{}x", from)), format!("{}x", to));
        }
        assert_eq!(m.maybe_reprefix(&"p99:x".to_string()), None);
    }

    #[test]
    fn several_spellings_can_map_to_one_prefix() {
        let m = map(&[("efo:", "efo:"), ("efo_", "efo:"), ("http://www.ebi.ac.uk/efo/EFO_", "efo:")]);
        for s in ["efo:1", "EFO_1", "http://www.ebi.ac.uk/efo/EFO_1"] {
            assert_eq!(m.reprefix(&s.to_string()), "efo:1", "{}", s);
        }
    }
}
