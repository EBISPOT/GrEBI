

use std::collections::HashMap;
use std::mem::size_of;

struct BuilderNode {
    curie_prefix:String,
    children:HashMap<u8, BuilderNode>
}

impl BuilderNode {

    fn to_buf(&self) -> Vec<u8> {

        let mut inner_buf:Vec<u8> = Vec::new();
        inner_buf.push(self.curie_prefix.len() as u8);
        inner_buf.extend(self.curie_prefix.as_bytes());
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
        return PrefixMapBuilder { root_node: BuilderNode { curie_prefix:String::new(), children: HashMap::new() } };
    }

    pub fn add_mapping(&mut self, iri_prefix:String, curie_prefix:String) {
        self.get_or_create_node(iri_prefix).curie_prefix = curie_prefix;
    }

    fn get_or_create_node(&mut self, iri_prefix:String) -> &mut BuilderNode {

        let mut cur_node = &mut self.root_node;

        let bytes = iri_prefix.as_bytes();

        for n in 0..bytes.len() {
            let b = bytes[n];
            if !cur_node.children.contains_key(&b) {
                let new_node = BuilderNode { curie_prefix:String::new(), children: HashMap::new() };
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

    // pub fn compact(&self, iri:&[u8]) -> Option<Vec<u8>> {
    //     return compact_impl(iri, &self.buf);
    // }
    pub fn compact(&self, iri:&String) -> String {
        let iri_bytes = iri.as_bytes();

        let res = compact_impl(iri_bytes, &self.buf[
            /* skip unused inner_size and curie_len of root node */
            (size_of::<usize>() + 1)..(self.buf.len())
            ]);

        if res.is_none() {
            return iri.clone();
        }
        return String::from_utf8(res.unwrap()).unwrap();
    }


}





/* compacted tree structure
Node {
    letter:u8
    inner_size:usize

    Inner {
        curie_prefix_size:u8
        curie_prefix:[u8]
        children:Node[...]
    }
}
*/
#[inline(always)]
fn compact_impl<'a>(iri:&[u8], buf:&[u8]) -> Option<Vec<u8>> {

    if iri.len() == 0 {
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

        // println!("iri {}", String::from_utf8( iri.to_vec()).unwrap() );
        //println!("letter {}, inner size {}, buf index {}, iri index {}", String::from_utf8([ buf_b ].to_vec()).unwrap(), inner_size, buf_index, 0);

        let iri_b = iri[0];

        if buf_b == iri_b {
            // the node at the beginning of buf is a match
            // see if we can get a longer match from its children
            //
            let curie_prefix_size = inner_buf[0];
            let children_buf = &inner_buf[(1+curie_prefix_size as usize)..inner_buf.len()];

            if children_buf.len() > 0 {

                let longer_match = compact_impl(&iri[1..iri.len()], &children_buf);
                    
                if longer_match.is_some() {
                    return longer_match;
                }
            }

            // there are no matching children (no longer prefixes than this that match)
            // do we have a curie?
            //
            if curie_prefix_size > 0 {

                let curie_prefix:&[u8] = &inner_buf[1..1+curie_prefix_size as usize];
                return Some([curie_prefix, ":".as_bytes(), &iri[1..iri.len()]].concat());

            }


            // there are no matching children and there is no curie assigned to this node
            // = we are on an intermediate node but nothing else has the same next char as our iri
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

