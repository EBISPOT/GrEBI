
pub fn check_headers(got:Vec<&str>, expected:Vec<&str>) {

    if got.len() != expected.len() {
        panic!("Expected {} headers, but found {}", expected.len(), got.len());
    }

    for n in 0..expected.len() {
        if got[n] != expected[n] {
            panic!("Expected header {} to be {}, but found {}", n, expected[n], got[n]);
        }
    }
}

