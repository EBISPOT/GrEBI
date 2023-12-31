
fn main() {
    cc::Build::new()
        .cpp(true)
        .std("c++11")
        .file("grebi_leveldb.cpp")
        .compile("grebi_leveldb");
}
