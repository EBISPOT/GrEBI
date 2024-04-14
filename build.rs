fn main() {
    println!("cargo:rustc-link-search=.");
    println!("cargo:rustc-link-lib=nl_data");
}
