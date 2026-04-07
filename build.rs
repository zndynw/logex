fn main() {
    let build_date = chrono::Local::now().format("%Y-%m-%d").to_string();
    println!("cargo:rustc-env=LOGEX_BUILD_DATE={build_date}");
}
