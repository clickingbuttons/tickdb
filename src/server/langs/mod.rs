pub mod v8;

const SCAN_FN_NAME: &str = "scan";

// #[derive(Debug)]
// struct UserSource<'a> {
// src: &'a str,
// name: &'a str
// }

pub trait Lang {
	fn init();
	fn deinit();
}
