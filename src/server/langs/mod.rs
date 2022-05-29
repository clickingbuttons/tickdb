pub mod v8;

const SCAN_FN_NAME: &str = "scan";

pub trait Lang {
	fn init();
	fn deinit();
}
