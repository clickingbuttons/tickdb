pub mod v8;
#[allow(dead_code)]
pub mod julia_bindings;
pub mod julia;
use tickdb::table::read::PartitionColumn;

pub const SCAN_FN_NAME: &str = "scan";

pub trait Lang {
	fn get_cols(&mut self) -> std::io::Result<Vec<String>>;
	fn scan_partition(&mut self, p: Vec<PartitionColumn>) -> std::io::Result<()>;
	fn serialize(&mut self) -> std::io::Result<Vec<u8>>;
}

