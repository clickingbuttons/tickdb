pub mod v8;
use serde::{ser::Serializer, Serialize};
use std::time::Duration;
use tickdb::table::read::PartitionColumn;

pub const SCAN_FN_NAME: &str = "scan";

pub trait Lang {
	fn init();
	fn deinit();
	fn get_cols(&mut self) -> std::io::Result<Vec<String>>;
	fn scan_partition(&mut self, p: Vec<PartitionColumn>) -> std::io::Result<()>;
	fn serialize(&mut self) -> std::io::Result<Vec<u8>>;
}

pub fn pretty_duration<S>(duration: &Duration, s: S) -> Result<S::Ok, S::Error>
where
	S: Serializer
{
	s.serialize_str(&format!("{:?}", duration))
}

#[derive(Serialize)]
pub struct LangScanRes {
	#[serde(serialize_with = "pretty_duration")]
	pub elapsed_loop: Duration,
	pub row_count:    u64,
	pub bytes_read:   u64,
	pub bytes_out:    u64,
	#[serde(skip)]
	pub bytes:        Vec<u8>
}
