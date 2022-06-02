pub mod v8;
use serde::{ser::Serializer, Serialize};
use std::time::Duration;

const SCAN_FN_NAME: &str = "scan";

pub trait Lang {
	fn init();
	fn deinit();
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
	pub elapsed:      Duration,
	#[serde(serialize_with = "pretty_duration")]
	pub elapsed_loop: Duration,
	pub row_count:    u64,
	pub bytes_read:   u64,
	pub bytes_out:    u64,
	#[serde(skip)]
	pub bytes:        Vec<u8>
}
