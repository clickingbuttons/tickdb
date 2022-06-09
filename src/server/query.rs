use crate::server::langs::{v8::V8, SCAN_FN_NAME};
use chrono::{DateTime, NaiveDate};
use http::Response;
use httparse::Request;
use log::info;
use serde::{de, Deserialize};
use std::{
	collections::HashMap,
	ffi::OsStr,
	io::{Error, ErrorKind},
	path::Path,
	time::Instant
};
use tickdb::table::Table;

#[derive(Debug, Deserialize, PartialEq, Eq, Copy, Clone)]
pub enum QueryLang {
	Unknown,
	JavaScript,
	Julia,
	Python
}

impl Default for QueryLang {
	fn default() -> Self { QueryLang::Unknown }
}

#[derive(Debug, Deserialize)]
pub struct Source {
	pub text: String,
	pub path: String
}

#[derive(Debug, Deserialize)]
pub struct Query {
	pub table:  String,
	#[serde(deserialize_with = "string_to_datetime")]
	pub from:   i64,
	#[serde(deserialize_with = "string_to_datetime")]
	pub to:     i64,
	#[serde(default)]
	pub lang:   QueryLang,
	pub source: Source
}

pub fn guess_query_lang(path: &str) -> QueryLang {
	let ext = Path::new(path).extension().and_then(OsStr::to_str);
	match ext {
		None => QueryLang::Unknown,
		Some(l) => match l {
			"js" => QueryLang::JavaScript,
			"jl" => QueryLang::Julia,
			"py" => QueryLang::Python,
			_ => QueryLang::Unknown
		}
	}
}

#[derive(Debug)]
struct ScanStats {
	row_count:  u64,
	byte_count: u64,
	loop_start: Instant,
	loop_secs:  f64
}

impl Default for ScanStats {
	fn default() -> Self {
		Self {
			row_count:  0,
			byte_count: 0,
			loop_start: Instant::now(),
			loop_secs:  0.0
		}
	}
}

fn handle_scan(
	query: &mut Query,
	tables: &HashMap<String, Table>
) -> Result<Vec<u8>, (u16, String)> {
	if query.lang == QueryLang::Unknown {
		query.lang = guess_query_lang(&query.source.path);
	}
	let mut isolate = match query.lang {
		QueryLang::JavaScript => Some(v8::Isolate::new(v8::CreateParams::default())),
		_ => None
	};
	let mut isolate_scope = match query.lang {
		QueryLang::JavaScript => Some(v8::HandleScope::new(isolate.as_mut().unwrap())),
		_ => None
	};

	let mut lang = match query.lang {
		QueryLang::JavaScript => V8::new(&query, isolate_scope.as_mut().unwrap()).unwrap(),
		QueryLang::Unknown => {
			let msg = "must specify query language or have known file extension";
			return Err((422, msg.to_string()));
		}
		lang => {
			let msg = format!("unsupported lang {:?}", lang);
			return Err((422, msg.to_string()));
		}
	};

	let table = match tables.get(&query.table) {
		None => {
			return Err((404, format!("table {} not loaded", query.table)));
		}
		Some(t) => t
	};

	let cols = match lang.get_cols() {
		Err(e) => {
			return Err((500, e.to_string()));
		}
		Ok(c) => c
	};
	if cols.len() == 0 {
		return Err((
			422,
			format!("function {} must have arguments", SCAN_FN_NAME)
		));
	}

	let mut stats = ScanStats::default();

	let partitions = table.partition_iter(query.from, query.to, cols);
	for p in partitions {
		stats.row_count += p[0].row_count as u64;
		for c in p.iter() {
			stats.byte_count += c.slice.len() as u64;
		}
		if let Err(e) = lang.scan_partition(p) {
			return Err((422, format!("runtime error in {}: {}", SCAN_FN_NAME, e)));
		}
	}

	stats.loop_secs = stats.loop_start.elapsed().as_secs_f64();

	match lang.serialize() {
		Err(e) => {
			return Err((500, format!("error serializing scan_ans: {}", e)));
		}
		Ok(bytes) => {
			info!(
				"loop {} GBps {} Mrps",
				stats.byte_count as f64 / stats.loop_secs / 1e9,
				stats.row_count as f64 / stats.loop_secs / 1e6
			);
			Ok(bytes)
		}
	}
}

pub fn handle_query(
	_req: &Request,
	query: &[u8],
	tables: &HashMap<String, Table>
) -> Response<Vec<u8>> {
	let resp = Response::builder();
	match serde_json::from_slice::<Query>(query) {
		Err(err) => {
			let q = std::str::from_utf8(query).unwrap();
			let msg = format!("error parsing query {}. query: {}\n", err, q);
			resp.status(400).body(Vec::from(msg)).unwrap()
		}
		Ok(mut query) => match handle_scan(&mut query, tables) {
			Err((code, msg)) => resp.status(code).body(Vec::from(msg)).unwrap(),
			Ok(bytes) => resp.body(bytes).unwrap()
		}
	}
}

static NICE_FORMAT: &str = "%Y-%m-%d";
pub fn string_to_nanoseconds(value: &str) -> std::io::Result<i64> {
	// Nanoseconds since epoch?
	if value.len() > 4 {
		let nanoseconds = value.parse::<i64>();
		if let Ok(nanoseconds) = nanoseconds {
			return Ok(nanoseconds);
		}
	}
	// TODO: check date is in valid range before calling timestamp_nanos
	match DateTime::parse_from_rfc3339(&value) {
		Ok(date) => Ok(date.timestamp_nanos()),
		Err(_e) => match NaiveDate::parse_from_str(&value, &NICE_FORMAT) {
			Ok(date) => Ok(date.and_hms(0, 0, 0).timestamp_nanos()),
			Err(_e) => {
				let msg = format!(
					"Could not parse {} in RFC3339 or {} format",
					&value, &NICE_FORMAT
				);
				Err(Error::new(ErrorKind::Other, msg))
			}
		}
	}
}

fn string_to_datetime<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
	D: de::Deserializer<'de>
{
	struct StringVisitor;

	impl<'de> de::Visitor<'de> for StringVisitor {
		type Value = i64;

		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("a rfc 3339 string")
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: de::Error
		{
			match string_to_nanoseconds(value) {
				Ok(nanos) => Ok(nanos),
				Err(e) => Err(E::custom(e))
			}
		}

		fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
		where
			E: de::Error
		{
			Ok(value as i64)
		}

		fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
		where
			E: de::Error
		{
			Ok(value)
		}
	}
	deserializer.deserialize_any(StringVisitor)
}
