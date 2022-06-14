use crate::server::langs::{v8::V8, julia::Julia, Lang, SCAN_FN_NAME};
use chrono::{DateTime, NaiveDate};
use tiny_http::Response;
use log::info;
use serde::{de, Deserialize};
use std::{
	collections::HashMap,
	ffi::OsStr,
	fmt::{Display, Formatter},
	io::{Error, ErrorKind},
	path::Path,
	time::Instant
};
use tickdb::table::Table;
use v8::{HandleScope, Isolate};

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

struct TimeStats {
	start: Instant,
	secs: f64
}

impl Default for TimeStats {
	fn default() -> Self {
		Self {
			start:  Instant::now(),
			secs: 0.0
		}
	}
}
struct ScanStats {
	lang: QueryLang,
	row_count:  u64,
	byte_count: u64,
	time_eval: TimeStats,
	time_loop: TimeStats,
	time_serialize: TimeStats,
}

impl ScanStats {
	pub fn new(lang: QueryLang) -> Self {
		Self {
			lang,
			row_count: 0,
			byte_count: 0,
			time_eval: Default::default(),
			time_loop: Default::default(),
			time_serialize: Default::default()
		}
	}
}

impl Display for ScanStats {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
		write!(f, "ScanStats {{\n")?;
		write!(f, "  lang: {:?}\n", self.lang)?;
		write!(f, "  row_count: {}\n", self.row_count)?;
		write!(f, "  byte_count: {}\n", self.byte_count)?;
		write!(f, "  time_eval: {}\n", self.time_eval.secs)?;
		write!(f, "  time_loop: {}\n", self.time_loop.secs)?;
		write!(f, "  time_serialize: {}\n", self.time_serialize.secs)?;
		write!(
			f,
			"  loop_GBps: {}\n",
			self.byte_count as f64 / self.time_loop.secs / 1e9
		)?;
		write!(
			f,
			"  loop_Mrps: {}\n",
			self.row_count as f64 / self.time_loop.secs / 1e6
		)?;
		write!(f, "}}")
	}
}

macro_rules! error_code {
	( $code:expr, $msg:expr ) => {
		return Err(($code, $msg.to_string()))
	};
}

fn handle_scan(
	query: &Query,
	tables: &HashMap<String, Table>
) -> Result<Vec<u8>, (u16, String)> {
	let mut stats = ScanStats::new(query.lang);
	// V8 requires isolate and isolate_scope be on the stack for the duration of
	// a scan. If you can find a way to get them out of here and into "struct V8"
	// please do so.
	let mut isolate = match query.lang {
		QueryLang::JavaScript => Some(Isolate::new(v8::CreateParams::default())),
		_ => None
	};
	let mut isolate_scope = match query.lang {
		QueryLang::JavaScript => Some(HandleScope::new(isolate.as_mut().unwrap())),
		_ => None
	};

	let mut lang: Box<dyn Lang> = match query.lang {
		QueryLang::JavaScript => {
			let scope = isolate_scope.as_mut().unwrap();
			let res = V8::new(&query, scope);
			if let Err(e) = res {
				error_code!(422, e);
			}
			Box::new(res.unwrap())
		}
		QueryLang::Julia => {
			let res = Julia::new(&query);
			if let Err(e) = res {
				error_code!(422, e);
			}
			Box::new(res.unwrap())
		}
		QueryLang::Unknown => {
			error_code!(422, format!("unknown query language {:?}", query.lang))
		}
		lang => error_code!(422, format!("unsupported lang {:?}", lang))
	};

	let table = match tables.get(&query.table) {
		None => error_code!(404, format!("table {} not loaded", query.table)),
		Some(t) => t
	};

	let cols = match lang.get_cols(&table.schema.columns) {
		Err(e) => error_code!(500, e),
		Ok(c) => c
	};
	if cols.len() == 0 {
		error_code!(
			422,
			format!("function {} must have arguments", SCAN_FN_NAME)
		);
	}
	stats.time_eval.secs = stats.time_eval.start.elapsed().as_secs_f64();

	stats.time_loop.start = Instant::now();
	let partitions = match table.partition_iter(query.from, query.to, cols) {
		Ok(p) => p,
		Err(e) => {
			error_code!(422, e)
		}
	};
	for p in partitions {
		stats.row_count += p[0].row_count as u64;
		for c in p.iter() {
			stats.byte_count += c.slice.len() as u64;
		}
		if let Err(e) = lang.scan_partition(p) {
			error_code!(422, format!("runtime error in {}: {}", SCAN_FN_NAME, e))
		}
	}
	stats.time_loop.secs = stats.time_loop.start.elapsed().as_secs_f64();
	
	stats.time_serialize.start = Instant::now();
	let res = match lang.serialize() {
		Err(e) => error_code!(500, format!("error serializing scan_ans: {}", e)),
		Ok(bytes) => bytes
	};
	stats.time_serialize.secs = stats.time_serialize.start.elapsed().as_secs_f64();

	info!("{}", stats);
	Ok(res)
}

pub fn handle_query(
	query: &[u8],
	tables: &HashMap<String, Table>
) -> Response<std::io::Cursor<Vec<u8>>> {
	match serde_json::from_slice::<Query>(query) {
		Err(err) => {
			let q = std::str::from_utf8(query).unwrap();
			let msg = format!("error parsing query {}. query: {}\n", err, q);
			Response::from_string(msg).with_status_code(400)
		}
		Ok(mut query) => {
			if query.lang == QueryLang::Unknown {
				query.lang = guess_query_lang(&query.source.path);
			}
			match handle_scan(&query, tables) {
				Err((code, msg)) => Response::from_string(msg).with_status_code(code),
				Ok(bytes) => Response::from_data(bytes),
			}
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

		fn expecting(
			&self,
			formatter: &mut std::fmt::Formatter
		) -> std::fmt::Result {
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
