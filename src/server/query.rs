use crate::server::langs::v8::V8;
use chrono::{DateTime, NaiveDate};
use httparse::Request;
use log::{debug, info};
use serde::{de, Deserialize};
use std::{
	collections::HashMap,
	ffi::OsStr,
	io::{Error, ErrorKind},
	path::Path,
	time::Instant
};
use tickdb::table::Table;

#[derive(Debug, Deserialize, PartialEq, Eq)]
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

pub fn handle_query(_req: &Request, query: &[u8], tables: &HashMap<String, Table>) -> Vec<u8> {
	match serde_json::from_slice::<Query>(query) {
		Err(err) => {
			let q = std::str::from_utf8(query).unwrap();
			eprintln!("{} parsing query {}", err, q);
			Vec::from(format!("error parsing query {}\n", err))
		}
		Ok(mut query) => {
			if query.lang == QueryLang::Unknown {
				query.lang = guess_query_lang(&query.source.path);
			}
			let start_time = Instant::now();
			let scan = match query.lang {
				QueryLang::JavaScript => V8::scan(&query, tables),
				QueryLang::Unknown => {
					let msg = "must specify query language or have known file extension";
					Err(Error::new(ErrorKind::Other, msg))
				}
				lang => {
					let msg = format!("unsupported lang {:?}", lang);
					Err(Error::new(ErrorKind::Other, msg))
				}
			};
			println!("scan finished in {:?}", start_time.elapsed());

			match scan {
				Ok(res) => {
					info!("success {}", serde_json::to_string(&res).unwrap());
					let elapsed_loop = res.elapsed_loop.as_secs_f64();
					info!(
						"loop {} GBps {} Mrps",
						res.bytes_read as f64 / elapsed_loop / 1e9,
						res.row_count as f64 / elapsed_loop / 1e6
					);
					res.bytes
				}
				Err(err) => Vec::from(err.to_string())
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
