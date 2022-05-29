use crate::server::langs::v8::V8;
use chrono::{DateTime, NaiveDate};
use httparse::Request;
use serde::{de, Deserialize};
use std::io::{Error, ErrorKind};

#[derive(Deserialize)]
enum QueryLang {
	JavaScript,
	Julia,
	Python
}

#[derive(Deserialize)]
pub struct Query {
	pub table: String,
	#[serde(deserialize_with = "string_to_datetime")]
	pub from:  i64,
	#[serde(deserialize_with = "string_to_datetime")]
	pub to:    i64,
	pub query: String
}

pub fn handle_query(_req: &Request, query: &[u8]) -> Vec<u8> {
	match serde_json::from_slice::<Query>(query) {
		Err(err) => {
			let q = std::str::from_utf8(query).unwrap();
			eprintln!("{} parsing query {}", err, q);
			Vec::from(format!("error parsing query {}\n", err))
		}
		Ok(query) => match V8::scan(&query) {
			Ok(res) => res,
			Err(err) => Vec::from(err.to_string())
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
