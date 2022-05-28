use crate::{
	schema::Column,
	table::{Partition, Schema},
	util::time::ToNaiveDateTime
};
use chrono::NaiveDateTime;
use std::{env, path::PathBuf};

pub fn get_home_path() -> PathBuf {
	PathBuf::from(env::var("TICKDB_HOME").unwrap_or(String::from("")))
}

pub fn get_data_path(name: &str) -> PathBuf {
	let mut path = get_home_path();
	path.push("data");
	path.push(name);
	path
}

pub fn get_meta_path(name: &str) -> PathBuf {
	let mut path = get_data_path(name);
	path.push("_meta");
	path
}

pub fn get_partition_dir(schema: &Schema, val: i64) -> PathBuf {
	let datetime: NaiveDateTime = val.to_naive_date_time();

	let mut dir = get_data_path(&schema.name);
	if schema.partition_fmt.is_empty() {
		dir.push("all");
	} else {
		dir.push(datetime.format(&schema.partition_fmt).to_string());
	}

	dir
}

pub fn get_col_path(p: &Partition, column: &Column) -> PathBuf {
	let mut path = PathBuf::from(&p.dir);
	path.push(&column.name);
	path.set_extension(String::from(format!("{:?}", column.r#type).to_lowercase()));
	path
}
