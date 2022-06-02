mod columns;
mod meta;
pub mod paths;
pub mod read;
mod write;
use crate::{schema::*, table::meta::read_meta};
use paths::*;
use serde::{Deserialize, Serialize};
use std::{
	fs::{create_dir_all, remove_dir_all},
	io::{Error, ErrorKind, Result},
	path::PathBuf
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MinMax<T> {
	pub min: T,
	pub max: T
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Partition {
	pub dir:       PathBuf,
	pub ts_range:  MinMax<i64>,
	pub ts_bounds: MinMax<i64>,
	pub row_count: usize
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
	pub schema:      Schema,
	pub partitions:  Vec<Partition>,
	#[serde(skip)]
	meta_path:       PathBuf,
	#[serde(skip)]
	column_index:    usize,
	#[serde(skip)]
	partition_index: usize
}

impl Table {
	pub fn create(schema: Schema) -> Result<Table> {
		let data_path = get_data_path(&schema.name);
		let meta_path = get_meta_path(&schema.name);

		if meta_path.exists() {
			remove_dir_all(&data_path).expect(&format!("rm -rf {:?}", data_path));
		}
		create_dir_all(&data_path).unwrap_or_else(|_| panic!("Cannot create dir {:?}", data_path));

		let mut table = Table {
			schema,
			column_index: 0,
			partitions: Vec::new(),
			partition_index: 0,
			meta_path
		};
		table.write_meta()?;

		Ok(table)
	}

	pub fn open<'b>(name: &'b str) -> Result<Table> {
		let meta_path = get_meta_path(&name);
		let mut res = read_meta(&name, &meta_path).map_err(|e| {
			let msg = format!("could not open table {}: {}", name, e);
			Error::new(ErrorKind::NotFound, msg)
		})?;
		res.meta_path = meta_path;

		Ok(res)
	}

	pub fn create_or_open(schema: Schema) -> Result<Table> {
		let name = schema.name.clone();
		match Self::create(schema) {
			Ok(table) => Ok(table),
			Err(_) => Self::open(&name)
		}
	}
}
