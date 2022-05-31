use crate::table::{Table, ColumnType, ColumnSymbolFile, get_col_symbols_path};
use std::{
	fs::{File, OpenOptions},
	io::{BufRead, BufReader, Write},
	path::PathBuf
};
use std::collections::HashMap;

pub fn read_meta(meta_path: &PathBuf) -> std::io::Result<Table> {
	let f = File::open(meta_path)?;
	let reader = BufReader::new(f);

	let mut res: Table = serde_json::from_reader(reader)?;

	for c in res.schema.columns.iter_mut() {
		c.symbol_file = match c.r#type {
			ColumnType::Symbol => {
				let path = get_col_symbols_path(&res.schema.name, &c);
				let file = OpenOptions::new()
					.read(true)
					.write(true)
					.create(true)
					.open(&path)?;

				let mut csf = ColumnSymbolFile {
					file,
					symbols: vec![],
					symbol_map: HashMap::new()
				};

				Some(csf)
			},
			_ => None
		};
	}

	Ok(res)
}

impl Table {
	pub fn write_meta(&mut self) -> std::io::Result<()> {
		let mut f = OpenOptions::new()
			.write(true)
			.create(true)
			.open(&self.meta_path)
			.unwrap_or_else(|_| panic!("Could not open meta file {:?}", &self.meta_path));

		self.partitions.sort_unstable_by_key(|p| p.ts_bounds.min);

		serde_json::to_writer_pretty(&f, &self)
			.unwrap_or_else(|_| panic!("Could not write to meta file {:?}", &self.meta_path));
		f.flush()
			.unwrap_or_else(|_| panic!("Could not flush to meta file {:?}", &self.meta_path));
		Ok(())
	}

	pub fn get_first_ts(&self) -> Option<i64> {
		match self.partitions.first() {
			None => None,
			Some(p) => Some(p.ts_range.min)
		}
	}

	pub fn get_last_ts(&self) -> Option<i64> {
		match self.partitions.last() {
			None => None,
			Some(p) => Some(p.ts_range.max)
		}
	}
}
