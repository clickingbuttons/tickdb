use crate::table::{get_col_symbols_path, ColumnSymbolFile, ColumnType, Table};
use std::{
	collections::HashMap,
	fs::{File, OpenOptions},
	io::{BufRead, BufReader, Write},
	path::PathBuf
};

pub fn read_meta(meta_path: &PathBuf) -> std::io::Result<Table> {
	let f = File::open(meta_path)?;
	let reader = BufReader::new(f);

	let mut res: Table = serde_json::from_reader(reader)?;

	for c in res.schema.columns.iter_mut() {
		if c.r#type != ColumnType::Symbol {
			continue;
		}
		let path = get_col_symbols_path(&res.schema.name, &c);
		let file = OpenOptions::new().write(true).create(true).open(&path)?;

		let mut csf = ColumnSymbolFile {
			file:       None,
			symbols:    vec![],
			symbol_map: HashMap::new()
		};

		let reader = BufReader::new(&file);

		for line in reader.lines() {
			let line = line.expect("a line");
			csf.add_sym(line, false);
		}

		csf.file = Some(file);
		c.symbol_file = Some(csf);
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

		serde_json::to_writer_pretty(&f, &self).expect(&format!(
			"Could not write to meta file {:?}",
			&self.meta_path
		));
		f.flush().expect(&format!(
			"Could not flush to meta file {:?}",
			&self.meta_path
		));

		for c in self.schema.columns.iter_mut() {
			if c.r#type != ColumnType::Symbol {
				continue;
			}
			let path = get_col_symbols_path(&self.schema.name, &c);
			let file = OpenOptions::new().write(true).create(true).open(&path)?;

			c.symbol_file = Some(ColumnSymbolFile {
				file:       Some(file),
				symbols:    vec![],
				symbol_map: HashMap::new()
			});
		}

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
