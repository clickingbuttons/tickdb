use crate::table::{get_col_path, ColumnFile, Table};
use std::{cmp::max, fs::OpenOptions};

const DEFAULT_ROWS: usize = 1_000_000;

impl Table {
	pub fn open_column(&self, i: usize) -> ColumnFile {
		let c = &self.schema.columns[i];
		let p = &self.partitions[self.partition_index];
		let path = get_col_path(p, c);
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&path)
			.unwrap_or_else(|_| panic!("Unable to open file {:?}", path));

		let init_size = max(p.row_count, DEFAULT_ROWS) * c.stride;
		file.set_len(init_size as u64).unwrap_or_else(|e| {
			panic!("Could not truncate {:?} to {}: {}", path, init_size, e)
		});
		unsafe {
			let data = memmap::MmapOptions::new()
				.map_mut(&file)
				.unwrap_or_else(|e| panic!("Could not mmap {:?}: {}", path, e));

			ColumnFile { file, data }
		}
	}

	pub fn open_columns(&mut self) {
		for i in 0..self.schema.columns.len() {
			self.schema.columns[i].file = Some(self.open_column(i));
		}
	}
}
