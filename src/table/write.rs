use crate::{
	table::*,
	util::time::{partition_max_ts, partition_min_ts, ToNaiveDateTime}
};
use chrono::NaiveDateTime;
use std::fs::create_dir_all;

impl Table {
	fn get_partition_bounds(&self, date: NaiveDateTime) -> MinMax<i64> {
		if self.schema.partition_fmt.is_empty() {
			return MinMax {
				min: i64::MIN,
				max: i64::MAX
			};
		}

		MinMax {
			min: partition_min_ts(&self.schema.partition_fmt, date),
			max: partition_max_ts(&self.schema.partition_fmt, date)
		}
	}

	fn get_partition(&mut self, ts: i64) -> usize {
		let dir = get_partition_dir(&self.schema, ts);
		match self.partitions.iter().position(|p| p.dir == dir) {
			Some(i) => {
				let prev_ts = self.partitions[i].ts_range.max;
				if ts < prev_ts {
					panic!("Timestamp {} is out of order (previous {})", ts, prev_ts);
				}
				i
			}
			None => {
				create_dir_all(&dir).unwrap_or_else(|_| panic!("Cannot create dir {:?}", &dir));
				let date = ts.to_naive_date_time();
				self.partitions.push(Partition {
					dir,
					ts_bounds: self.get_partition_bounds(date),
					ts_range: MinMax { min: ts, max: ts },
					row_count: 0
				});
				self.partitions.len() - 1
			}
		}
	}

	pub fn put(&mut self, ts: i64) {
		if self.partitions.len() == 0
			|| self.schema.columns[0].file.is_none()
			|| ts > self.partitions[self.partition_index].ts_bounds.max
			|| ts < self.partitions[self.partition_index].ts_bounds.min
		{
			self.partition_index = self.get_partition(ts);
			self.open_columns();
		}

		let p = &mut self.partitions[self.partition_index];

		let f = self.schema.columns[self.column_index]
			.file
			.as_mut()
			.expect("column open");
		if f.data.len() <= p.row_count * (self.schema.columns[0].stride) {
			for c in &mut self.schema.columns {
				let file = c.file.as_mut().expect("column open");
				let size = file.data.len();
				drop(&file.data);
				// println!("{} grow {} -> {}", c.name, size, size * 2);
				file
					.file
					.set_len(size as u64 * 2)
					.unwrap_or_else(|e| panic!("Could not truncate {:?} to {}: {}", file, size * 2, e));
				unsafe {
					file.data = memmap::MmapOptions::new()
						.map_mut(&file.file)
						.unwrap_or_else(|_| panic!("Could not mmapp after grow {:?}", file));
				}
			}
		}
		p.ts_range.max = ts;
		self.put_i64(ts);
	}

	pub fn put_symbol(&mut self, val: &str) {
		let c = &mut self.schema.columns[self.column_index];
		if c.r#type != ColumnType::Symbol {
			panic!("cannot put_symbol {} on non-symbol column {:?}", val, c);
		}
		let symbols = &mut c.symbol_file.as_mut().expect("symbol_file open");
		symbols.add_sym(val.to_string(), true);
		self.column_index += 1;
	}

	fn put_bytes(&mut self, bytes: &[u8]) {
		let size = bytes.len();
		let p = &mut self.partitions[self.partition_index];
		let offset = p.row_count * size;
		// println!("put {} bytes {} {}", size, self.column_index, self.cur_partition_meta.row_count);
		let f = self.schema.columns[self.column_index]
			.file
			.as_mut()
			.expect("column open");
		f.data[offset..offset + size].copy_from_slice(bytes);
		self.column_index += 1;
		if self.column_index == self.schema.columns.len() {
			self.column_index = 0;
			p.row_count += 1;
		}
	}

	pub fn put_i8(&mut self, val: i8) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_u8(&mut self, val: u8) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_i16(&mut self, val: i16) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_u16(&mut self, val: u16) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_i32(&mut self, val: i32) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_u32(&mut self, val: u32) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_f32(&mut self, val: f32) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_i64(&mut self, val: i64) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_timestamp(&mut self, val: i64) { self.put_i64(val) }

	pub fn put_u64(&mut self, val: u64) { self.put_bytes(&val.to_le_bytes()) }

	pub fn put_f64(&mut self, val: f64) { self.put_bytes(&val.to_le_bytes()) }

	pub fn flush(&mut self) {
		for column in &mut self.schema.columns {
			let file = &mut column.file.as_mut().expect("column open");

			file
				.data
				.flush()
				.unwrap_or_else(|_| panic!("Could not flush {:?}", file));
			// Leave a spot for the next insert
			let p = &self.partitions[self.partition_index];
			let size = column.stride * p.row_count;
			file.file.set_len(size as u64).unwrap_or_else(|_| {
				panic!(
					"Could not truncate {:?} to {} to save {} bytes on disk",
					file.file,
					size,
					file.data.len() - size
				)
			});
		}
		self
			.write_meta()
			.expect("Could not write meta file with row_count");
	}
}
