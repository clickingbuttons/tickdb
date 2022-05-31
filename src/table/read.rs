use crate::{
	schema::Column,
	table::{ColumnFile, Partition, Table}
};
use std::{fmt::Debug, slice::from_raw_parts_mut};

impl Table {
	fn get_union<'a>(
		&'a self,
		columns: impl IntoIterator<Item = impl AsRef<str>>
	) -> Vec<&'a Column> {
		columns
			.into_iter()
			.map(|col_name| {
				let index = self
					.schema
					.columns
					.iter()
					.position(|col| &col.name == col_name.as_ref())
					.unwrap_or_else(|| panic!("column {} does not exist on table {}", col_name.as_ref(), self.schema.name));
				&self.schema.columns[index]
			})
			.collect::<Vec<&'a Column>>()
	}

	// Inclusive of from and to
	pub fn partition_iter<'a>(
		&'a self,
		from_ts: i64,
		to_ts: i64,
		columns: impl IntoIterator<Item = impl AsRef<str>>
	) -> PartitionIterator<'a> {
		assert!(to_ts >= from_ts);
		let partitions = self
			.partitions
			.iter()
			.filter(|p| {
				// Start
				(from_ts >= p.ts_range.min && from_ts <= p.ts_range.max) ||
        // Middle
        (from_ts < p.ts_range.min && to_ts > p.ts_range.max) ||
        // End
        (to_ts >= p.ts_range.min && to_ts <= p.ts_range.max)
			})
			.collect::<Vec<_>>();
		// println!("partitions {:?}", partitions);
		PartitionIterator {
			table: self,
			columns: self.get_union(columns),
			column_files: Vec::new(),
			from_ts,
			to_ts,
			partitions,
			partition_index: 0
		}
	}
}

#[derive(Debug)]
pub struct PartitionColumn<'a> {
	pub column:    &'a Column,
	pub slice:     &'a [u8],
	pub partition: &'a Partition,
	pub row_count: usize
}

macro_rules! get_partition_slice {
	($slice: expr, $_type: ty) => {
		unsafe {
			from_raw_parts_mut(
				$slice.as_ptr() as *mut $_type,
				$slice.len() / std::mem::size_of::<$_type>()
			)
		}
	};
}

impl<'a> PartitionColumn<'a> {
	pub fn get_i8(&self) -> &mut [i8] { get_partition_slice!(self.slice, i8) }

	pub fn get_u8(&self) -> &mut [u8] { get_partition_slice!(self.slice, u8) }

	pub fn get_i16(&self) -> &mut [i16] { get_partition_slice!(self.slice, i16) }

	pub fn get_u16(&self) -> &mut [u16] { get_partition_slice!(self.slice, u16) }

	pub fn get_i32(&self) -> &mut [i32] { get_partition_slice!(self.slice, i32) }

	pub fn get_u32(&self) -> &mut [u32] { get_partition_slice!(self.slice, u32) }

	pub fn get_i64(&self) -> &mut [i64] { get_partition_slice!(self.slice, i64) }

	pub fn get_u64(&self) -> &mut [u64] { get_partition_slice!(self.slice, u64) }

	pub fn get_f32(&self) -> &mut [f32] { get_partition_slice!(self.slice, f32) }

	pub fn get_f64(&self) -> &mut [f64] { get_partition_slice!(self.slice, f64) }
}

#[derive(Debug)]
pub struct PartitionIterator<'a> {
	table: &'a Table,
	columns: Vec<&'a Column>,
	column_files: Vec<ColumnFile>,
	from_ts: i64,
	to_ts: i64,
	pub partitions: Vec<&'a Partition>,
	partition_index: usize
}

fn binary_search_seek(haystack: &[i64], needle: i64, seek_start: bool) -> Result<usize, usize> {
	let mut index = haystack.binary_search(&needle);
	if let Ok(ref mut i) = index {
		// Seek to beginning/end
		if seek_start {
			while *i > 0 && haystack[*i - 1] == needle {
				*i -= 1;
			}
		} else {
			while *i < haystack.len() - 1 && haystack[*i + 1] == needle {
				*i += 1;
			}
			// This is going to be used as an end index
			*i += 1;
		}
	}
	index
}

fn find_ts(ts_column: &ColumnFile, row_count: usize, needle: i64, seek_start: bool) -> usize {
	let data = unsafe { from_raw_parts_mut(ts_column.data.as_ptr() as *mut i64, row_count) };
	let search_results = binary_search_seek(data, needle, seek_start);
	match search_results {
		Ok(n) => n,
		Err(n) => n
	}
}

impl<'a> Iterator for PartitionIterator<'a> {
	type Item = Vec<PartitionColumn<'a>>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.partition_index == self.partitions.len() {
			return None;
		}
		let p = self.partitions.get(self.partition_index)?;
		let start_row = if self.partition_index == 0 {
			let ts_column = self.table.open_column(0);
			find_ts(&ts_column, p.row_count, self.from_ts, true)
		} else {
			0
		};
		let end_row = if self.partition_index == self.partitions.len() - 1 {
			let ts_column = self.table.open_column(0);
			find_ts(&ts_column, p.row_count, self.to_ts, false)
		} else {
			p.row_count
		};
		let row_count = end_row - start_row;
		// println!("from_ts {}, to_ts {}", self.from_ts, self.to_ts);
		// println!("start_row {}, end_row {}", start_row, end_row);
		let data_columns = self
			.columns
			.iter()
			.map(|column| {
				let table_column_index = self
					.table
					.schema
					.columns
					.iter()
					.position(|c| c.name == column.name)
					.expect(&column.name);
				let table_column = &self.table.schema.columns[table_column_index];
				let col_file = self.table.open_column(table_column_index);
				// println!("col_file {} {:?}", table_column_index, col_file);
				let slice = unsafe {
					from_raw_parts_mut(
						col_file.data.as_ptr().add(start_row * table_column.stride) as *mut u8,
						row_count * table_column.stride
					)
				};
				// println!("slice {:?}", slice);
				// keep ref alive
				self.column_files.push(col_file);

				PartitionColumn {
					slice,
					column: table_column,
					partition: p,
					row_count
				}
			})
			.collect::<Vec<_>>();

		self.partition_index += 1;
		return Some(data_columns);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_binary_search_seek() {
		let data = [1, 2, 2, 2, 2, 2, 3, 4, 5, 5, 5, 5, 5, 5, 6, 7, 8, 10];
		assert_eq!(binary_search_seek(&data, 2, true), Ok(1));
		assert_eq!(binary_search_seek(&data, 2, false), Ok(5 + 1));
		assert_eq!(binary_search_seek(&data, 5, true), Ok(8));
		assert_eq!(binary_search_seek(&data, 5, false), Ok(13 + 1));
		assert_eq!(binary_search_seek(&data, 9, false), Err(data.len() - 1));
		assert_eq!(binary_search_seek(&data, 10, false), Ok(data.len()));
		assert_eq!(binary_search_seek(&data, 21, false), Err(data.len()));

		let data = [1];
		assert_eq!(binary_search_seek(&data, 1, true), Ok(0));
		assert_eq!(binary_search_seek(&data, 1, false), Ok(1));

		let data = [];
		assert_eq!(binary_search_seek(&data, 1, true), Err(0));
		assert_eq!(binary_search_seek(&data, 1, false), Err(0));
	}
}
