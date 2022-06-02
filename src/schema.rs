use serde::{Deserialize, Serialize};
use std::{cmp::PartialEq, collections::HashMap, fmt, fs::File, io::Write};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum ColumnType {
	Timestamp,
	Symbol,
	I8,
	U8,
	I16,
	U16,
	I32,
	U32,
	F32,
	I64,
	U64,
	F64
}

#[derive(Debug)]
pub struct ColumnFile {
	pub file: File,
	pub data: memmap::MmapMut
}

#[derive(Debug)]
pub struct ColumnSymbolFile {
	pub file:       Option<File>,
	pub symbols:    Vec<String>,
	pub symbol_map: HashMap<String, usize>
}

impl ColumnSymbolFile {
	pub fn add_sym(&mut self, sym: String, write: bool) -> usize {
		*self.symbol_map.entry(sym.clone()).or_insert_with(|| {
			if write {
				match self.file.as_mut() {
					None => panic!("write called but no open file"),
					Some(f) => {
						if self.symbols.len() != 0 {
							f.write_all(b"\n").expect("write_all (1)");
						}
						f.write_all(sym.as_bytes()).expect("write_all (2)");
					}
				}
			}
			self.symbols.push(sym);
			self.symbols.len()
		})
	}
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
	pub name:        String,
	pub r#type:      ColumnType,
	pub stride:      usize,
	// Used in Table
	#[serde(skip)]
	pub file:        Option<ColumnFile>,
	#[serde(skip)]
	pub symbol_file: Option<ColumnSymbolFile>
}

impl Column {
	pub fn new(name: &str, r#type: ColumnType) -> Column {
		Column {
			name: name.to_owned(),
			r#type,
			stride: match r#type {
				ColumnType::I8 | ColumnType::U8 => 1,
				ColumnType::I16 | ColumnType::U16 => 2,
				ColumnType::I32 | ColumnType::U32 => 4,
				ColumnType::F32 => 4,
				ColumnType::Timestamp
				| ColumnType::Symbol
				| ColumnType::I64
				| ColumnType::U64
				| ColumnType::F64 => 8
			},
			file: None,
			symbol_file: None
		}
	}
}

#[derive(Serialize, Deserialize)]
pub struct Schema {
	#[serde(skip, default)] // Filesystem path is source of truth
	pub name: String,
	pub columns: Vec<Column>,
	// Specifiers: https://docs.rs/chrono/0.3.1/chrono/format/strftime/index.html
	pub partition_fmt: String
}

impl fmt::Debug for Schema {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"Schema {} {}:\n  {}",
			self.name,
			self.partition_fmt,
			self
				.columns
				.iter()
				.map(|c| format!("({}, {:?})", c.name, c.r#type))
				.collect::<Vec<_>>()
				.join("\n  ")
		)
	}
}

impl<'a> Schema {
	pub fn new(name: &'a str, partition_fmt: &'a str) -> Self {
		Self {
			name: name.to_owned(),
			columns: vec![Column::new("ts", ColumnType::Timestamp)],
			partition_fmt: partition_fmt.to_owned()
		}
	}

	pub fn add_col(mut self, column: Column) -> Self {
		self.columns.push(column);
		self
	}

	pub fn add_cols(mut self, columns: Vec<Column>) -> Self {
		self.columns.extend(columns);
		self
	}
}
