use serde::{Deserialize, Serialize};
use std::{cmp::PartialEq, fmt, fs::File};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum ColumnType {
  Timestamp,
  Symbol,
  SymbolPool,
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
  pub name:   String,
  pub r#type: ColumnType,
  pub stride: usize,
  #[serde(skip)]
  pub file:   Option<ColumnFile>
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
        | ColumnType::SymbolPool
        | ColumnType::I64
        | ColumnType::U64
        | ColumnType::F64 => 8
      },
      file: None
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
