use crate::table::Table;
use std::{
  fs::{File, OpenOptions},
  io::{BufReader, Write},
  path::PathBuf
};

pub fn read_meta(meta_path: &PathBuf) -> std::io::Result<Table> {
  let f = File::open(meta_path)?;
  let reader = BufReader::new(f);

  let res = serde_json::from_reader(reader)?;
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
