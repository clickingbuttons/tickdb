use chrono::{DateTime, NaiveDate};
use httparse::Request;
use serde::{de, Deserialize};
use std::{
  ffi::c_void,
  io::{Error, ErrorKind}
};
use tickdb::{schema::ColumnType, table::Table};

#[derive(Deserialize)]
pub struct Query {
  pub table: String,
  #[serde(deserialize_with = "string_to_datetime")]
  pub from:  i64,
  #[serde(deserialize_with = "string_to_datetime")]
  pub to:    i64,
  pub query: String
}

use v8::inspector::*;

struct Client {
  base: V8InspectorClientBase
}

impl Client {
  fn new() -> Self {
    Self {
      base: V8InspectorClientBase::new::<Self>()
    }
  }
}

impl V8InspectorClientImpl for Client {
  fn base(&self) -> &V8InspectorClientBase { &self.base }

  fn base_mut(&mut self) -> &mut V8InspectorClientBase { &mut self.base }

  fn console_api_message(
    &mut self,
    _context_group_id: i32,
    _level: i32,
    message: &StringView,
    _url: &StringView,
    _line_number: u32,
    _column_number: u32,
    _stack_trace: &mut V8StackTrace
  ) {
    println!("{}", message.to_string());
  }
}

const SCAN_FN_NAME: &str = "scan";
const RUNTIME_FN_NAME: &str = "tickdb_get_params";
unsafe extern "C" fn backing_store_deleter(
  _data: *mut c_void,
  _byte_length: usize,
  _deleter_data: *mut c_void
) {
  println!("backing_store_deleter");
}

// ColumnType::F64 => thing!(scope, buffer, buffer_len, f64, Float64Array),
macro_rules! toArrayBufferView {
  ($scope: expr, $buffer: expr, $len: expr, $_type: ty, $node_type: ident) => {
    v8::$node_type::new($scope, $buffer, 0, $len / std::mem::size_of::<$_type>())
      .unwrap()
      .into()
  };
}

// https://v8.dev/docs/embed
pub fn run_query<'a>(query: &Query) -> std::io::Result<Vec<u8>> {
  let table = Table::open(&query.table)?;
  let isolate = &mut v8::Isolate::new(v8::CreateParams::default());

  let mut client = Client::new();
  let mut inspector = V8Inspector::create(isolate, &mut client);

  let handle_scope = &mut v8::HandleScope::new(isolate);
  let context = v8::Context::new(handle_scope);
  let scope = &mut v8::ContextScope::new(handle_scope, context);
  let runtime = v8::String::new(scope, include_str!("./runtime.js")).unwrap();
  let script = v8::Script::compile(scope, runtime, None).unwrap();
  script.run(scope).expect("runs (1)");
  let code = v8::String::new(scope, &query.query).unwrap();
  let script = v8::Script::compile(scope, code, None).unwrap();
  script.run(scope).expect("runs (2)");

  // Add logging
  let name = b"";
  let name_view = StringView::from(&name[..]);
  inspector.context_created(context, 1, name_view);

  let global = context.global(scope);
  let scan_key = v8::String::new(scope, SCAN_FN_NAME).unwrap();
  let scan_fn = global.get(scope, scan_key.into()).unwrap();
  let scan_fn = v8::Local::<v8::Function>::try_from(scan_fn).map_err(|e| {
    let msg = format!("{} must be a function: {}", SCAN_FN_NAME, e);
    Error::new(ErrorKind::Other, msg)
  })?;

  let getargs_fn = v8::String::new(scope, RUNTIME_FN_NAME).unwrap();
  let runtime_fn = global.get(scope, getargs_fn.into()).unwrap();
  let runtime_fn = v8::Local::<v8::Function>::try_from(runtime_fn).map_err(|e| {
    let msg = format!("{} must be a function: {}", RUNTIME_FN_NAME, e);
    Error::new(ErrorKind::Other, msg)
  })?;
  let args = runtime_fn
    .call(scope, global.into(), &[scan_fn.into()])
    .expect("runtime_fn call");
  let args = v8::Local::<v8::Array>::try_from(args).expect("args");
  let mut cols = Vec::<String>::with_capacity(args.length() as usize);
  for i in 0..args.length() {
    let val = args.get_index(scope, i).unwrap();
    let val = val.to_string(scope).unwrap();
    let val = val.to_rust_string_lossy(scope);
    cols.push(val);
  }
  let cols = cols.iter().map(|c| c.as_ref()).collect::<Vec<_>>();
  println!("cols {:?}", cols);

  let undefined = v8::undefined(scope).into();
  let partitions = table.partition_iter(query.from, query.to, cols);
  let mut ans: v8::Local<v8::Value> = undefined;
  for p in partitions {
    println!("{:?}", p[0].partition);
    let args: Vec<v8::Local<v8::Value>> = p
      .iter()
      .map(|c| {
        let buffer = unsafe {
          v8::ArrayBuffer::new_backing_store_from_ptr(
            c.slice.as_ptr() as *mut c_void,
            c.slice.len(),
            backing_store_deleter,
            std::ptr::null_mut()
          )
          .make_shared()
        };
        let buffer = v8::ArrayBuffer::with_backing_store(scope, &buffer);
        let buffer_len = c.slice.len();
        let buffer: v8::Local<v8::ArrayBufferView> = match c.column.r#type {
          ColumnType::F32 => toArrayBufferView!(scope, buffer, buffer_len, f32, Float32Array),
          ColumnType::F64 => toArrayBufferView!(scope, buffer, buffer_len, f64, Float64Array),
          ColumnType::I8 => toArrayBufferView!(scope, buffer, buffer_len, i8, Int8Array),
          ColumnType::I16 => toArrayBufferView!(scope, buffer, buffer_len, i16, Int16Array),
          ColumnType::I32 => toArrayBufferView!(scope, buffer, buffer_len, i32, Int32Array),
          ColumnType::I64 | ColumnType::Timestamp => {
            toArrayBufferView!(scope, buffer, buffer_len, i64, BigInt64Array)
          }
          ColumnType::U8 => toArrayBufferView!(scope, buffer, buffer_len, u8, Uint8Array),
          ColumnType::U16 => toArrayBufferView!(scope, buffer, buffer_len, u16, Uint16Array),
          ColumnType::U32 => toArrayBufferView!(scope, buffer, buffer_len, u32, Uint32Array),
          ColumnType::U64 => toArrayBufferView!(scope, buffer, buffer_len, u64, BigUint64Array),
          ColumnType::Symbol | ColumnType::SymbolPool => todo!("impl")
        };

        v8::Local::<v8::Value>::try_from(buffer).unwrap()
      })
      .collect::<Vec<_>>();

    println!("call");
    ans = scan_fn.call(scope, global.into(), args.as_slice()).unwrap();
  }
  println!("done");
  let ans = ans.to_string(scope).unwrap();
  println!("don2");
  let ans = ans.to_rust_string_lossy(scope);
  println!("don3");
  let ans = Vec::from(ans).clone();
  println!("don4");
  Ok(ans)
}

pub fn handle_query<'a>(_req: &Request, query: &[u8]) -> Vec<u8> {
  match serde_json::from_slice::<Query>(query) {
    Err(err) => {
      let q = std::str::from_utf8(query).unwrap();
      eprintln!("{} parsing query {}", err, q);
      Vec::from(format!("error parsing query {}\n", err))
    }
    Ok(mut query) => match run_query(&mut query) {
      Ok(value) => {
        println!("run_query done");
        value
      }
      Err(err) => Vec::from(format!("error running query: {}\n", err))
    }
  }
}

static NICE_FORMAT: &str = "%Y-%m-%d";
pub fn string_to_nanoseconds(value: &str) -> std::io::Result<i64> {
  // Nanoseconds since epoch?
  if value.len() > 4 {
    let nanoseconds = value.parse::<i64>();
    if let Ok(nanoseconds) = nanoseconds {
      return Ok(nanoseconds);
    }
  }
  // TODO: check date is in valid range before calling timestamp_nanos
  match DateTime::parse_from_rfc3339(&value) {
    Ok(date) => Ok(date.timestamp_nanos()),
    Err(_e) => match NaiveDate::parse_from_str(&value, &NICE_FORMAT) {
      Ok(date) => Ok(date.and_hms(0, 0, 0).timestamp_nanos()),
      Err(_e) => {
        let msg = format!(
          "Could not parse {} in RFC3339 or {} format",
          &value, &NICE_FORMAT
        );
        Err(Error::new(ErrorKind::Other, msg))
      }
    }
  }
}

fn string_to_datetime<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
  D: de::Deserializer<'de>
{
  struct StringVisitor;

  impl<'de> de::Visitor<'de> for StringVisitor {
    type Value = i64;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
      formatter.write_str("a rfc 3339 string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
      E: de::Error
    {
      match string_to_nanoseconds(value) {
        Ok(nanos) => Ok(nanos),
        Err(e) => Err(E::custom(e))
      }
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
      E: de::Error
    {
      Ok(value as i64)
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
      E: de::Error
    {
      Ok(value)
    }
  }
  deserializer.deserialize_any(StringVisitor)
}
