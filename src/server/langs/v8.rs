// https://v8.dev/docs/embed
use super::{Lang, SCAN_FN_NAME};
use crate::server::query::Query;
use std::{
	ffi::c_void,
	io::{Error, ErrorKind}
};
use tickdb::{
	schema::{ColumnType, Schema},
	table::{read::PartitionColumn, Table}
};

const RUNTIME_FN_NAME: &str = "tickdb_get_params";
unsafe extern "C" fn backing_store_deleter(
	_data: *mut c_void,
	_byte_length: usize,
	_deleter_data: *mut c_void
) {
}

macro_rules! arr_view {
	($scope: expr, $buffer: expr, $len: expr, $_type: ty, $node_type: ident) => {
		v8::$node_type::new($scope, $buffer, 0, $len / std::mem::size_of::<$_type>())
			.unwrap()
			.into()
	};
}

fn fmt_error_line<'s>(
	scope: &mut v8::HandleScope,
	msg: v8::Local<v8::Message>,
	line: v8::Local<'s, v8::String>
) -> String {
	let mut res = String::new();
	let line = line.to_rust_string_lossy(scope);
	res.push_str(&line);
	let start_col = msg.get_start_column();
	res.push('\n');
	for i in 0..start_col {
		if line.as_bytes()[i] as char == '\t' {
			res.push('\t');
		} else {
			res.push(' ');
		}
	}
	let end_col = msg.get_end_column();
	for _ in start_col..end_col {
		res.push('^');
	}
	res
}

fn fmt_stack_trace<'s>(
	scope: &mut v8::HandleScope,
	stack_trace: v8::Local<v8::StackTrace>,
	fname: &str
) -> String {
	let mut res = String::new();
	res.push('\n');
	for i in 0..stack_trace.get_frame_count() {
		let frame = stack_trace.get_frame(scope, i).unwrap();
		res.push_str("\tat ");
		let fn_name = match frame.get_function_name(scope) {
			Some(n) => n.to_rust_string_lossy(scope),
			None => String::from("Object.<anonymous>")
		};
		res.push_str(&fn_name);
		res.push_str(" (");
		let fname = match frame.get_script_name(scope) {
			Some(name) => name.to_rust_string_lossy(scope),
			None => String::from(fname)
		};
		res.push_str(&format!(
			"{}:{}:{}",
			fname,
			frame.get_line_number(),
			frame.get_column()
		));
		res.push_str(")\n");
	}

	res
}

// query.js:12
// asdfasdfasdf
// ^
//
// ReferenceError: asdfasdfasdf is not defined
//     at Object.<anonymous> (/home/thesm/src/tickdb/tests/query.js:12:1)
//
// V8 v10.3.174.6
fn fmt_error(scope: &mut v8::HandleScope, msg: v8::Local<v8::Message>, fname: &str) -> String {
	let mut res = String::from(fname);
	let lineno = msg.get_line_number(scope);
	if lineno.is_some() {
		res.push_str(&format!(":{}", lineno.unwrap()));
	}
	res.push('\n');

	let line = msg.get_source_line(scope);
	if line.is_some() {
		res.push_str(&fmt_error_line(scope, msg, line.unwrap()));
	}
	res.push('\n');
	res.push('\n');
	res.push_str(&msg.get(scope).to_rust_string_lossy(scope));

	let stack_trace = msg.get_stack_trace(scope);
	if stack_trace.is_some() && stack_trace.unwrap().get_frame_count() > 0 {
		res.push_str(&fmt_stack_trace(scope, stack_trace.unwrap(), fname));
	}

	res.push_str(&format!("\n\nV8 v{}\n", v8::V8::get_version()));
	res
}

fn eval<'s>(scope: &mut v8::HandleScope<'s>, code: &str, file: &str) -> std::io::Result<()> {
	let scope = &mut v8::EscapableHandleScope::new(scope);
	let source = v8::String::new(scope, code).unwrap();
	let scope = &mut v8::TryCatch::new(scope);
	let script = v8::Script::compile(scope, source, None);
	if script.is_none() {
		let msg = scope.message().unwrap();
		let msg = fmt_error(scope, msg, file);
		return Err(Error::new(ErrorKind::Other, msg));
	}
	let script = script.unwrap();
	let script = script.run(scope);
	if script.is_none() {
		let msg = scope.message().unwrap();
		let msg = fmt_error(scope, msg, file);
		return Err(Error::new(ErrorKind::Other, msg));
	}
	script.map(|v| scope.escape(v));
	Ok(())
}

#[derive(Debug)]
pub struct V8 {}

fn get_fn<'s>(
	name: &str,
	scope: &mut v8::HandleScope<'s>,
	global: v8::Local<'s, v8::Object>
) -> std::io::Result<v8::Local<'s, v8::Function>> {
	let key = v8::String::new(scope, name).unwrap();
	let v8_fn = global.get(scope, key.into()).unwrap();
	let v8_fn = v8::Local::<v8::Function>::try_from(v8_fn).map_err(|e| {
		let msg = format!("{} must be a function: {}", SCAN_FN_NAME, e);
		Error::new(ErrorKind::Other, msg)
	})?;

	Ok(v8_fn)
}

fn get_cols<'s>(
	scan_fn: v8::Local<'s, v8::Function>,
	schema: &Schema,
	scope: &mut v8::HandleScope<'s>,
	global: v8::Local<'s, v8::Object>
) -> std::io::Result<Vec<String>> {
	let runtime_fn = get_fn(RUNTIME_FN_NAME, scope, global)?;

	let args = runtime_fn
		.call(scope, global.into(), &[scan_fn.into()])
		.expect("runtime_fn call");
	let args = v8::Local::<v8::Array>::try_from(args).expect("args");
	let mut cols = Vec::<String>::with_capacity(args.length() as usize);
	for i in 0..args.length() {
		let val = args.get_index(scope, i).unwrap();
		let val = val.to_string(scope).unwrap();
		let val = val.to_rust_string_lossy(scope);
		if !schema.columns.iter().any(|c| c.name == val) {
			let msg = format!("column {} doesn't exist on table {}", val, schema.name);
			return Err(Error::new(ErrorKind::Other, msg));
		}
		cols.push(val);
	}
	Ok(cols)
}

fn get_buffer<'s>(
	c: &PartitionColumn,
	scope: &mut v8::HandleScope<'s>
) -> v8::Local<'s, v8::Value> {
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
		ColumnType::F32 => arr_view!(scope, buffer, buffer_len, f32, Float32Array),
		ColumnType::F64 => arr_view!(scope, buffer, buffer_len, f64, Float64Array),
		ColumnType::I8 => arr_view!(scope, buffer, buffer_len, i8, Int8Array),
		ColumnType::I16 => arr_view!(scope, buffer, buffer_len, i16, Int16Array),
		ColumnType::I32 => arr_view!(scope, buffer, buffer_len, i32, Int32Array),
		ColumnType::I64 | ColumnType::Timestamp => {
			arr_view!(scope, buffer, buffer_len, i64, BigInt64Array)
		}
		ColumnType::U8 => arr_view!(scope, buffer, buffer_len, u8, Uint8Array),
		ColumnType::U16 => arr_view!(scope, buffer, buffer_len, u16, Uint16Array),
		ColumnType::U32 => arr_view!(scope, buffer, buffer_len, u32, Uint32Array),
		ColumnType::U64 => arr_view!(scope, buffer, buffer_len, u64, BigUint64Array),
		ColumnType::Symbol | ColumnType::SymbolPool => todo!("impl")
	};

	v8::Local::<v8::Value>::try_from(buffer).unwrap()
}

impl V8 {
	pub fn scan(query: &Query) -> std::io::Result<Vec<u8>> {
		let table = Table::open(&query.table)?;
		let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
		isolate.set_capture_stack_trace_for_uncaught_exceptions(true, 32);

		let handle_scope = &mut v8::HandleScope::new(isolate);
		let context = v8::Context::new(handle_scope);
		let scope = &mut v8::ContextScope::new(handle_scope, context);

		eval(scope, include_str!("./runtime.js"), "runtime.js")?;
		eval(scope, &query.query, "query.js")?;

		let global = context.global(scope);
		let scan_fn = get_fn(SCAN_FN_NAME, scope, global)?;
		let cols = get_cols(scan_fn, &table.schema, scope, global)?;
		let partitions = table.partition_iter(query.from, query.to, cols);

		let mut ans: v8::Local<v8::Value> = v8::undefined(scope).into();
		let scope = &mut v8::TryCatch::new(scope);
		for p in partitions {
			let args = p.iter().map(|c| get_buffer(c, scope)).collect::<Vec<_>>();

			let res = scan_fn.call(scope, global.into(), args.as_slice());
			if res.is_none() {
				let msg = scope.message().unwrap();
				let msg = fmt_error(scope, msg, "query.js");
				return Err(Error::new(ErrorKind::Other, msg));
			}
			ans = res.unwrap();
		}
		let ans = ans.to_string(scope).unwrap().to_rust_string_lossy(scope);
		Ok(Vec::from(ans))
	}
}

impl Lang for V8 {
	fn init() {
		let platform = v8::new_default_platform(0, false).make_shared();
		v8::V8::initialize_platform(platform);
		v8::V8::initialize();
	}

	fn deinit() {
		unsafe {
			v8::V8::dispose();
			v8::V8::dispose_platform();
		}
	}
}
