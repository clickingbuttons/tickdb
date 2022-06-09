// https://v8.dev/docs/embed
use super::{Lang, SCAN_FN_NAME};
use crate::server::query::Query;
use std::{
	ffi::c_void,
	io::{Error, ErrorKind}
};
use tickdb::{schema::ColumnType, table::read::PartitionColumn};

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

fn get_buffer<'s>(
	c: &PartitionColumn,
	scope: &mut v8::HandleScope<'s>
) -> v8::Local<'s, v8::Value> {
	if c.column.r#type == ColumnType::Symbol {
		let buffer = v8::Array::new(scope, c.row_count as i32);
		let csf = c.column.symbol_file.as_ref().expect("symbol_file open");
		for (i, sym_index) in c.get_u64().iter().enumerate() {
			let sym = &csf.symbols[*sym_index as usize - 1];
			let sym =
				v8::String::new_from_one_byte(scope, sym.as_bytes(), v8::NewStringType::Normal).unwrap();
			buffer.set_index(scope, i as u32, sym.into());
		}

		return v8::Local::<v8::Value>::try_from(buffer).unwrap();
	}
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
		ColumnType::Symbol => panic!("symbols have no array view")
	};

	v8::Local::<v8::Value>::try_from(buffer).unwrap()
}

pub struct V8<'s, 'i> {
	scope:    v8::ContextScope<'i, v8::HandleScope<'s, v8::Context>>,
	// scope:   v8::TryCatch<'i, v8::HandleScope<'s, v8::Context>>,
	global:   v8::Local<'s, v8::Object>,
	scan_fn:  v8::Local<'s, v8::Function>,
	scan_ans: v8::Local<'s, v8::Value>
}

impl<'s, 'i> V8<'s, 'i> {
	pub fn new(
		query: &Query,
		handle_scope: &'i mut v8::HandleScope<'s, ()>
	) -> std::io::Result<Self> {
		let context = v8::Context::new(handle_scope);
		let mut scope = v8::ContextScope::new(handle_scope, context);

		eval(&mut scope, include_str!("./runtime.js"), "runtime.js")?;
		eval(&mut scope, &query.source.text, &query.source.path)?;

		let global = context.global(&mut scope);
		let scan_fn = get_fn(SCAN_FN_NAME, &mut scope, global)?;

		let scan_ans: v8::Local<v8::Value> = v8::undefined(&mut scope).into();

		Ok(Self {
			scope,
			global,
			scan_fn,
			scan_ans
		})
	}

	pub fn get_cols(&mut self) -> std::io::Result<Vec<String>> {
		let runtime_fn = get_fn(RUNTIME_FN_NAME, &mut self.scope, self.global)?;

		let args = runtime_fn
			.call(&mut self.scope, self.global.into(), &[self.scan_fn.into()])
			.expect("runtime_fn call");
		let args = v8::Local::<v8::Array>::try_from(args).expect("args");
		let mut cols = Vec::<String>::with_capacity(args.length() as usize);
		for i in 0..args.length() {
			let val = args.get_index(&mut self.scope, i).unwrap();
			let val = val.to_string(&mut self.scope).unwrap();
			let val = val.to_rust_string_lossy(&mut self.scope);
			cols.push(val);
		}
		Ok(cols)
	}

	pub fn scan_partition(&mut self, p: Vec<PartitionColumn>) -> std::io::Result<()> {
		let scope = &mut v8::TryCatch::new(&mut self.scope);
		let args = p.iter().map(|c| get_buffer(c, scope)).collect::<Vec<_>>();

		let res = self
			.scan_fn
			.call(scope, self.global.into(), args.as_slice());
		if res.is_none() {
			let msg = scope.message().unwrap();
			let msg = fmt_error(scope, msg, "query.js");
			return Err(Error::new(ErrorKind::Other, msg));
		}
		self.scan_ans = res.unwrap();

		Ok(())
	}

	pub fn serialize(&mut self) -> std::io::Result<Vec<u8>> {
		let scope = &mut self.scope;
		let ans = self
			.scan_ans
			.to_string(scope)
			.unwrap()
			.to_rust_string_lossy(scope);
		Ok(Vec::from(ans))
	}
}

impl<'s, 'i> Lang for V8<'s, 'i> {
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
