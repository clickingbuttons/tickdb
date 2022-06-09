// https://docs.julialang.org/en/v1/manual/embedding/
use super::Lang;
use crate::server::query::Query;
use crate::c_str;
use tickdb::table::read::PartitionColumn;
use crate::server::langs::julia_bindings::*;
use std::{
  ffi::{c_void, CStr, CString},
  io::{Error, ErrorKind},
  slice::from_raw_parts
};

macro_rules! check_julia_error {
  () => {
    // https://github.com/JuliaLang/julia/blob/f6b51abb294998571ff88a95b50a15ce062a2994/test/embedding/embedding.c
    if !jl_exception_occurred().is_null() {
      // https://discourse.julialang.org/t/julia-exceptions-in-c/18387
      let err = jl_unbox_voidpointer(jl_eval_string(c_str!("pointer(sprint(showerror, ccall(:jl_exception_occurred, Any, ())))")));
      let err = CStr::from_ptr(err as *const i8).to_str().unwrap();
      return Err(Error::new(ErrorKind::Other, err));
    }
  }
}

unsafe fn get_julia_1d_array(
  partition_col: &PartitionColumn,
  arg_type: &*mut jl_datatype_t
) -> *mut jl_value_t {
  let ptr = partition_col.get_u8().as_ptr() as *mut c_void;

  return jl_ptr_to_array_1d(
    *arg_type as *mut jl_value_t,
    ptr,
    partition_col.row_count,
    0 // Let julia deal with freeing it
  ) as *mut jl_value_t;
}

pub struct Julia {
	scan_fn: *mut jl_value_t,
	scan_ans: *mut jl_value_t,
	arg_types: Vec<*mut jl_datatype_t>,
}

impl Julia {
	pub fn init() {
		unsafe {
			jl_init__threading();
		}
	}

	pub fn new(query: &Query) ->  std::io::Result<Self>{
		let jl_string = CString::new(format!("module Scan {}\nend", query.source.text)).unwrap();
		unsafe {
			jl_eval_string(jl_string.as_ptr());
			check_julia_error!();
			Ok( Self{
				scan_fn: jl_eval_string(c_str!("Scan.scan")),
				scan_ans: jl_nothing,
				arg_types: Vec::new()
			})
		}
	}
}

impl Lang for Julia {
	fn get_cols(&mut self) -> std::io::Result<Vec<String>> {
		unsafe {
			let n_args = jl_eval_string(c_str!("typeof(Scan.scan).name.mt.defs.func.nargs"));
			let n_args = (jl_unbox_int32(n_args) - 1) as usize;
			let arg_names = jl_eval_string(c_str!("typeof(Scan.scan).name.mt.defs.func.slot_syms"));
			let arg_names = from_raw_parts(jl_string_data(arg_names), jl_string_len(arg_names) - 1);
			let arg_names = String::from_utf8(arg_names.to_vec()).unwrap();
			let arg_names = arg_names
				.split('\0')
				.skip(1)
				.filter(|n| !n.starts_with('#'))
				.take(n_args)
				.map(|s| s.to_string())
				.collect::<Vec<String>>();
			let arg_types =
				jl_eval_string(c_str!("typeof(Scan.scan).name.mt.defs.sig.types")) as *mut jl_svec_t;
			let arg_types = from_raw_parts(
				jl_svec_data(arg_types).add(1) as *mut *mut jl_datatype_t,
				(*arg_types).length - 1
			);
			let arg_types = Vec::from(arg_types);
			self.arg_types = arg_types;
			// TODO: typecheck
			Ok(arg_names)
		}
	}

	fn scan_partition(&mut self, p: Vec<PartitionColumn>) -> std::io::Result<()> {
		unsafe {
			let mut args: Vec<*mut jl_value_t> = Vec::new();
			for (partition_col, arg_type) in p.iter().zip(self.arg_types.iter()) {
				args.push(get_julia_1d_array(partition_col, arg_type));
			}
			self.scan_ans = jl_call(self.scan_fn, args.as_mut_ptr(), args.len() as i32);
			check_julia_error!();
		}

		Ok(())
	}

	fn serialize(&mut self) -> std::io::Result<Vec<u8>> {
		unsafe {
			let func = jl_get_function(jl_main_module, "string");
			let ans = jl_call1(func, self.scan_ans);
			let ans = *(jl_get_field(ans, c_str!("data")) as *const jl_array_t);
			let ans = CStr::from_ptr(ans.data as *const i8).to_str().unwrap();
			Ok(Vec::from(ans))
		}
	}
}
