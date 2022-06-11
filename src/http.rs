mod http;

// I really don't want to write this but there don't seem to be any good
// forking HTTP servers in the Rust ecosystem. We need the fork for:
// 1. seccomp
// 2. julia thread local storage not crashing
// 3. monitoring CPU and memory
// 4. potentially supporting query multithreading WHILE being able to monitor it

// List of ones I've ruled out:
// 1. tokio/hyper and deratives: async is a mess and takes forever to build
// 2. h2: doesn't support HTTP 1
// 3. http-rs: uses async instead of processes and too many deps
// 4. tiny-http: closest, but uses thread pool instead of process pool

use std::str::from_utf8;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use std::io::{Read, Write};
use ::http::Response;

pub fn stringify_header(response: &Response<Vec<u8>>) -> String {
	let mut header = format!(
		"{:?} {} {}\r\nContent-Length: {}\r\n",
		response.version(),
		response.status().as_str(),
		response.status().canonical_reason().unwrap_or(""),
		response.body().len()
	);
	for (key, val) in response.headers() {
		header.push_str(&format!("{}: {}\r\n", key, val.to_str().unwrap()));
	}
	header.push_str("\r\n");
	header
}

struct Req<'a> {
	buffer: [u8; BUFFER_SIZE],
	pub request: Request<'a, 'a>,
}

impl<'a> Req<'a> {
	pub fn new(headers: &'a mut [httparse::Header<'a>; 16]) -> Self {
		let request = Request::new(headers);
		let buffer = [0; BUFFER_SIZE];
		Self {
			buffer, 
			request,
		}
	}

	pub fn parse(&'a mut self, stream: &mut TcpStream) -> std::io::Result<&'a [u8]> {
		let mut total_size: usize = 0;
		let mut body_offset: usize = 0;
		loop {
			total_size += stream.read(&mut self.buffer)?;
			match self.request.parse(&self.buffer) {
				Ok(status) => match status {
					httparse::Status::Complete(o) => {
						body_offset = o;
						break;
					},
					httparse::Status::Partial => {}
				},
				Err(e) => {
					return Err(Error::new(ErrorKind::Other, e.to_string()));
				}
			};
		}
		let expected_size = match self.request.headers.iter().find(|h| h.name.to_lowercase() == "content-length") {
			Some(h) => match from_utf8(h.value) {
				Ok(s) => match s.parse::<usize>() {
					Ok(v) => v,
					Err(e) => {
						return Err(Error::new(ErrorKind::Other, e.to_string()));
					}
				},
				Err(e) => {
					return Err(Error::new(ErrorKind::Other, e.to_string()));
				}
			},
			None => 0
		};

		let mut body_size: usize = total_size - body_offset;
		while body_size < expected_size {
			body_size += stream.read(&mut self.buffer)?;
		}

		return Ok(&self.buffer[body_offset..body_size]);
	}
}


