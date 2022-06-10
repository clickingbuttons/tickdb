mod server;
use std::str::from_utf8;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use http::Response;
use httparse::Request;
use log::debug;
use nix::{
	sys::wait::waitpid,
	unistd::{fork, ForkResult, Pid}
};
use server::{
	langs::{v8::V8, julia::Julia},
	query::handle_query
};
use simple_logger::SimpleLogger;
use std::{
	collections::HashMap,
	env::var,
	fs::read_dir,
	io::{Read, Write},
	net::TcpListener
};
use tickdb::table::{paths::get_data_path, Table};

const PORT: &str = "8080";
const BUFFER_SIZE: usize = 1 << 16;

fn handle_request(
	req: &Request,
	body: &[u8],
	tables: &HashMap<String, Table>
) -> Response<Vec<u8>> {
	let builder = Response::builder();
	match req.method {
		None => builder
			.status(400)
			.body(Vec::from("expected request method\n"))
			.unwrap(),
		Some(m) => match m {
			"GET" => builder.body(Vec::from("tickdb here\n")).unwrap(),
			"POST" => handle_query(req, body, tables),
			_ => builder.status(404).body(Vec::from("not found\n")).unwrap()
		}
	}
}

fn get_http_header(response: &Response<Vec<u8>>) -> String {
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

fn open_tables() -> HashMap<String, Table> {
	let mut res = HashMap::new();
	let data_dir = get_data_path("");
	let paths = read_dir(data_dir).unwrap();

	for path in paths {
		let path = path.unwrap().path();
		let path = path.file_name().unwrap();
		let path = path.to_os_string().into_string().unwrap();
		match Table::open(&path) {
			Ok(table) => {
				res.insert(path, table);
			}
			Err(e) => eprintln!("error opening table {}: {}", path, e)
		};
	}

	res
}

struct Req<'a> {
	headers: [httparse::Header<'a>; 16],
	buffer: &'a mut [u8; BUFFER_SIZE],
	pub request: Request<'a, 'a>,
	pub body: &'a [u8]
}

impl<'a> Req<'a> {
	pub fn new() -> Self {
		let mut headers = [httparse::EMPTY_HEADER; 16];
		let request = Request::new(&mut headers);
		let buffer = [0; BUFFER_SIZE];
		Self {
			headers,
			buffer, 
			request,
			body: &buffer[..0]
		}
	}

	pub fn parse(&mut self, stream: &mut TcpStream) -> std::io::Result<()> {
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

		//self.body = &self.buffer[body_offset..body_size];
		return Ok(());
	}
}

fn main() {
	SimpleLogger::new().init().unwrap();
	debug!("start");

	let addr = format!("0.0.0.0:{}", PORT);

	let listener = TcpListener::bind(&addr).unwrap();

	let num_procs = var("TICKDB_NUM_PROCS")
		.unwrap_or("1".to_string())
		.parse::<i64>()
		.unwrap();

	debug!("opening tables");
	let tables = open_tables();

	for _ in 0..num_procs {
		match unsafe { fork() } {
			Ok(ForkResult::Child) => {
				println!("fork {}", std::process::id());

				V8::init();
				Julia::init();
				debug!("scripting init");

				debug!("listening on {}", addr);
				for stream in listener.incoming() {
					let stream = &mut stream.unwrap();
					let (request, body) = match parse_request(stream, headers, buffer) {
						Ok((r, b)) => (r, b),
						Err(e) => {
							let builder = Response::builder();
							let msg = format!("error parsing request: {}", e);
							let msg = Vec::from(msg.into_bytes());
							let response = builder.status(400).body(msg).unwrap();
							stream.write(get_http_header(&response).as_bytes()).unwrap();
							stream.write(response.body()).unwrap();
							continue;
						}
					};

					//println!("handle_request {}", from_utf8(&buffer).unwrap());
					let response = handle_request(&request, body, &tables);
					stream.write(get_http_header(&response).as_bytes()).unwrap();
					stream.write(response.body()).unwrap();
					debug!("wrote response");
				}
			}
			Ok(ForkResult::Parent { child: _ }) => {}
			Err(_) => println!("Fork failed")
		}
	}
	drop(listener);

	waitpid(Some(Pid::from_raw(-1)), None).unwrap();
	debug!("joined");
}
