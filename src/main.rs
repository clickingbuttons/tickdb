mod server;

use http::Response;
use httparse::Request;
use log::debug;
use nix::{
	sys::{signal, wait::waitpid},
	unistd::{fork, ForkResult, Pid}
};
use server::{
	langs::{v8::V8, Lang},
	query::handle_query
};
use simple_logger::SimpleLogger;
use std::{
	collections::HashMap,
	env::var,
	fs::read_dir,
	io::{Read, Write},
	net::TcpListener,
	process::exit
};
use tickdb::table::{paths::get_data_path, Table};

const PORT: &str = "8080";
const BUFFER_SIZE: usize = 1 << 16;

extern "C" fn handle_sigint(_: i32) { exit(0); }

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
			"POST" => builder.body(handle_query(req, body, tables)).unwrap(),
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

fn register_signal() {
	let sig_action = signal::SigAction::new(
		signal::SigHandler::Handler(handle_sigint),
		signal::SaFlags::SA_NODEFER,
		signal::SigSet::empty()
	);
	unsafe {
		signal::sigaction(signal::SIGINT, &sig_action).unwrap();
	}
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

fn main() {
	SimpleLogger::new().init().unwrap();
	debug!("start");

	let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).unwrap();

	let num_procs = var("TICKDB_NUM_PROCS")
		.unwrap_or("1".to_string())
		.parse::<i64>()
		.unwrap();

	let tables = open_tables();

	for _ in 0..num_procs {
		match unsafe { fork() } {
			Ok(ForkResult::Child) => {
				println!("fork {}", std::process::id());
				register_signal();

				V8::init();
				debug!("scripting init");

				let mut buffer = [0; BUFFER_SIZE];
				for stream in listener.incoming() {
					let stream = &mut stream.unwrap();
					debug!("stream start");
					let request_size = stream.read(&mut buffer).unwrap();

					let mut headers = [httparse::EMPTY_HEADER; 16];
					let mut request = Request::new(&mut headers);
					let body_offset: usize = request.parse(&buffer).unwrap().unwrap();
					let body = &buffer[body_offset..request_size];
					debug!("read stream");

					// println!("handle_request {}", std::str::from_utf8(&buffer[..request_size]).unwrap());
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
}
