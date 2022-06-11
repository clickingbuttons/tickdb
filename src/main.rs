mod server;
use nix::sys::signal;
use tiny_http::{Server, Request, Response, Method};
use log::{debug, error};
use server::{
	langs::{v8::V8, julia::Julia},
	query::handle_query
};
use simple_logger::SimpleLogger;
use std::{
	process::exit,
	collections::HashMap,
	fs::read_dir
};
use tickdb::table::{paths::get_data_path, Table};

const ADDR: &str = "0.0.0.0:8080";

fn handle_request(
	req: &mut Request,
	tables: &HashMap<String, Table>
) -> Response<std::io::Cursor<Vec<u8>>> {
	match req.method() {
		Method::Get => Response::from_string("tickdb here\n"),
		Method::Post => {
			let mut content = String::new();
			req.as_reader().read_to_string(&mut content).unwrap();
			handle_query(content.as_bytes(), tables)
		},
		_ => Response::from_string("unsupported method").with_status_code(404)
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

extern "C" fn handle_sigint(_: i32) {
	exit(0);
}
fn register_handlers() {
	let sig_action = signal::SigAction::new(
		signal::SigHandler::Handler(handle_sigint),
		signal::SaFlags::SA_NODEFER,
		signal::SigSet::empty()
	);
	unsafe {
		signal::sigaction(signal::SIGINT, &sig_action).unwrap();
	}
}

fn main() {
	SimpleLogger::new().init().unwrap();

	debug!("register handlers");

	let server = Server::http(ADDR).unwrap();

	debug!("opening tables");
	let tables = open_tables();

	debug!("initing scripting");
	V8::init();
	Julia::init();

	// Julia creates its own sigint handler that doesn't exit.
	// https://github.com/JuliaLang/julia/blob/master/src/signals-unix.c#L967
	// TODO: Why do they do this? Is there a proper way to undo it and use the
	// default Rust handler instead?
	register_handlers();

	for mut request in server.incoming_requests() {
		//println!("handle_request {}", from_utf8(&buffer).unwrap());
		let response = handle_request(&mut request, &tables);
		if let Err(e) = request.respond(response) {
			error!("error sending response: {}", e);
		}
	}
}
