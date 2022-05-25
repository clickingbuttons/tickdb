use http::Response;
use httparse::Request;
use nix::{
  sys::{signal, wait::waitpid},
  unistd::{fork, ForkResult, Pid}
};
use std::{
  env::var,
  io::{Read, Write},
  net::{TcpStream, TcpListener},
  process::exit
};
mod server;
use server::query::handle_query;

const PORT: &str = "8080";
const BUFFER_SIZE: usize = 1 << 16;

extern "C" fn handle_sigint(_: i32) { exit(0); }

fn handle_request(req: &Request, body: &[u8]) -> Response<String> {
  let builder = Response::builder();
  match req.method {
    None => builder
      .status(400)
      .body("expected request method\n".to_string())
      .unwrap(),
    Some(m) => match m {
      "GET" => builder.body("tickdb here\n".to_string()).unwrap(),
      "POST" => {
				let resp = handle_query(req, body);
				builder.body(resp).unwrap()
			},
      _ => builder.status(404).body("not found\n".to_string()).unwrap()
    }
  }
}

fn write_http_header(stream: &mut TcpStream, response: &Response<String>) {
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
	stream.write(header.as_bytes()).unwrap();
}

fn main() {
  let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).unwrap();

  let num_procs = var("TICKDB_NUM_PROCS")
    .unwrap_or("2".to_string())
    .parse::<i64>()
    .unwrap();

  for i in 0..num_procs {
    match unsafe { fork() } {
      Ok(ForkResult::Child) => {
        println!("fork {}", i);
        let sig_action = signal::SigAction::new(
          signal::SigHandler::Handler(handle_sigint),
          signal::SaFlags::SA_NODEFER,
          signal::SigSet::empty()
        );
        unsafe {
          signal::sigaction(signal::SIGINT, &sig_action).unwrap();
        }
        let buffer = &mut [0; BUFFER_SIZE];
        for stream in listener.incoming() {
          let mut headers = vec![httparse::EMPTY_HEADER; 20];
          let stream = &mut stream.unwrap();
          let request_size = stream.read(buffer).unwrap();

          let mut request = Request::new(&mut headers[..]);
          let body_offset: usize = request.parse(buffer).unwrap().unwrap();
					let body = &buffer[body_offset..request_size];

          let response = handle_request(&request, body);
					write_http_header(stream, &response);
          stream.write(response.body().as_bytes()).unwrap();
        }
      }
      Ok(ForkResult::Parent { child: _ }) => {}
      Err(_) => println!("Fork failed")
    }
  }
  drop(listener);
  waitpid(Some(Pid::from_raw(-1)), None).unwrap();
}
