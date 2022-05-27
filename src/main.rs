use http::Response;
use httparse::Request;
use nix::{
  sys::{signal, wait::waitpid},
  unistd::{fork, ForkResult, Pid}
};
use std::{
  env::var,
  io::{Read, Write},
  net::{TcpListener, TcpStream},
  process::exit
};
mod server;
use server::query::handle_query;

const PORT: &str = "8080";
const BUFFER_SIZE: usize = 1 << 16;

extern "C" fn handle_sigint(_: i32) { exit(0); }

fn init_scripting() {
  let platform = v8::new_default_platform(0, false).make_shared();
  v8::V8::initialize_platform(platform);
  v8::V8::initialize();
}

fn handle_request(req: &Request, body: &[u8]) -> Response<Vec<u8>> {
  let builder = Response::builder();
  match req.method {
    None => builder
      .status(400)
      .body(Vec::from("expected request method\n"))
      .unwrap(),
    Some(m) => match m {
      "GET" => builder.body(Vec::from("tickdb here\n")).unwrap(),
      "POST" => builder.body(handle_query(req, body)).unwrap(),
      _ => builder.status(404).body(Vec::from("not found\n")).unwrap()
    }
  }
}

fn write_http_header(stream: &mut TcpStream, response: &Response<Vec<u8>>) {
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
        init_scripting();

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
          // println!("handle_request {}", std::str::from_utf8(&buffer[..request_size]).unwrap());

          let mut request = Request::new(&mut headers[..]);
          let body_offset: usize = request.parse(buffer).unwrap().unwrap();
          let body = &buffer[body_offset..request_size];

          let response = handle_request(&request, body);
          write_http_header(stream, &response);
          stream.write(response.body()).unwrap();
        }
      }
      Ok(ForkResult::Parent { child: _ }) => {}
      Err(_) => println!("Fork failed")
    }
  }
  drop(listener);

  waitpid(Some(Pid::from_raw(-1)), None).unwrap();
}
