use http::Response;
use httparse::Request;
use nix::{
  sys::{signal, wait::waitpid},
  unistd::{fork, ForkResult, Pid}
};
use std::{
  env::var,
  io::{Read, Write},
  net::TcpListener,
  process::exit
};

const PORT: &str = "8080";
const BUFFER_SIZE: usize = 2 << 15;

extern "C" fn handle_sigint(_: i32) { exit(0); }

fn handle_request<'a>(req: &Request) -> Response<&'a [u8]> {
  let builder = Response::builder();
  match req.method {
    None => builder
      .status(400)
      .body("expected request method\n".as_bytes())
      .unwrap(),
    Some(m) => match m {
      "GET" => builder.body("tickdb here\n".as_bytes()).unwrap(),
      "POST" => builder.body("post\n".as_bytes()).unwrap(),
      _ => builder.status(404).body("not found\n".as_bytes()).unwrap()
    }
  }
}

fn main() {
  let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).unwrap();

  let num_threads = var("ZDB_NUM_THREADS")
    .unwrap_or("2".to_string())
    .parse::<i64>()
    .unwrap();

  for i in 0..num_threads {
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
        let mut buffer = [0; BUFFER_SIZE];
        for stream in listener.incoming() {
          let mut headers = vec![httparse::EMPTY_HEADER; 20];
          let mut stream = stream.unwrap();
          stream.read(&mut buffer).unwrap();

          let mut request = Request::new(&mut headers[..]);
          request.parse(&mut buffer).unwrap();

          let response = handle_request(&request);
          let header = format!(
            "{:?} {} {}\r\n\r\n",
            response.version(),
            response.status().as_str(),
            response.status().canonical_reason().unwrap_or("")
          );
          stream.write(header.as_bytes()).unwrap();
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
