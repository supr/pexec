use std::env;
use std::process;
use std::path::Path;
use std::sync::{mpsc,Arc,Mutex};
use std::error::Error;
use std::thread;

extern crate getopts;
use getopts::Options;

extern crate csv;
extern crate rustc_serialize;
extern crate num_cpus;

#[derive(RustcDecodable, Debug)]
struct HostRecord {
    hostname: String,
    port: Option<u16>,
    remote_path: String,
    local_path: String,
    sudo: bool
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    println!("{}", opts.usage(&brief));
}

fn parse_csv_records<P: AsRef<Path>>(csv_file: P, tx: mpsc::Sender<HostRecord>) {
    let csv_rdr_handle = csv::Reader::from_file(csv_file);

    if csv_rdr_handle.is_err() {
        let e = csv_rdr_handle.err().unwrap();
        println!("ERROR: {:?}", e.description());
        process::exit(1);
    }

    let mut rdr = csv_rdr_handle.unwrap().has_headers(false);

    thread::spawn(move || {
        for record_result in rdr.decode() {
            if let Ok(rec) = record_result {
                tx.send(rec);
            } else {
                println!("ERROR: {:?}", record_result.err().unwrap());
            }
        }
    });
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("H", "hosts", "hosts feeder file", "FILE");
    opts.optopt("u", "user", "remote ssh user name", "USER");
    opts.optflag("h", "help", "print this help menu");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => { println!("ERROR: {}", f.to_string()); process::exit(1); }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let feeder_file = matches.opt_str("H").unwrap_or("hosts.csv".to_string());
    let remote_user = matches.opt_str("u").unwrap_or("root".to_string());

    let (record_tx, record_rx) = mpsc::channel::<HostRecord>();
    parse_csv_records(Path::new(&feeder_file), record_tx);

    let data = Arc::new(Mutex::new(record_rx));
    let ncpus = num_cpus::get();

    let mut children = Vec::with_capacity(ncpus);

    for i in 0..ncpus {
        let data = data.clone();
        let child = thread::spawn(move || {
            loop {
                let guard = data.lock().unwrap();
                let record_result = (*guard).try_recv();

                if record_result.is_err() {
                    match record_result.err().unwrap() {
                        mpsc::TryRecvError::Disconnected => { return; },
                        _ => { continue; }
                    }
                }

                println!("{:?}", record_result.unwrap());
            }
        });

        children.insert(i, child);
    }

    for child in children {
        let _ = child.join();
    }
}
