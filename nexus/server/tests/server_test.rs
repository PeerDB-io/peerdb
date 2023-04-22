use postgres::{Client, NoTls, SimpleQueryMessage};
use std::{
    fs::{read_dir, File},
    io::{self, prelude::*, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

fn input_files() -> Vec<String> {
    let sql_directory = read_dir("tests/sql").unwrap();
    let test_files = sql_directory
        .filter_map(|sql_input| {
            sql_input.ok().and_then(|sql_file| {
                sql_file
                    .path()
                    .file_name()
                    .and_then(|n| n.to_str().map(|s| String::from(s)))
            })
        })
        .collect::<Vec<String>>();

    test_files
}

fn read_queries(filename: impl AsRef<Path>) -> Vec<String> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}

struct PeerDBServer {
    server: std::process::Child,
}

impl PeerDBServer {
    fn new() -> Self {
        let mut server_start = Command::new("cargo");
        server_start.envs(std::env::vars());
        server_start.args(["run"]);
        println!("Starting server...");

        let f = File::create("server.log").expect("unable to open server.log");
        let child = server_start
            .stdout(std::process::Stdio::from(f))
            .spawn()
            .expect("Failed to start peerdb-server");

        thread::sleep(Duration::from_millis(2000));
        println!("peerdb-server Server started");
        Self { server: child }
    }
}

impl Drop for PeerDBServer {
    fn drop(&mut self) {
        println!("Stopping server...");
        self.server.kill().expect("Failed to kill peerdb-server");
        println!("Server stopped");
    }
}

#[test]
fn server_test() {
    let connection_string = "host=localhost port=9900 password=peerdb user=peerdb";

    let _server = PeerDBServer::new();
    let mut client_result = Client::connect(connection_string, NoTls);

    let mut client_established = false;
    let max_attempts = 10;
    let mut attempts = 0;
    while !client_established && attempts < max_attempts {
        match client_result {
            Ok(_) => {
                client_established = true;
            }
            Err(_) => {
                attempts += 1;
                thread::sleep(Duration::from_millis(2000 * attempts));
                client_result = Client::connect(connection_string, NoTls);
            }
        }
    }

    let mut client = match client_result {
        Ok(c) => c,
        Err(_e) => {
            println!(
                "unable to connect to server after {} attempts",
                max_attempts
            );
            panic!("Failed to connect to server.");
        }
    };

    let test_files = input_files();
    for i in 0..test_files.len() {
        let file: &str = test_files[i].as_str();
        let queries = read_queries(["tests/sql/", file].concat());
        let actual_output_path = ["tests/results/actual/", file, ".out"].concat();
        let expected_output_path = ["tests/results/expected/", file, ".out"].concat();
        let mut output_file = File::create(["tests/results/actual/", file, ".out"].concat())
            .expect("Unable to create result file");
        for query in queries {
            let mut output = Vec::new();
            dbg!(query.as_str());

            // filter out comments and empty lines
            if query.starts_with("--") || query.is_empty() {
                continue;
            }

            let res = client
                .simple_query(query.as_str())
                .expect("Failed to query");
            let mut column_names = Vec::new();
            if res.is_empty() {
                panic!("No results for query: {}", query);
            }

            match res[0] {
                // Fetch column names for the output
                SimpleQueryMessage::Row(ref simplerow) => {
                    for column in simplerow.columns() {
                        column_names.push(column.name());
                    }
                }
                SimpleQueryMessage::CommandComplete(_x) => (),
                _ => (),
            };

            for i in 0..res.len() {
                let row = &res[i];
                for column_head in column_names.to_owned() {
                    let row_parse = match row {
                        SimpleQueryMessage::Row(ref simplerow) => simplerow.get(column_head),
                        SimpleQueryMessage::CommandComplete(_x) => None,
                        _ => None,
                    };

                    let row_value = match row_parse {
                        None => {
                            continue;
                        }
                        Some(x) => x,
                    };
                    output.push(row_value.to_owned());
                }
            }

            for i in &output {
                let output_line = (*i).as_bytes();
                output_file
                    .write_all(output_line)
                    .expect("Unable to write query output");
                output_file
                    .write_all("\n".as_bytes())
                    .expect("Output file write failure");
            }

            // flush the output file
            output_file.flush().expect("Unable to flush output file");
        }

        // Compare hash of expected and obtained files
        let obtained_file = std::fs::read(&actual_output_path).unwrap();
        let expected_file = std::fs::read(&expected_output_path).unwrap();
        let obtained_hash = sha256::digest_bytes(&obtained_file);
        let expected_hash = sha256::digest_bytes(&expected_file);

        // if there is a mismatch, print the diff, along with the path.
        if obtained_hash != expected_hash {
            println!("expected: {expected_output_path}");
            println!("obtained: {actual_output_path}");
        }

        assert_eq!(obtained_hash, expected_hash);
    }
}
