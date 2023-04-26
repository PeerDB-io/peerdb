use postgres::{Client, NoTls, SimpleQueryMessage};
use std::{
    fs::{read_dir, File},
    io::{prelude::*, BufReader, Write},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};

fn input_files() -> Vec<String> {
    let sql_directory = read_dir("tests/sql").unwrap();
    sql_directory
        .filter_map(|sql_input| {
            sql_input.ok().and_then(|sql_file| {
                sql_file
                    .path()
                    .file_name()
                    .and_then(|n| n.to_str().map(String::from))
            })
        })
        .collect::<Vec<String>>()
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

    fn connect_dying(&self) -> Client {
        let connection_string = "host=localhost port=9900 password=peerdb user=peerdb";
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

        match client_result {
            Ok(c) => c,
            Err(_e) => {
                println!(
                    "unable to connect to server after {} attempts",
                    max_attempts
                );
                panic!("Failed to connect to server.");
            }
        }
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
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();

    let test_files = input_files();
    test_files.iter().for_each(|file| {
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

            res.iter().for_each(|row| {
                for column_head in &column_names {
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
            });

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
        let obtained_hash = sha256::digest(obtained_file.as_slice());
        let expected_hash = sha256::digest(expected_file.as_slice());

        // if there is a mismatch, print the diff, along with the path.
        if obtained_hash != expected_hash {
            println!("expected: {expected_output_path}");
            println!("obtained: {actual_output_path}");
        }

        assert_eq!(obtained_hash, expected_hash);
    });
}

#[test]
fn extended_query_protocol_no_params() {
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();

    // run `select * from peers` as a prepared statement.
    let stmt = client
        .prepare("select * from peers")
        .expect("Failed to prepare query");

    // run the prepared statement with no parameters.
    let res = client
        .execute(&stmt, &[])
        .expect("Failed to execute prepared statement");

    // check that the result is non-empty.
    assert!(res > 0);
}
