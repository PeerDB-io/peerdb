use postgres::{Client, NoTls, SimpleQueryMessage};
use std::{
    fs::{read_dir, File},
    io::{prelude::*, BufReader, Write},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};
mod create_peers;
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

fn setup_peers(client: &mut Client) {
    create_peers::create_bq::create(client);
    create_peers::create_pg::create(client);
    create_peers::create_sf::create(client);
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
        tracing::info!("Starting server...");

        let f = File::create("server.log").expect("unable to open server.log");
        let child = server_start
            .stdout(std::process::Stdio::from(f))
            .spawn()
            .expect("Failed to start peerdb-server");

        thread::sleep(Duration::from_millis(5000));
        tracing::info!("peerdb-server Server started");
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
                tracing::info!(
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
        tracing::info!("Stopping server...");
        self.server.kill().expect("Failed to kill peerdb-server");
        tracing::info!("Server stopped");
    }
}

#[test]
fn server_test() {
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();
    setup_peers(&mut client);
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
            tracing::info!("expected: {expected_output_path}");
            tracing::info!("obtained: {actual_output_path}");
        }

        assert_eq!(obtained_hash, expected_hash);
    });
}

#[test]
fn extended_query_protocol_no_params_catalog() {
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();
    // create bigquery peer so that the following command returns a non-zero result
    create_peers::create_bq::create(&mut client);
    // run `select * from peers` as a prepared statement.
    let stmt = client
        .prepare("SELECT * FROM peers;")
        .expect("Failed to prepare query");

    // run the prepared statement with no parameters.
    let res = client
        .execute(&stmt, &[])
        .expect("Failed to execute prepared statement");

    // check that the result is non-empty.
    assert!(res > 0);
}

#[test]
fn query_unknown_peer_doesnt_crash_server() {
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();

    // the server should not crash when a query is sent to an unknown peer.
    let query = "SELECT * FROM unknown_peer.test_table;";
    let res = client.simple_query(query);
    assert!(res.is_err());

    // assert that server is able to process a valid query after.
    let query = "SELECT * FROM peers;";
    let res = client.simple_query(query);
    assert!(res.is_ok());
}

#[test]
fn mirror_with_bad_staging_path_should_err() {
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();
    let cdc_query = "CREATE MIRROR fail_cdc
    FROM pg_test TO bq_test
    WITH TABLE MAPPING (
      public.cats:cats
    )
    WITH (
      cdc_sync_mode = 'avro'
    );";
    let res = client.simple_query(cdc_query);
    assert!(res.is_err());
    if let Err(e) = res {
        assert!(e.to_string().contains("cdc_staging_path missing or invalid for your destination peer"));
    }
    let snapshot_query = "CREATE MIRROR fail_snapshot
    FROM pg_test TO bq_test
    WITH TABLE MAPPING (
      public.cats:cats
    )
    WITH (
      snapshot_sync_mode = 'avro'
    );";
    let res = client.simple_query(snapshot_query);
    assert!(res.is_err());
    if let Err(e) = res {
        assert!(e.to_string().contains("snapshot_staging_path missing or invalid for your destination peer"));
    }
}

#[test]
fn snowflake_mirror_errs_for_bad_stage() {
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();
    let sf_query = "CREATE MIRROR fail_cdc
    FROM pg_test TO sf_test
    WITH TABLE MAPPING (
      public.cats:cats
    )
    WITH (
      snapshot_sync_mode = 'avro',
      snapshot_staging_path = 'something'
    );";
    let res = client.simple_query(sf_query);
    assert!(res.is_err());
    if let Err(e) = res {
        assert!(e.to_string()
        .contains("Staging path for Snowflake must either be an S3 URL or an empty string"));
    }
}

#[test]
#[ignore = "requires some work for extended query prepares on bigquery."]
fn extended_query_protocol_no_params_bq() {
    let server = PeerDBServer::new();
    let mut client = server.connect_dying();

    let query = "SELECT country,count(*) from bq_test.users GROUP BY country;";

    // run `select * from peers` as a prepared statement.
    let stmt = client.prepare(query).expect("Failed to prepare query");

    // run the prepared statement with no parameters.
    let res = client
        .execute(&stmt, &[])
        .expect("Failed to execute prepared statement");

    // check that the result is non-empty.
    assert!(res > 0);
}
