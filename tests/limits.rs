//! --max-rows and --query-memory: a runaway query fails, the server and the connection survive.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{Conn, LocalInfileHandler, Opts, OptsBuilder};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(port: u16, extra: &[&str]) -> Server {
    Server(
        Command::new(env!("CARGO_BIN_EXE_snapshot-db"))
            .args(["--listen", &format!("127.0.0.1:{port}"), "--memory", "128M"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server"),
    )
}

/// Every LOAD DATA gets `rows` lines "i,name<i>".
fn connect(port: u16, rows: usize) -> Conn {
    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .local_infile_handler(Some(LocalInfileHandler::new(move |_name, out| {
            use std::io::Write;
            for i in 0..rows {
                writeln!(out, "{i},name{i}")?;
            }
            Ok(())
        })))
        .into();
    for _ in 0..80 {
        if let Ok(c) = Conn::new(opts.clone()) {
            return c;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not connect");
}

fn count(c: &mut Conn, q: &str) -> Option<i64> {
    c.query_first::<i64, _>(q).unwrap()
}

#[test]
fn max_rows_stops_an_oversized_result_and_the_connection_survives() {
    let _s = start(33101, &["--max-rows", "100"]);
    let mut c = connect(33101, 10);
    c.query_drop("SNAPSHOT CREATE TABLE t (id BIGINT, name VARCHAR(20)) ROWS 10 BYTES_PER_ROW_HINT 10").unwrap();
    c.query_drop("LOAD DATA LOCAL INFILE 'x' INTO TABLE t FIELDS TERMINATED BY ','").unwrap();

    // 10 x 10 = 100 rows: exactly at the limit, allowed.
    let rows: Vec<(i64, i64)> = c.query("SELECT a.id, b.id FROM t a CROSS JOIN t b").unwrap();
    assert_eq!(rows.len(), 100);

    // 10 x 10 x 10 = 1000 rows: refused, with a hint about what to do.
    let e = c
        .query::<(i64,), _>("SELECT a.id FROM t a CROSS JOIN t b CROSS JOIN t c")
        .unwrap_err()
        .to_string();
    assert!(e.contains("--max-rows 100"), "{e}");

    // The same connection still works afterwards (the protocol stayed in sync).
    assert_eq!(count(&mut c, "SELECT COUNT(*) FROM t"), Some(10));
    // An aggregate over the same big join returns one row, so it is fine.
    assert_eq!(count(&mut c, "SELECT COUNT(*) FROM t a CROSS JOIN t b CROSS JOIN t c"), Some(1000));
}

#[test]
fn max_rows_zero_means_unlimited() {
    let _s = start(33102, &["--max-rows", "0"]);
    let mut c = connect(33102, 10);
    c.query_drop("SNAPSHOT CREATE TABLE t (id BIGINT, name VARCHAR(20)) ROWS 10 BYTES_PER_ROW_HINT 10").unwrap();
    c.query_drop("LOAD DATA LOCAL INFILE 'x' INTO TABLE t FIELDS TERMINATED BY ','").unwrap();
    let rows: Vec<(i64,)> = c.query("SELECT a.id FROM t a CROSS JOIN t b CROSS JOIN t c").unwrap();
    assert_eq!(rows.len(), 1000);
}

#[test]
fn query_memory_stops_a_join_that_needs_too_much_and_the_server_survives() {
    // A tiny query pool; the arena (--memory) is separate and untouched by the join.
    let _s = start(33103, &["--query-memory", "1M"]);
    let mut c = connect(33103, 100_000);
    c.query_drop("SNAPSHOT CREATE TABLE big (id BIGINT, name VARCHAR(20)) ROWS 100000 BYTES_PER_ROW_HINT 12").unwrap();
    c.query_drop("LOAD DATA LOCAL INFILE 'x' INTO TABLE big FIELDS TERMINATED BY ','").unwrap();

    let before: Vec<(String, String, String, String)> = c.query("SNAPSHOT STATUS").unwrap();
    let arena_before = before.iter().find(|r| r.0 == "(arena)").unwrap().2.clone();

    let e = c
        .query::<(i64,), _>("SELECT COUNT(*) FROM big a JOIN big b ON a.id = b.id")
        .unwrap_err()
        .to_string();
    assert!(e.to_lowercase().contains("resources exhausted") || e.to_lowercase().contains("memory"), "{e}");

    // The join did not touch the arena, and the server and this connection are fine.
    let after: Vec<(String, String, String, String)> = c.query("SNAPSHOT STATUS").unwrap();
    assert_eq!(after.iter().find(|r| r.0 == "(arena)").unwrap().2, arena_before);
    assert_eq!(count(&mut c, "SELECT COUNT(*) FROM big"), Some(100_000));
    assert_eq!(count(&mut c, "SELECT id FROM big WHERE id = 99999"), Some(99_999));
}
