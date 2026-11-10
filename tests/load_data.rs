//! End-to-end protocol tests: a real MySQL client streams files via LOAD DATA LOCAL INFILE.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{Conn, LocalInfileHandler, Opts, OptsBuilder};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn start(port: u16) -> Server {
    Server(
        Command::new(env!("CARGO_BIN_EXE_snapshot-db"))
            .args(["--listen", &format!("127.0.0.1:{port}"), "--memory", "16M"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server"),
    )
}

/// `files`: each LOAD DATA names a file; the handler generates `n` rows `i,nameI` for it.
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
    let mut last = String::new();
    for _ in 0..50 {
        match Conn::new(opts.clone()) {
            Ok(c) => return c,
            Err(e) => last = e.to_string(),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not connect: {last}");
}

fn status(conn: &mut Conn) -> Vec<(String, String, String, String)> {
    conn.query("SNAPSHOT STATUS").expect("status")
}

const CREATE: &str = "SNAPSHOT CREATE TABLE people (id BIGINT, name VARCHAR(100)) ROWS 100 BYTES_PER_ROW_HINT 16";
const LOAD: &str = "LOAD DATA LOCAL INFILE 'people.csv' INTO TABLE people FIELDS TERMINATED BY ','";

#[test]
fn create_load_once_and_refuse_everything_else() {
    let port = 33071;
    let _server = start(port);
    let mut conn = connect(port, 100);

    conn.query_drop(CREATE).expect("create");
    conn.query_drop(LOAD).expect("load data");
    assert_eq!(conn.affected_rows(), 100);

    let s = status(&mut conn);
    assert_eq!(s[0].0, "people");
    assert_eq!(s[0].1, "Loaded");
    assert_eq!(s[0].2, "100");

    // Connection is still in sync, and a table is loadable exactly once.
    let one: Option<i64> = conn.query_first("SELECT 1").unwrap();
    assert_eq!(one, Some(1));
    let again = conn.query_drop(LOAD).unwrap_err().to_string();
    assert!(again.contains("immutable"), "{again}");

    // REPLACE, writes, and duplicate CREATE are refused.
    let replace = conn
        .query_drop("LOAD DATA LOCAL INFILE 'p.csv' REPLACE INTO TABLE people")
        .unwrap_err()
        .to_string();
    assert!(replace.contains("REPLACE"), "{replace}");
    assert!(conn.query_drop("INSERT INTO people VALUES (1, 'x')").is_err());
    assert!(conn.query_drop(CREATE).unwrap_err().to_string().contains("already exists"));
}

#[test]
fn row_count_mismatch_fails_and_table_can_be_retried() {
    let port = 33072;
    let _server = start(port);
    let mut conn = connect(port, 60); // declared 100, file has 60

    conn.query_drop(CREATE).unwrap();
    let e = conn.query_drop(LOAD).unwrap_err().to_string();
    assert!(e.contains("ROWS 100") && e.contains("60"), "{e}");
    assert_eq!(status(&mut conn)[0].1, "Created"); // rolled back, not Loaded
    assert_eq!(status(&mut conn)[0].2, "0");
}

/// 社員 x 所属 x 給与 joined by employee id; files are served from memory by name.
fn connect_with_files(port: u16) -> Conn {
    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .local_infile_handler(Some(LocalInfileHandler::new(|name, out| {
            use std::io::Write;
            let body: &[u8] = match name {
                b"emp.csv" => b"1,Alice\n2,Bob\n3,\\N\n",
                b"dept.csv" => "1,\u{55b6}\u{696d}\n2,\u{958b}\u{767a}\n3,\u{958b}\u{767a}\n".as_bytes(),
                b"sal.csv" => b"1,300000.5\n2,400000\n3,350000\n",
                _ => b"",
            };
            out.write_all(body)
        })))
        .into();
    for _ in 0..50 {
        if let Ok(c) = Conn::new(opts.clone()) {
            return c;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not connect");
}

#[test]
fn select_joins_aggregates_and_rejects_writes() {
    let port = 33074;
    let _server = start(port);
    let mut c = connect_with_files(port);

    c.query_drop("SNAPSHOT CREATE TABLE employee (id BIGINT, name VARCHAR(50)) ROWS 3").unwrap();
    c.query_drop("SNAPSHOT CREATE TABLE dept (emp_id BIGINT, dept VARCHAR(50)) ROWS 3 BYTES_PER_ROW_HINT 16").unwrap();
    c.query_drop("SNAPSHOT CREATE TABLE salary (emp_id BIGINT, amount DOUBLE) ROWS 3").unwrap();
    for (f, t) in [("emp.csv", "employee"), ("dept.csv", "dept"), ("sal.csv", "salary")] {
        c.query_drop(format!("LOAD DATA LOCAL INFILE '{f}' INTO TABLE {t} FIELDS TERMINATED BY ','")).unwrap();
    }

    // Three-way join on employee id; NULL survives; backtick identifiers work (MySQL dialect).
    let rows: Vec<(i64, Option<String>, String, String)> = c
        .query(
            "SELECT e.id, e.name, d.dept, CAST(s.amount AS VARCHAR) \
             FROM `employee` e JOIN dept d ON d.emp_id = e.id JOIN salary s ON s.emp_id = e.id \
             ORDER BY e.id",
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, Some("Alice".into()), "\u{55b6}\u{696d}".into(), "300000.5".into()));
    assert_eq!(rows[2].1, None);

    // Aggregation.
    let agg: Vec<(String, i64, f64)> = c
        .query("SELECT d.dept, COUNT(*), SUM(s.amount) FROM dept d JOIN salary s ON s.emp_id = d.emp_id GROUP BY d.dept ORDER BY d.dept")
        .unwrap();
    assert_eq!(agg.len(), 2);
    let dev = agg.iter().find(|r| r.0 == "\u{958b}\u{767a}").unwrap();
    assert_eq!((dev.1, dev.2), (2, 750000.0));

    // Nothing that writes gets through the query path, however it is spelled.
    for q in [
        "INSERT INTO employee VALUES (9, 'x')",
        "CREATE TABLE t AS SELECT * FROM employee",
        "DELETE FROM employee",
        "DROP TABLE employee",
        "WITH x AS (SELECT 1) INSERT INTO employee SELECT 1, 'x'",
    ] {
        assert!(c.query_drop(q).is_err(), "should be refused: {q}");
    }
    let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM employee").unwrap();
    assert_eq!(n, Some(3));
}

fn connect_serving(port: u16, files: &'static [(&'static str, &'static str)]) -> Conn {
    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .local_infile_handler(Some(LocalInfileHandler::new(move |name, out| {
            use std::io::Write;
            let body = files.iter().find(|(n, _)| n.as_bytes() == name).map(|f| f.1).unwrap_or("");
            out.write_all(body.as_bytes())
        })))
        .into();
    for _ in 0..50 {
        if let Ok(c) = Conn::new(opts.clone()) {
            return c;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not connect");
}

#[test]
fn date_datetime_decimal_and_small_ints() {
    let port = 33075;
    let _server = start(port);
    let mut c = connect_serving(
        port,
        &[
            (
                "t.csv",
                "1,1,100,1234.50,2020-04-01,2020-04-01 09:30:15,A\n\
                 2,0,-5,0.25,2026-01-31,2026-01-31 23:59:59.5,B\n\
                 3,\\N,\\N,\\N,\\N,\\N,\\N\n",
            ),
            ("bad.csv", "1,2026-02-30\n"),
        ],
    );
    c.query_drop(
        "SNAPSHOT CREATE TABLE t (id BIGINT UNSIGNED NOT NULL, flag TINYINT, n SMALLINT, \
         price DECIMAL(10, 2), hired DATE, updated DATETIME, name VARCHAR(20)) ROWS 3",
    )
    .unwrap();
    c.query_drop("LOAD DATA LOCAL INFILE 't.csv' INTO TABLE t FIELDS TERMINATED BY ','").unwrap();

    let one = |c: &mut Conn, q: &str| -> Vec<Option<String>> {
        c.query::<(Option<String>,), _>(q).unwrap().into_iter().map(|r| r.0).collect()
    };
    assert_eq!(one(&mut c, "SELECT SUM(price) FROM t"), vec![Some("1234.75".into())]);
    assert_eq!(one(&mut c, "SELECT price FROM t ORDER BY id"), vec![Some("1234.50".into()), Some("0.25".into()), None]);
    assert_eq!(one(&mut c, "SELECT hired FROM t WHERE hired >= '2025-01-01'"), vec![Some("2026-01-31".into())]);
    assert_eq!(one(&mut c, "SELECT n FROM t ORDER BY id"), vec![Some("100".into()), Some("-5".into()), None]);
    let dt = one(&mut c, "SELECT updated FROM t ORDER BY id");
    assert_eq!(dt[0].as_deref(), Some("2020-04-01 09:30:15")); // space, no fraction
    assert!(dt[1].as_deref().unwrap().starts_with("2026-01-31 23:59:59.5"), "{dt:?}");
    assert_eq!(dt[2], None);

    // A bad date fails the load and says which row/column; the table can be retried.
    c.query_drop("SNAPSHOT CREATE TABLE b (id BIGINT, d DATE) ROWS 1").unwrap();
    let e = c.query_drop("LOAD DATA LOCAL INFILE 'bad.csv' INTO TABLE b FIELDS TERMINATED BY ','").unwrap_err().to_string();
    assert!(e.contains("row 1") && e.contains("DATE"), "{e}");

    // Out-of-range DECIMAL precision is refused at CREATE.
    let e = c.query_drop("SNAPSHOT CREATE TABLE x (a DECIMAL(50,2)) ROWS 1").unwrap_err().to_string();
    assert!(e.contains("unsupported type"), "{e}");
}

#[test]
fn out_of_memory_is_an_error() {
    let port = 33073;
    let _server = start(port); // 16M arena
    let mut conn = connect(port, 1);
    let e = conn
        .query_drop("SNAPSHOT CREATE TABLE big (a BIGINT) ROWS 100000000")
        .unwrap_err()
        .to_string();
    assert!(e.contains("out of memory"), "{e}");
    assert!(status(&mut conn).iter().all(|r| r.0 != "big"));
}
