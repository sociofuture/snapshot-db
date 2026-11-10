//! Save to object storage (a local folder here, same code path as S3), stop, restore.

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
            .args(["--listen", &format!("127.0.0.1:{port}"), "--memory", "16M"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server"),
    )
}

fn connect(port: u16) -> Conn {
    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .local_infile_handler(Some(LocalInfileHandler::new(|name, out| {
            use std::io::Write;
            let body: &[u8] = match name {
                b"emp.csv" => "1,Alice,1234.50,2020-04-01\n2,\u{592a}\u{90ce},0.25,2026-01-31\n".as_bytes(),
                b"later.csv" => b"7\n8\n9\n",
                _ => b"",
            };
            out.write_all(body)
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

fn names(c: &mut Conn, q: &str) -> Vec<String> {
    c.query::<(String,), _>(q).unwrap().into_iter().map(|r| r.0).collect()
}

#[test]
fn save_stop_restore_keeps_tables_immutability_and_pending_tables() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("{}snap1", url::Url::from_directory_path(dir.path()).unwrap());

    // --- first life: build a snapshot and save it
    {
        let _a = start(33081, &[]);
        let mut c = connect(33081);
        c.query_drop("SNAPSHOT CREATE TABLE employee (id BIGINT, name VARCHAR(30), pay DECIMAL(10,2), hired DATE) ROWS 2").unwrap();
        c.query_drop("LOAD DATA LOCAL INFILE 'emp.csv' INTO TABLE employee FIELDS TERMINATED BY ','").unwrap();
        c.query_drop("SNAPSHOT CREATE TABLE later (n BIGINT) ROWS 3").unwrap(); // declared, never loaded
        c.query_drop(format!("SNAPSHOT SAVE '{url}'")).expect("save");
    }
    assert!(dir.path().join("snap1/arena.bin").exists());
    assert!(dir.path().join("snap1/manifest.json").exists());

    // --- second life: a new process, restored from the image
    let _b = start(33082, &["--restore", &url]);
    let mut c = connect(33082);

    assert_eq!(names(&mut c, "SELECT name FROM employee ORDER BY id"), vec!["Alice", "\u{592a}\u{90ce}"]);
    assert_eq!(names(&mut c, "SELECT CAST(pay AS VARCHAR) FROM employee ORDER BY id"), vec!["1234.50", "0.25"]);
    assert_eq!(names(&mut c, "SELECT CAST(hired AS VARCHAR) FROM employee ORDER BY id"), vec!["2020-04-01", "2026-01-31"]);

    // Still immutable: a loaded table cannot be loaded again, and nothing can be written.
    let e = c.query_drop("LOAD DATA LOCAL INFILE 'emp.csv' INTO TABLE employee").unwrap_err().to_string();
    assert!(e.contains("immutable"), "{e}");
    assert!(c.query_drop("INSERT INTO employee VALUES (3, 'x', 1, '2020-01-01')").is_err());

    // The table that was only declared is restored empty and can still be loaded once.
    let status: Vec<(String, String, String, String)> = c.query("SNAPSHOT STATUS").unwrap();
    assert!(status.iter().any(|r| r.0 == "employee" && r.1 == "Loaded" && r.2 == "2"));
    assert!(status.iter().any(|r| r.0 == "later" && r.1 == "Created" && r.2 == "0"));
    c.query_drop("LOAD DATA LOCAL INFILE 'later.csv' INTO TABLE later").unwrap();
    assert_eq!(names(&mut c, "SELECT CAST(SUM(n) AS VARCHAR) FROM later"), vec!["24"]);
}

#[test]
fn a_corrupted_image_is_refused_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("{}snap2", url::Url::from_directory_path(dir.path()).unwrap());
    {
        let _a = start(33083, &[]);
        let mut c = connect(33083);
        c.query_drop("SNAPSHOT CREATE TABLE employee (id BIGINT, name VARCHAR(30), pay DECIMAL(10,2), hired DATE) ROWS 2").unwrap();
        c.query_drop("LOAD DATA LOCAL INFILE 'emp.csv' INTO TABLE employee FIELDS TERMINATED BY ','").unwrap();
        c.query_drop(format!("SNAPSHOT SAVE '{url}'")).unwrap();
    }
    // Flip one byte of the image.
    let image = dir.path().join("snap2/arena.bin");
    let mut bytes = std::fs::read(&image).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    std::fs::write(&image, bytes).unwrap();

    let mut b = start(33084, &["--restore", &url]);
    let mut exited = None;
    for _ in 0..50 {
        if let Some(s) = b.0.try_wait().unwrap() {
            exited = Some(s);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let status = exited.expect("server should refuse to start");
    assert!(!status.success());
}
