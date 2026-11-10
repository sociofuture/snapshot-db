//! Scale / timing run with 200,000 rows per table. Not part of the normal test run:
//!
//!   cargo test --release --test scale -- --ignored --nocapture
//!
//! Prints load times (CSV and Arrow), query times, save/restore times and the server's memory.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{ArrayRef, Decimal128Array, Int64Array, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use mysql::prelude::Queryable;
use mysql::{Conn, LocalInfileHandler, Opts, OptsBuilder};
use sysinfo::{Pid, ProcessesToUpdate, System};

const N: usize = 200_000;

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
            .args(["--listen", &format!("127.0.0.1:{port}"), "--memory", "512M"])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn server"),
    )
}

fn rss_mb(s: &Server) -> f64 {
    let mut sys = System::new().expect("sysinfo");
    sys.refresh_processes(ProcessesToUpdate::All, true);
    sys.process(Pid::from_u32(s.0.id())).map(|p| p.memory() as f64 / 1048576.0).unwrap_or(f64::NAN)
}

fn csv_employee(out: &mut dyn std::io::Write) -> std::io::Result<()> {
    for i in 0..N {
        writeln!(out, "{i},name{i},{},{}-{:02}-{:02},{}.{:02}", i % 50, 2000 + i % 20, 1 + i % 12, 1 + i % 28, 300000 + i % 100000, i % 100)?;
    }
    Ok(())
}

fn csv_dept(out: &mut dyn std::io::Write) -> std::io::Result<()> {
    for i in 0..N {
        writeln!(out, "{i},dept{}", i % 50)?;
    }
    Ok(())
}

fn arrow_salary() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("emp_id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(12, 2), true),
        Field::new("updated", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..N as i64)),
        Arc::new(
            Decimal128Array::from_iter_values((0..N).map(|i| ((300000 + i % 100000) * 100 + i % 100) as i128))
                .with_precision_and_scale(12, 2)
                .unwrap(),
        ),
        Arc::new(TimestampMicrosecondArray::from_iter_values((0..N).map(|i| 1_700_000_000_000_000 + i as i64 * 1_000_000))),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let mut out = Vec::new();
    let mut w = StreamWriter::try_new(&mut out, &schema).unwrap();
    // Several batches, as a real producer would send.
    let mut off = 0;
    while off < N {
        let len = 20_000.min(N - off);
        w.write(&batch.slice(off, len)).unwrap();
        off += len;
    }
    w.finish().unwrap();
    drop(w);
    out
}

fn connect(port: u16, salary: Arc<Vec<u8>>) -> Conn {
    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .local_infile_handler(Some(LocalInfileHandler::new(move |name, out| {
            use std::io::Write;
            match name {
                b"employee.csv" => csv_employee(out),
                b"dept.csv" => csv_dept(out),
                b"salary.arrows" => out.write_all(&salary),
                _ => Ok(()),
            }
        })))
        .into();
    for _ in 0..100 {
        if let Ok(c) = Conn::new(opts.clone()) {
            return c;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("could not connect");
}

fn timed<T>(label: &str, f: impl FnOnce() -> T) -> T {
    let t = Instant::now();
    let r = f();
    println!("{label:<52} {:>9.1} ms", t.elapsed().as_secs_f64() * 1000.0);
    r
}

#[test]
#[ignore = "scale run; use --ignored --nocapture (ideally with --release)"]
fn two_hundred_thousand_rows() {
    let salary = Arc::new(arrow_salary());
    println!("\nArrow stream for salary: {:.1} MB", salary.len() as f64 / 1048576.0);

    let dir = tempfile::tempdir().unwrap();
    let url = format!("{}snap", url::Url::from_directory_path(dir.path()).unwrap());

    let server = start(33201, &[]);
    let mut c = connect(33201, salary.clone());

    c.query_drop(format!("SNAPSHOT CREATE TABLE employee (id BIGINT, name VARCHAR(30), dept_id INT, hired DATE, pay DECIMAL(12,2)) ROWS {N} BYTES_PER_ROW_HINT 12")).unwrap();
    c.query_drop(format!("SNAPSHOT CREATE TABLE dept (emp_id BIGINT, dept VARCHAR(20)) ROWS {N} BYTES_PER_ROW_HINT 8")).unwrap();
    c.query_drop(format!("SNAPSHOT CREATE TABLE salary (emp_id BIGINT, amount DECIMAL(12,2), updated DATETIME) ROWS {N}")).unwrap();

    println!("--- load");
    timed("LOAD employee  (CSV, 5 cols, 200k rows)", || {
        c.query_drop("LOAD DATA LOCAL INFILE 'employee.csv' INTO TABLE employee FIELDS TERMINATED BY ','").unwrap()
    });
    timed("LOAD dept      (CSV, 2 cols, 200k rows)", || {
        c.query_drop("LOAD DATA LOCAL INFILE 'dept.csv' INTO TABLE dept FIELDS TERMINATED BY ','").unwrap()
    });
    timed("LOAD salary    (Arrow, 3 cols, 200k rows)", || {
        c.query_drop("LOAD DATA LOCAL INFILE 'salary.arrows' INTO TABLE salary FORMAT ARROW").unwrap()
    });
    println!("server memory after load: {:.0} MB", rss_mb(&server));

    println!("--- queries");
    let n = |c: &mut Conn, q: &str| c.query_first::<i64, _>(q).unwrap().unwrap();
    assert_eq!(timed("COUNT(*) employee", || n(&mut c, "SELECT COUNT(*) FROM employee")), N as i64);
    assert_eq!(timed("point lookup  WHERE id = 123456", || n(&mut c, "SELECT id FROM employee WHERE id = 123456")), 123456);
    let groups: Vec<(String, i64)> = timed("GROUP BY dept (50 groups)", || {
        c.query("SELECT d.dept, COUNT(*) FROM dept d GROUP BY d.dept").unwrap()
    });
    assert_eq!(groups.len(), 50);
    assert_eq!(
        timed("3-way JOIN + COUNT(*)", || n(
            &mut c,
            "SELECT COUNT(*) FROM employee e JOIN dept d ON d.emp_id = e.id JOIN salary s ON s.emp_id = e.id"
        )),
        N as i64
    );
    let sum = timed("3-way JOIN + SUM(id) by dept", || {
        c.query::<(String, i64), _>(
            "SELECT d.dept, SUM(e.id) FROM employee e JOIN dept d ON d.emp_id = e.id JOIN salary s ON s.emp_id = e.id GROUP BY d.dept",
        )
        .unwrap()
    });
    assert_eq!(sum.iter().map(|r| r.1).sum::<i64>(), (N as i64 * (N as i64 - 1)) / 2);
    let top: Vec<(i64,)> = timed("ORDER BY pay DESC LIMIT 10", || {
        c.query("SELECT id FROM employee ORDER BY pay DESC, id LIMIT 10").unwrap()
    });
    assert_eq!(top.len(), 10);
    let all: Vec<(i64, String, String)> = timed("3-way JOIN returning 200k rows (transfer)", || {
        c.query("SELECT e.id, e.name, d.dept FROM employee e JOIN dept d ON d.emp_id = e.id JOIN salary s ON s.emp_id = e.id")
            .unwrap()
    });
    assert_eq!(all.len(), N);
    println!("server memory after queries: {:.0} MB", rss_mb(&server));

    println!("--- save / restore");
    timed("SNAPSHOT SAVE (local folder)", || c.query_drop(format!("SNAPSHOT SAVE '{url}'")).unwrap());
    let size = std::fs::metadata(dir.path().join("snap/arena.bin")).unwrap().len();
    println!("arena.bin: {:.1} MB", size as f64 / 1048576.0);
    drop(c);
    drop(server);

    let t = Instant::now();
    let restored = start(33202, &["--restore", &url]);
    let mut c2 = connect(33202, salary);
    let cnt = c2.query_first::<i64, _>("SELECT COUNT(*) FROM employee").unwrap().unwrap();
    println!("{:<52} {:>9.1} ms", "restart: --restore until first query answered", t.elapsed().as_secs_f64() * 1000.0);
    assert_eq!(cnt, N as i64);
    assert_eq!(
        c2.query_first::<i64, _>("SELECT COUNT(*) FROM employee e JOIN dept d ON d.emp_id = e.id JOIN salary s ON s.emp_id = e.id")
            .unwrap(),
        Some(N as i64)
    );
    println!("server memory after restore: {:.0} MB", rss_mb(&restored));

    let st: Vec<(String, String, String, String)> = c2.query("SNAPSHOT STATUS").unwrap();
    for r in &st {
        println!("  {:<10} {:<14} rows/used={:<10} reserved/capacity={}", r.0, r.1, r.2, r.3);
    }
    let _: HashMap<(), ()> = HashMap::new(); // keep the import used on all platforms
}
