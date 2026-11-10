//! LOAD DATA LOCAL INFILE ... FORMAT ARROW: the client streams an Arrow IPC stream built in memory.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use mysql::prelude::Queryable;
use mysql::{Conn, LocalInfileHandler, Opts, OptsBuilder};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(port: u16) -> Server {
    Server(
        Command::new(env!("CARGO_BIN_EXE_snapshot-db"))
            .args(["--listen", &format!("127.0.0.1:{port}"), "--memory", "64M"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server"),
    )
}

/// Serves in-memory byte buffers by "file name", like Connector/J's setLocalInfileInputStream.
fn connect(port: u16, files: HashMap<String, Vec<u8>>) -> Conn {
    let files = Arc::new(files);
    let opts: Opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .prefer_socket(false)
        .local_infile_handler(Some(LocalInfileHandler::new(move |name, out| {
            use std::io::Write;
            let name = String::from_utf8_lossy(name).into_owned();
            out.write_all(files.get(&name).map(|v| v.as_slice()).unwrap_or(&[]))
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

fn ipc(batches: &[RecordBatch]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut w = StreamWriter::try_new(&mut out, &batches[0].schema()).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
    drop(w);
    out
}

fn col(c: &mut Conn, q: &str) -> Vec<Option<String>> {
    c.query::<(Option<String>,), _>(q).unwrap().into_iter().map(|r| r.0).collect()
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("pay", DataType::Decimal128(10, 2), true),
        Field::new("hired", DataType::Date32, true),
        Field::new("updated", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

fn batch(ids: Vec<i64>, names: Vec<Option<&str>>, pay: Vec<Option<i128>>, hired: Vec<Option<i32>>, upd: Vec<Option<i64>>) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(names)),
        Arc::new(Decimal128Array::from(pay).with_precision_and_scale(10, 2).unwrap()),
        Arc::new(Date32Array::from(hired)),
        Arc::new(TimestampMicrosecondArray::from(upd)),
    ];
    RecordBatch::try_new(schema(), cols).unwrap()
}

const DDL: &str = "SNAPSHOT CREATE TABLE emp (id BIGINT, name VARCHAR(30), pay DECIMAL(10,2), hired DATE, updated DATETIME) ROWS 3 BYTES_PER_ROW_HINT 16";
const LOAD: &str = "LOAD DATA LOCAL INFILE 'emp.arrows' INTO TABLE emp FORMAT ARROW";

fn sample_ipc() -> Vec<u8> {
    let t = 18353i64 * 86_400_000_000 + 34_215_000_000; // 2020-04-01 09:30:15
    // Two batches, to cover batch boundaries; a sliced start offset is covered by the 2nd batch.
    ipc(&[
        batch(vec![1, 2], vec![Some("Alice"), Some("\u{592a}\u{90ce}")], vec![Some(123450), Some(25)], vec![Some(18353), Some(20484)], vec![Some(t), None]),
        batch(vec![3], vec![None], vec![None], vec![None], vec![Some(t + 500_000)]),
    ])
}

#[test]
fn arrow_stream_loads_all_types_nulls_and_multiple_batches() {
    let _s = start(33091);
    let mut c = connect(33091, HashMap::from([("emp.arrows".into(), sample_ipc())]));
    c.query_drop(DDL).unwrap();
    c.query_drop(LOAD).expect("load arrow");
    assert_eq!(c.affected_rows(), 3);

    assert_eq!(col(&mut c, "SELECT name FROM emp ORDER BY id"), vec![Some("Alice".into()), Some("\u{592a}\u{90ce}".into()), None]);
    assert_eq!(col(&mut c, "SELECT CAST(pay AS VARCHAR) FROM emp ORDER BY id"), vec![Some("1234.50".into()), Some("0.25".into()), None]);
    assert_eq!(col(&mut c, "SELECT CAST(hired AS VARCHAR) FROM emp ORDER BY id"), vec![Some("2020-04-01".into()), Some("2026-01-31".into()), None]);
    let upd = col(&mut c, "SELECT updated FROM emp ORDER BY id");
    assert_eq!(upd[0].as_deref(), Some("2020-04-01 09:30:15"));
    assert_eq!(upd[1], None);
    assert!(upd[2].as_deref().unwrap().starts_with("2020-04-01 09:30:15.5"), "{upd:?}");
    assert_eq!(col(&mut c, "SELECT CAST(SUM(id) AS VARCHAR) FROM emp"), vec![Some("6".into())]);

    // Loaded once, like any other table.
    assert!(c.query_drop(LOAD).unwrap_err().to_string().contains("immutable"));
}

#[test]
fn a_large_stream_spanning_many_packets() {
    let n = 100_000usize;
    let names: Vec<String> = (0..n).map(|i| format!("name{i}")).collect();
    let b = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("name", DataType::Utf8, true)])),
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(names.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let bytes = ipc(&[b]);
    assert!(bytes.len() > 1 << 20, "should be bigger than one MySQL packet's worth of reads");

    let _s = start(33092);
    let mut c = connect(33092, HashMap::from([("big.arrows".into(), bytes)]));
    c.query_drop(format!("SNAPSHOT CREATE TABLE big (id BIGINT, name VARCHAR(20)) ROWS {n} BYTES_PER_ROW_HINT 12")).unwrap();
    c.query_drop("LOAD DATA LOCAL INFILE 'big.arrows' INTO TABLE big FORMAT ARROW").unwrap();
    assert_eq!(c.affected_rows(), n as u64);
    assert_eq!(col(&mut c, "SELECT name FROM big WHERE id = 99999"), vec![Some("name99999".into())]);
    assert_eq!(col(&mut c, "SELECT CAST(COUNT(*) AS VARCHAR) FROM big"), vec![Some(n.to_string())]);
}

#[test]
fn bad_streams_fail_the_load_and_the_table_can_be_retried() {
    let good = sample_ipc();
    let wrong_type = ipc(&[RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap()]);
    let truncated = good[..good.len() - 40].to_vec();
    let files = HashMap::from([
        ("wrong_type.arrows".to_string(), wrong_type),
        ("truncated.arrows".to_string(), truncated),
        ("garbage.arrows".to_string(), b"this is not an arrow stream at all, just text\n".to_vec()),
        ("emp.arrows".to_string(), good),
    ]);

    let _s = start(33093);
    let mut c = connect(33093, files);
    c.query_drop(DDL).unwrap();

    let e = c.query_drop("LOAD DATA LOCAL INFILE 'wrong_type.arrows' INTO TABLE emp FORMAT ARROW").unwrap_err().to_string();
    assert!(e.contains("columns"), "{e}"); // 1 column vs 5
    let e = c.query_drop("LOAD DATA LOCAL INFILE 'truncated.arrows' INTO TABLE emp FORMAT ARROW").unwrap_err().to_string();
    assert!(e.contains("Arrow") || e.contains("ROWS"), "{e}");
    let e = c.query_drop("LOAD DATA LOCAL INFILE 'garbage.arrows' INTO TABLE emp FORMAT ARROW").unwrap_err().to_string();
    assert!(e.contains("Arrow"), "{e}");

    // After three failures the table is untouched and the good stream still loads.
    let st: Vec<(String, String, String, String)> = c.query("SNAPSHOT STATUS").unwrap();
    assert!(st.iter().any(|r| r.0 == "emp" && r.1 == "Created" && r.2 == "0"), "{st:?}");
    c.query_drop(LOAD).unwrap();
    assert_eq!(col(&mut c, "SELECT name FROM emp WHERE id = 1"), vec![Some("Alice".into())]);
}

#[test]
fn a_column_type_mismatch_names_the_column() {
    // Same column count and names as the table, but `id` is Int32 while the table says BIGINT.
    let mut fields = schema().fields().iter().map(|f| f.as_ref().clone()).collect::<Vec<_>>();
    fields[0] = Field::new("id", DataType::Int32, false);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(StringArray::from(vec![Some("a")])),
        Arc::new(Decimal128Array::from(vec![Some(1i128)]).with_precision_and_scale(10, 2).unwrap()),
        Arc::new(Date32Array::from(vec![Some(1)])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(1i64)])),
    ];
    let bytes = ipc(&[RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()]);

    let _s = start(33094);
    let mut c = connect(33094, HashMap::from([("emp.arrows".into(), bytes)]));
    c.query_drop(DDL).unwrap();
    let e = c.query_drop(LOAD).unwrap_err().to_string();
    assert!(e.contains("id") && e.contains("Int32") && e.contains("Int64"), "{e}");
}
