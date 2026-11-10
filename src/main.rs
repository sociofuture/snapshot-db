//! snapshot-db: read-only MySQL-protocol server. Data enters only via LOAD DATA LOCAL INFILE.
//!
//!   SNAPSHOT CREATE TABLE t (id BIGINT, name VARCHAR(100)) ROWS 1000 [BYTES_PER_ROW_HINT 32];
//!   LOAD DATA LOCAL INFILE 'x.csv' INTO TABLE t FIELDS TERMINATED BY ',';
//!
//! All memory is one arena reserved at startup (`--memory`). Running out is an error.

mod arena;
mod loader;
mod persist;
mod table;
mod values;

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use arena::Arena;
use async_trait::async_trait;
use clap::Parser;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::execution::context::SQLOptions;
use datafusion::prelude::{SessionConfig, SessionContext};
use loader::{parse_load_data, LineParser};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind,
    InitWriter, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use table::{ColType, ColumnDef, State, Table};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:3307")]
    listen: String,
    /// Size of the memory arena, fixed for the life of the process (e.g. 512M, 8G).
    #[arg(long, default_value = "256M", value_parser = parse_size)]
    memory: usize,
    /// Restore a saved image at startup, e.g. s3://bucket/prefix or file:///path.
    #[arg(long)]
    restore: Option<String>,
}

fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('K' | 'k') => (&s[..s.len() - 1], 1usize << 10),
        Some('M' | 'm') => (&s[..s.len() - 1], 1 << 20),
        Some('G' | 'g') => (&s[..s.len() - 1], 1 << 30),
        _ => (s, 1),
    };
    num.parse::<usize>().map(|n| n * mult).map_err(|e| e.to_string())
}

struct Store {
    arena: Arena,
    tables: HashMap<String, Table>,
}

struct Inner {
    store: Mutex<Store>,
    /// DataFusion catalog holding the loaded (immutable) tables. Never locked together with `store`
    /// across an await.
    ctx: SessionContext,
}
type Shared = Arc<Inner>;

struct Loading {
    table: String,
    parser: LineParser,
    error: Option<String>,
}

struct Session {
    shared: Shared,
    loading: Option<Loading>,
}

/// `SNAPSHOT CREATE TABLE name (col type, ...) ROWS n [BYTES_PER_ROW_HINT m]`
fn parse_create(q: &str) -> Option<Result<(String, Vec<ColumnDef>, usize, usize), String>> {
    let rest = q.trim().trim_end_matches(';').trim();
    let upper = rest.to_ascii_uppercase();
    let prefix = "SNAPSHOT CREATE TABLE ";
    if !upper.starts_with(prefix) {
        return None;
    }
    let body = &rest[prefix.len()..];
    let (open, close) = (body.find('('), body.rfind(')'));
    let (Some(open), Some(close)) = (open, close) else {
        return Some(Err("expected (column definitions)".into()));
    };
    let name = body[..open].trim().trim_matches('`').to_ascii_lowercase();
    if name.is_empty() {
        return Some(Err("missing table name".into()));
    }
    let mut cols = Vec::new();
    for def in split_top_level(&body[open + 1..close]) {
        let mut it = def.split_whitespace();
        let Some(cname) = it.next() else {
            return Some(Err("empty column definition".into()));
        };
        // The type is everything up to a column modifier such as NOT NULL / DEFAULT.
        const MODIFIERS: [&str; 9] =
            ["NOT", "NULL", "DEFAULT", "PRIMARY", "COMMENT", "AUTO_INCREMENT", "UNIQUE", "KEY", "COLLATE"];
        let ctype: Vec<&str> = it.take_while(|t| !MODIFIERS.contains(&t.to_ascii_uppercase().as_str())).collect();
        if ctype.is_empty() {
            return Some(Err(format!("bad column definition '{}'", def.trim())));
        }
        let ctype = ctype.join(" ");
        let Some(ty) = ColType::parse(&ctype) else {
            return Some(Err(format!(
                "unsupported type '{ctype}' (TINYINT, SMALLINT, INT, BIGINT, DOUBLE, DECIMAL(p,s), DATE, DATETIME, VARCHAR)"
            )));
        };
        cols.push(ColumnDef { name: cname.trim_matches('`').to_ascii_lowercase(), ty });
    }
    let tail: Vec<String> = body[close + 1..].split_whitespace().map(|t| t.to_ascii_uppercase()).collect();
    let (mut rows, mut hint) = (None, 32usize);
    let mut i = 0;
    while i < tail.len() {
        let val = tail.get(i + 1).and_then(|v| v.parse::<usize>().ok());
        match (tail[i].as_str(), val) {
            ("ROWS", Some(v)) => rows = Some(v),
            ("BYTES_PER_ROW_HINT", Some(v)) => hint = v,
            _ => return Some(Err(format!("unexpected '{}' (use ROWS n [BYTES_PER_ROW_HINT m])", tail[i]))),
        }
        i += 2;
    }
    let Some(rows) = rows else {
        return Some(Err("ROWS n is required: the memory is reserved up front".into()));
    };
    Some(Ok((name, cols, rows, hint)))
}

/// Splits on commas that are not inside parentheses, so `DECIMAL(10,2)` stays in one piece.
fn split_top_level(s: &str) -> Vec<&str> {
    let (mut parts, mut depth, mut start) = (Vec::new(), 0i32, 0);
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn text_cols(names: &[&str]) -> Vec<Column> {
    names
        .iter()
        .map(|n| Column {
            table: String::new(),
            column: n.to_string(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        })
        .collect()
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Session {
    type Error = io::Error;

    fn version(&self) -> String {
        "8.0.36-snapshot-db".to_string()
    }

    async fn local_infile_request(&mut self, query: &str) -> Option<String> {
        let Ok(opts) = parse_load_data(query)? else { return None };
        let mut st = self.shared.store.lock().unwrap();
        let t = st.tables.get_mut(&opts.table)?;
        if t.state != State::Created {
            return None; // on_query reports why
        }
        t.state = State::Loading;
        self.loading = Some(Loading { table: opts.table.clone(), parser: LineParser::new(&opts), error: None });
        Some(opts.file)
    }

    async fn on_infile_data(&mut self, chunk: &[u8]) {
        let Some(l) = self.loading.as_mut() else { return };
        if l.error.is_some() {
            return; // keep draining the stream; the error is reported at the end
        }
        let mut st = self.shared.store.lock().unwrap();
        let Store { arena, tables } = &mut *st;
        let t = tables.get_mut(&l.table).expect("table vanished");
        if let Err(e) = l.parser.feed(chunk, &mut |row| t.push_row(arena, row)) {
            l.error = Some(e);
        }
    }

    async fn on_infile_end<'a>(&'a mut self, results: QueryResultWriter<'a, W>) -> io::Result<()> {
        let mut l = self.loading.take().expect("infile_end without a load");
        // Finish all work under the lock in a scope that ends before any `.await`.
        let outcome: Result<u64, String> = {
            let mut st = self.shared.store.lock().unwrap();
            let Store { arena, tables } = &mut *st;
            let t = tables.get_mut(&l.table).expect("table vanished");
            if l.error.is_none() {
                if let Err(e) = l.parser.finish(&mut |row| t.push_row(arena, row)) {
                    l.error = Some(e);
                }
            }
            if l.error.is_none() && t.rows != t.rows_expected {
                l.error = Some(format!("table was declared with ROWS {} but the file has {} rows", t.rows_expected, t.rows));
            }
            // Publish: build the zero-copy Arrow view (this validates UTF-8) and register it.
            if l.error.is_none() {
                if let Err(e) = persist::publish(&self.shared.ctx, arena, t) {
                    l.error = Some(e);
                }
            }
            match l.error.take() {
                Some(e) => {
                    t.reset(arena);
                    Err(e)
                }
                None => {
                    t.state = State::Loaded;
                    Ok(t.rows as u64)
                }
            }
        };
        match outcome {
            Err(e) => results.error(ErrorKind::ER_UNKNOWN_ERROR, e.as_bytes()).await,
            Ok(rows) => {
                eprintln!("loaded {rows} rows into {}", l.table);
                results.completed(OkResponse { affected_rows: rows, ..Default::default() }).await
            }
        }
    }

    async fn on_prepare<'a>(&'a mut self, _q: &'a str, info: StatementMetaWriter<'a, W>) -> io::Result<()> {
        info.error(ErrorKind::ER_UNKNOWN_ERROR, b"prepared statements are not supported yet" as &[u8]).await
    }

    async fn on_execute<'a>(&'a mut self, _id: u32, _p: ParamParser<'a>, results: QueryResultWriter<'a, W>) -> io::Result<()> {
        results.error(ErrorKind::ER_UNKNOWN_ERROR, b"prepared statements are not supported yet" as &[u8]).await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) where W: 'async_trait {}

    async fn on_init<'a>(&'a mut self, _db: &'a str, w: InitWriter<'a, W>) -> io::Result<()> {
        w.ok().await
    }

    async fn on_query<'a>(&'a mut self, query: &'a str, results: QueryResultWriter<'a, W>) -> io::Result<()> {
        let q = query.trim().trim_end_matches(';').trim();

        // A LOAD DATA that local_infile_request declined: say why.
        match parse_load_data(q) {
            Some(Err(e)) => return results.error(ErrorKind::ER_UNKNOWN_ERROR, e.as_bytes()).await,
            Some(Ok(o)) => {
                let state = self.shared.store.lock().unwrap().tables.get(&o.table).map(|t| t.state);
                let msg = match state {
                    None => format!("table '{}' does not exist (SNAPSHOT CREATE TABLE first)", o.table),
                    Some(State::Created) => "load refused".to_string(),
                    Some(_) => format!("table '{}' is already loaded (or loading) and is immutable", o.table),
                };
                return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes()).await;
            }
            None => {}
        }

        match parse_create(q) {
            Some(Err(e)) => return results.error(ErrorKind::ER_UNKNOWN_ERROR, e.as_bytes()).await,
            Some(Ok((name, cols, rows, hint))) => {
                let created: Result<(), (ErrorKind, String)> = {
                    let mut st = self.shared.store.lock().unwrap();
                    if st.tables.contains_key(&name) {
                        Err((ErrorKind::ER_TABLE_EXISTS_ERROR, format!("table '{name}' already exists")))
                    } else {
                        match Table::create(&mut st.arena, name.clone(), cols, rows, hint) {
                            Ok(t) => {
                                st.tables.insert(name, t);
                                Ok(())
                            }
                            Err(e) => Err((ErrorKind::ER_UNKNOWN_ERROR, e)),
                        }
                    }
                };
                return match created {
                    Ok(()) => results.completed(OkResponse::default()).await,
                    Err((kind, msg)) => results.error(kind, msg.as_bytes()).await,
                };
            }
            None => {}
        }

        // SNAPSHOT SAVE 's3://bucket/prefix'
        if q.len() > 14 && q[..14].eq_ignore_ascii_case("snapshot save ") {
            let url = q[14..].trim().trim_matches(|c| c == '\'' || c == '"').to_string();
            return match persist::save(&self.shared, &url).await {
                Ok(bytes) => {
                    eprintln!("saved {bytes} bytes to {url}");
                    results.completed(OkResponse::default()).await
                }
                Err(e) => results.error(ErrorKind::ER_UNKNOWN_ERROR, format!("save failed: {e}").as_bytes()).await,
            };
        }

        if q.eq_ignore_ascii_case("snapshot status") {
            let cols = text_cols(&["table", "state", "rows", "reserved_bytes"]);
            let rows: Vec<[String; 4]> = {
                let st = self.shared.store.lock().unwrap();
                let mut v: Vec<_> = st
                    .tables
                    .values()
                    .map(|t| [t.name.clone(), format!("{:?}", t.state), t.rows.to_string(), t.reserved_bytes().to_string()])
                    .collect();
                v.sort();
                v.push([
                    "(arena)".into(),
                    "used/capacity".into(),
                    st.arena.used().to_string(),
                    st.arena.capacity().to_string(),
                ]);
                v
            };
            let mut w = results.start(&cols).await?;
            for r in rows {
                w.write_row(r).await?;
            }
            return w.finish().await;
        }

        if q.eq_ignore_ascii_case("select 1") {
            let cols = [Column {
                table: String::new(),
                column: "1".to_string(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            }];
            let mut w = results.start(&cols).await?;
            w.write_row([1i64]).await?;
            return w.finish().await;
        }

        // `SELECT @@var` probes sent by clients on connect. Known variables get a value,
        // unknown ones NULL (this is what MySQL does for an unset variable).
        if let Some(var) = q.strip_prefix("select @@").or_else(|| q.strip_prefix("SELECT @@")) {
            if !var.contains(char::is_whitespace) && !var.contains(',') {
                let name = var.trim_start_matches("session.").trim_start_matches("global.");
                let value: Option<&str> = match name.to_ascii_lowercase().as_str() {
                    "version" => Some("8.0.36-snapshot-db"),
                    "version_comment" => Some("snapshot-db"),
                    "sql_mode" => Some("ONLY_FULL_GROUP_BY"),
                    "character_set_client" | "character_set_results" => Some("utf8mb4"),
                    "collation_connection" => Some("utf8mb4_general_ci"),
                    "read_only" | "transaction_read_only" => Some("1"),
                    "autocommit" => Some("1"),
                    "max_allowed_packet" => Some("67108864"),
                    "lower_case_table_names" => Some("0"),
                    "socket" => Some(""),
                    "time_zone" | "system_time_zone" => Some("UTC"),
                    _ => None,
                };
                let cols = [Column {
                    table: String::new(),
                    column: format!("@@{var}"),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                }];
                let mut w = results.start(&cols).await?;
                w.write_row([value]).await?;
                return w.finish().await;
            }
        }

        // Session settings that clients send on connect are accepted and ignored for now.
        if q.len() >= 4 && q[..4].eq_ignore_ascii_case("set ") {
            return results.completed(OkResponse::default()).await;
        }

        // Queries go to DataFusion, but only plain queries: DDL, DML (INSERT/UPDATE/DELETE) and
        // statements like SET/COPY are rejected by SQLOptions before anything runs.
        let first = q.split_whitespace().next().unwrap_or("").to_ascii_lowercase();
        if matches!(first.as_str(), "select" | "with" | "(" | "values" | "explain") {
            let opts = SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false);
            let ctx = self.shared.ctx.clone();
            let outcome = async {
                let df = ctx.sql_with_options(q, opts).await?;
                let schema = Arc::new(df.schema().as_arrow().clone());
                let batches = df.collect().await?;
                Ok::<_, datafusion::error::DataFusionError>((schema, batches))
            }
            .await;
            return match outcome {
                Ok((schema, batches)) => send_batches(results, &schema, &batches).await,
                Err(e) => results.error(ErrorKind::ER_PARSE_ERROR, e.to_string().as_bytes()).await,
            };
        }

        results
            .error(ErrorKind::ER_OPTION_PREVENTS_STATEMENT, b"snapshot-db is read-only (use LOAD DATA LOCAL INFILE)" as &[u8])
            .await
    }
}

fn mysql_type(dt: &DataType) -> ColumnType {
    match dt {
        DataType::Int8 | DataType::UInt8 => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int16 | DataType::UInt16 => ColumnType::MYSQL_TYPE_SHORT,
        DataType::Int32 | DataType::UInt32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 | DataType::UInt64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Decimal128(..) | DataType::Decimal256(..) => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        DataType::Date32 | DataType::Date64 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Timestamp(..) => ColumnType::MYSQL_TYPE_DATETIME,
        _ => ColumnType::MYSQL_TYPE_VAR_STRING,
    }
}

/// Sends Arrow batches as a MySQL text-protocol result set.
async fn send_batches<W: AsyncWrite + Send + Unpin>(
    results: QueryResultWriter<'_, W>,
    schema: &Schema,
    batches: &[RecordBatch],
) -> io::Result<()> {
    let cols: Vec<Column> = schema
        .fields()
        .iter()
        .map(|f| Column {
            table: String::new(),
            column: f.name().clone(),
            coltype: mysql_type(f.data_type()),
            colflags: ColumnFlags::empty(),
        })
        .collect();
    let mut w = results.start(&cols).await?;
    // MySQL writes DATETIME with a space, not Arrow's 'T'.
    let opts = FormatOptions::default().with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"));
    for b in batches {
        // Format the batch to strings synchronously: ArrayFormatter is not Send, so it must not
        // live across an await.
        let cells: Vec<Vec<Option<String>>> = {
            let fmts = b
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            (0..b.num_rows())
                .map(|r| {
                    b.columns()
                        .iter()
                        .zip(&fmts)
                        .map(|(c, f)| if c.is_null(r) { None } else { Some(f.value(r).to_string()) })
                        .collect()
                })
                .collect()
        };
        for row in cells {
            for cell in row {
                w.write_col(cell)?;
            }
            w.end_row().await?;
        }
    }
    w.finish().await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // MySQL dialect: backtick identifiers, LIMIT a, b, ... Identifiers are folded to lower case.
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "MySQL");
    let store: Shared = Arc::new(Inner {
        store: Mutex::new(Store { arena: Arena::new(args.memory)?, tables: HashMap::new() }),
        ctx: SessionContext::new_with_config(config),
    });
    if let Some(url) = &args.restore {
        let used = persist::restore(&store, url).await.map_err(|e| anyhow::anyhow!("restore failed: {e}"))?;
        eprintln!("restored {used} bytes from {url}");
    }
    let listener = TcpListener::bind(&args.listen).await?;
    eprintln!("snapshot-db listening on {} (arena {} bytes)", args.listen, args.memory);
    loop {
        let (stream, _) = listener.accept().await?;
        let session = Session { shared: store.clone(), loading: None };
        tokio::spawn(async move {
            let (r, w) = stream.into_split();
            if let Err(e) = AsyncMysqlIntermediary::run_on(session, r, w).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}
