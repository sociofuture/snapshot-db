//! Parsing of the LOAD DATA statement options and of the delimited file stream.
//! The stream arrives in arbitrary chunks, so lines are reassembled across chunk boundaries.

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Format {
    /// Delimited text (the default).
    Csv,
    /// Arrow IPC stream, binary.
    Arrow,
}

pub struct LoadOpts {
    pub format: Format,
    pub file: String,
    pub table: String,
    pub term: u8,
    pub enclosed: Option<u8>,
    pub ignore_lines: usize,
}

/// Interprets the escapes allowed in `TERMINATED BY '...'`.
fn unescape_delim(s: &str) -> Option<u8> {
    match s {
        "\\t" => Some(b'\t'),
        "\\n" => Some(b'\n'),
        _ if s.len() == 1 => Some(s.as_bytes()[0]),
        _ => None,
    }
}

/// `LOAD DATA LOCAL INFILE 'f' INTO TABLE t [FIELDS TERMINATED BY ','] [ENCLOSED BY '"'] [IGNORE n LINES]`
/// Returns None if the statement is not a LOAD DATA LOCAL INFILE at all, and
/// Some(Err) if it is one but uses something unsupported.
pub fn parse_load_data(query: &str) -> Option<Result<LoadOpts, String>> {
    let q = query.trim().trim_end_matches(';');
    let tokens: Vec<&str> = q.split_whitespace().collect();
    let up: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    if up.len() < 4 || up[0] != "LOAD" || up[1] != "DATA" {
        return None;
    }
    if up[2] != "LOCAL" || up[3] != "INFILE" {
        return Some(Err("only LOAD DATA LOCAL INFILE is supported".into()));
    }
    // Strip exactly one pair of surrounding quotes, so that '"' stays a double quote.
    let unq = |s: &str| {
        let b = s.as_bytes();
        if b.len() >= 2 && (b[0] == b'\'' || b[0] == b'"') && b[b.len() - 1] == b[0] {
            s[1..s.len() - 1].to_string()
        } else {
            s.to_string()
        }
    };
    let Some(file) = tokens.get(4).map(|t| unq(t)) else {
        return Some(Err("missing file name".into()));
    };
    let Some(into) = up.iter().position(|t| t == "TABLE") else {
        return Some(Err("missing INTO TABLE".into()));
    };
    // Between the file name and INTO TABLE only REPLACE / IGNORE may appear: both are refused,
    // because a table can be loaded exactly once.
    if up.get(5..into).unwrap_or(&[]).iter().any(|t| t == "REPLACE" || t == "IGNORE") {
        return Some(Err("REPLACE / IGNORE are not supported: a table can be loaded only once".into()));
    }
    let Some(table) = tokens.get(into + 1).map(|t| t.trim_matches('`').to_ascii_lowercase()) else {
        return Some(Err("missing table name".into()));
    };
    let mut o = LoadOpts { format: Format::Csv, file, table, term: b'\t', enclosed: None, ignore_lines: 0 };
    let mut i = into + 2;
    while i < tokens.len() {
        match up[i].as_str() {
            "TERMINATED" if up.get(i + 1).map(String::as_str) == Some("BY") => {
                let Some(d) = tokens.get(i + 2).and_then(|t| unescape_delim(&unq(t))) else {
                    return Some(Err("unsupported TERMINATED BY".into()));
                };
                o.term = d;
                i += 3;
            }
            "ENCLOSED" if up.get(i + 1).map(String::as_str) == Some("BY") => {
                let Some(d) = tokens.get(i + 2).and_then(|t| unescape_delim(&unq(t))) else {
                    return Some(Err("unsupported ENCLOSED BY".into()));
                };
                o.enclosed = Some(d);
                i += 3;
            }
            "FORMAT" => {
                match tokens.get(i + 1).map(|t| unq(t).to_ascii_uppercase()).as_deref() {
                    Some("ARROW") => o.format = Format::Arrow,
                    Some(other) => return Some(Err(format!("unsupported FORMAT '{other}' (only ARROW)"))),
                    None => return Some(Err("FORMAT needs a value (ARROW)".into())),
                }
                i += 2;
            }
            "IGNORE" => {
                let n = tokens.get(i + 1).and_then(|t| t.parse::<usize>().ok());
                match n {
                    Some(n) => o.ignore_lines = n,
                    None => return Some(Err("IGNORE needs a number of LINES".into())),
                }
                i += 3;
            }
            _ => i += 1, // FIELDS, OPTIONALLY, LINES ... are accepted and ignored
        }
    }
    Some(Ok(o))
}

pub struct LineParser {
    term: u8,
    enclosed: Option<u8>,
    to_skip: usize,
    carry: Vec<u8>,
}

type Row<'a> = Vec<Option<&'a [u8]>>;

impl LineParser {
    pub fn new(o: &LoadOpts) -> Self {
        LineParser { term: o.term, enclosed: o.enclosed, to_skip: o.ignore_lines, carry: Vec::new() }
    }

    /// Feeds the next chunk; `f` is called once per complete data line.
    pub fn feed(&mut self, mut data: &[u8], f: &mut dyn FnMut(&[Option<&[u8]>]) -> Result<(), String>) -> Result<(), String> {
        while let Some(i) = data.iter().position(|&b| b == b'\n') {
            if self.carry.is_empty() {
                self.line(&data[..i], f)?;
            } else {
                self.carry.extend_from_slice(&data[..i]);
                let line = std::mem::take(&mut self.carry);
                self.line(&line, f)?;
                self.carry = line;
                self.carry.clear();
            }
            data = &data[i + 1..];
        }
        self.carry.extend_from_slice(data);
        Ok(())
    }

    /// The file may end without a final newline.
    pub fn finish(&mut self, f: &mut dyn FnMut(&[Option<&[u8]>]) -> Result<(), String>) -> Result<(), String> {
        if self.carry.is_empty() {
            return Ok(());
        }
        let line = std::mem::take(&mut self.carry);
        self.line(&line, f)
    }

    fn line(&mut self, line: &[u8], f: &mut dyn FnMut(&[Option<&[u8]>]) -> Result<(), String>) -> Result<(), String> {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if self.to_skip > 0 {
            self.to_skip -= 1;
            return Ok(());
        }
        if line.is_empty() {
            return Ok(());
        }
        let mut row: Row = Vec::new();
        for field in line.split(|&b| b == self.term) {
            let field = match self.enclosed {
                Some(q) if field.len() >= 2 && field[0] == q && field[field.len() - 1] == q => {
                    &field[1..field.len() - 1]
                }
                _ => field,
            };
            row.push(if field == b"\\N" { None } else { Some(field) });
        }
        f(&row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(q: &str) -> LoadOpts {
        parse_load_data(q).unwrap().unwrap()
    }

    #[test]
    fn parses_options() {
        let o = opts("LOAD DATA LOCAL INFILE 'a.csv' INTO TABLE `t` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES");
        assert_eq!((o.file.as_str(), o.table.as_str(), o.term, o.enclosed, o.ignore_lines), ("a.csv", "t", b',', Some(b'"'), 1));
        assert_eq!(o.format, Format::Csv);
        let a = opts("LOAD DATA LOCAL INFILE 'a.arrows' INTO TABLE t FORMAT ARROW");
        assert_eq!(a.format, Format::Arrow);
        assert!(parse_load_data("LOAD DATA LOCAL INFILE 'a' INTO TABLE t FORMAT PARQUET").unwrap().is_err());
        assert!(parse_load_data("LOAD DATA LOCAL INFILE 'a' REPLACE INTO TABLE t").unwrap().is_err());
        assert!(parse_load_data("SELECT 1").is_none());
    }

    #[test]
    fn lines_survive_chunk_boundaries() {
        let o = opts("LOAD DATA LOCAL INFILE 'a' INTO TABLE t FIELDS TERMINATED BY ','");
        let mut p = LineParser::new(&o);
        let mut got: Vec<Vec<Option<String>>> = vec![];
        let mut f = |r: &[Option<&[u8]>]| {
            got.push(r.iter().map(|x| x.map(|b| String::from_utf8_lossy(b).into_owned())).collect());
            Ok(())
        };
        for chunk in [&b"1,al"[..], b"ice\n2,\\N\r\n3,c", b"\n4,d"] {
            p.feed(chunk, &mut f).unwrap();
        }
        p.finish(&mut f).unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0], vec![Some("1".into()), Some("alice".into())]);
        assert_eq!(got[1], vec![Some("2".into()), None]);
        assert_eq!(got[3], vec![Some("4".into()), Some("d".into())]);
    }
}
