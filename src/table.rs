//! Table definition, column layout inside the arena, and row storage.
//!
//! Layout per column (all offsets are inside the arena, no pointers):
//!   validity : ceil(rows/8) bytes, Arrow convention (bit set = value present)
//!   values   : fixed width: rows * width;  VARCHAR: (rows+1) * 4 byte offsets
//!   data     : VARCHAR only, the concatenated bytes (capacity from BYTES_PER_ROW_HINT)

use std::ops::Range;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use datafusion::arrow::array::{make_array, ArrayData, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;

use crate::arena::Arena;
use crate::values;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ColType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Double,
    Decimal { precision: u8, scale: i8 },
    Date,
    DateTime,
    Varchar,
}

impl ColType {
    /// Parses a MySQL type such as `DECIMAL(10,2)` or `BIGINT UNSIGNED`.
    /// UNSIGNED and display widths are accepted and ignored (values must still fit the signed type).
    pub fn parse(s: &str) -> Option<ColType> {
        let s: String = s.split_whitespace().collect::<Vec<_>>().join("");
        let upper = s.to_ascii_uppercase();
        let base = upper.split('(').next()?;
        let args: Vec<i64> = upper
            .split_once('(')
            .and_then(|(_, r)| r.split_once(')'))
            .map(|(a, _)| a.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_default();
        match base.trim_end_matches("UNSIGNED") {
            "TINYINT" => Some(ColType::TinyInt),
            "SMALLINT" => Some(ColType::SmallInt),
            "INT" | "INTEGER" | "MEDIUMINT" => Some(ColType::Int),
            "BIGINT" => Some(ColType::BigInt),
            "DOUBLE" | "FLOAT" => Some(ColType::Double),
            "DECIMAL" | "NUMERIC" => {
                let precision = *args.first().unwrap_or(&10);
                let scale = *args.get(1).unwrap_or(&0);
                if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
                    return None;
                }
                Some(ColType::Decimal { precision: precision as u8, scale: scale as i8 })
            }
            "DATE" => Some(ColType::Date),
            "DATETIME" | "TIMESTAMP" => Some(ColType::DateTime),
            "VARCHAR" | "CHAR" | "TEXT" | "LONGTEXT" | "MEDIUMTEXT" => Some(ColType::Varchar),
            _ => None,
        }
    }

    fn width(self) -> usize {
        match self {
            ColType::TinyInt => 1,
            ColType::SmallInt => 2,
            ColType::Int | ColType::Date | ColType::Varchar => 4,
            ColType::BigInt | ColType::Double | ColType::DateTime => 8,
            ColType::Decimal { .. } => 16,
        }
    }

    pub fn arrow(self) -> DataType {
        match self {
            ColType::TinyInt => DataType::Int8,
            ColType::SmallInt => DataType::Int16,
            ColType::Int => DataType::Int32,
            ColType::BigInt => DataType::Int64,
            ColType::Double => DataType::Float64,
            ColType::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
            ColType::Date => DataType::Date32,
            ColType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            ColType::Varchar => DataType::Utf8,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColType,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ColumnLayout {
    pub validity: Range<usize>,
    pub values: Range<usize>,
    pub data: Range<usize>, // empty for fixed-width columns
    pub(crate) data_used: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum State {
    /// Space reserved, no data yet. LOAD DATA is allowed.
    Created,
    /// A LOAD DATA is streaming into it.
    Loading,
    /// Complete and immutable.
    Loaded,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub cols: Vec<ColumnDef>,
    pub layout: Vec<ColumnLayout>,
    pub rows_expected: usize,
    pub rows: usize,
    pub state: State,
}

impl Table {
    /// Reserves all the space the table will ever use.
    pub fn create(
        arena: &mut Arena,
        name: String,
        cols: Vec<ColumnDef>,
        rows: usize,
        bytes_per_row_hint: usize,
    ) -> Result<Table, String> {
        let mut layout = Vec::new();
        for c in &cols {
            let validity = arena.alloc(rows.div_ceil(8)).map_err(|e| e.to_string())?;
            let (values, data) = if c.ty == ColType::Varchar {
                let v = arena.alloc((rows + 1) * 4).map_err(|e| e.to_string())?;
                let d = arena.alloc(rows * bytes_per_row_hint).map_err(|e| e.to_string())?;
                (v, d)
            } else {
                let v = arena.alloc(rows * c.ty.width()).map_err(|e| e.to_string())?;
                (v, 0..0)
            };
            layout.push(ColumnLayout { validity, values, data, data_used: 0 });
        }
        Ok(Table { name, cols, layout, rows_expected: rows, rows: 0, state: State::Created })
    }

    /// Appends one row. `fields[i] == None` is SQL NULL.
    pub fn push_row(&mut self, arena: &mut Arena, fields: &[Option<&[u8]>]) -> Result<(), String> {
        if fields.len() != self.cols.len() {
            return Err(format!(
                "row {} has {} fields, table has {} columns",
                self.rows + 1,
                fields.len(),
                self.cols.len()
            ));
        }
        if self.rows >= self.rows_expected {
            return Err(format!("more rows than the declared ROWS {}", self.rows_expected));
        }
        let r = self.rows;
        for (i, f) in fields.iter().enumerate() {
            let ty = self.cols[i].ty;
            let name = self.cols[i].name.clone();
            let lay = &mut self.layout[i];
            let bad = |what: &str| format!("row {}: column {name}: invalid {what}", r + 1);

            if ty == ColType::Varchar {
                let bytes = f.unwrap_or(&[]);
                if lay.data_used + bytes.len() > lay.data.len() {
                    return Err(format!(
                        "row {}: column {name}: VARCHAR data exceeds the space reserved by BYTES_PER_ROW_HINT",
                        r + 1
                    ));
                }
                let start = lay.data.start + lay.data_used;
                arena.slice_mut(&(start..start + bytes.len())).copy_from_slice(bytes);
                lay.data_used += bytes.len();
                let off = lay.values.start + (r + 1) * 4;
                arena.slice_mut(&(off..off + 4)).copy_from_slice(&(lay.data_used as u32).to_le_bytes());
            } else if let Some(bytes) = f {
                let text = std::str::from_utf8(bytes).map_err(|_| bad("UTF-8"))?.trim();
                let w = ty.width();
                let off = lay.values.start + r * w;
                let dst = arena.slice_mut(&(off..off + w));
                let ctx = |e: String| format!("row {}: column {name}: {e}", r + 1);
                match ty {
                    ColType::TinyInt => dst.copy_from_slice(&text.parse::<i8>().map_err(|_| bad("TINYINT"))?.to_le_bytes()),
                    ColType::SmallInt => dst.copy_from_slice(&text.parse::<i16>().map_err(|_| bad("SMALLINT"))?.to_le_bytes()),
                    ColType::Int => dst.copy_from_slice(&text.parse::<i32>().map_err(|_| bad("INT"))?.to_le_bytes()),
                    ColType::BigInt => dst.copy_from_slice(&text.parse::<i64>().map_err(|_| bad("BIGINT"))?.to_le_bytes()),
                    ColType::Double => dst.copy_from_slice(&text.parse::<f64>().map_err(|_| bad("DOUBLE"))?.to_le_bytes()),
                    ColType::Decimal { precision, scale } => {
                        dst.copy_from_slice(&values::parse_decimal(text, precision, scale).map_err(ctx)?.to_le_bytes())
                    }
                    ColType::Date => dst.copy_from_slice(&values::parse_date(text).map_err(ctx)?.to_le_bytes()),
                    ColType::DateTime => dst.copy_from_slice(&values::parse_datetime(text).map_err(ctx)?.to_le_bytes()),
                    ColType::Varchar => unreachable!(),
                }
            }
            if f.is_some() {
                let byte = lay.validity.start + r / 8;
                arena.slice_mut(&(byte..byte + 1))[0] |= 1 << (r % 8);
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Arrow view of the finished table. Zero-copy: every buffer points into the arena.
    /// `build()` validates offsets and UTF-8 once, so bad data fails the load, not a later query.
    pub fn to_record_batch(&self, arena: &Arena) -> Result<RecordBatch, String> {
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (c, l) in self.cols.iter().zip(&self.layout) {
            let dt = c.ty.arrow();
            let mut b = ArrayData::builder(dt.clone())
                .len(self.rows)
                .null_bit_buffer(Some(arena.buffer(&l.validity)))
                .add_buffer(arena.buffer(&l.values));
            if c.ty == ColType::Varchar {
                b = b.add_buffer(arena.buffer(&(l.data.start..l.data.start + l.data_used)));
            }
            let data = b.build().map_err(|e| format!("column {}: {e}", c.name))?;
            fields.push(Field::new(&c.name, dt, true));
            arrays.push(make_array(data));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| e.to_string())
    }

    /// Bytes reserved for this table in the arena.
    pub fn reserved_bytes(&self) -> usize {
        self.layout
            .iter()
            .map(|l| l.validity.len() + l.values.len() + l.data.len())
            .sum()
    }

    /// Wipes a half-loaded table so the LOAD DATA can be retried.
    pub fn reset(&mut self, arena: &mut Arena) {
        for l in &mut self.layout {
            arena.slice_mut(&l.validity).fill(0);
            arena.slice_mut(&l.values).fill(0);
            arena.slice_mut(&l.data).fill(0);
            l.data_used = 0;
        }
        self.rows = 0;
        self.state = State::Created;
    }

    pub fn read_i64(&self, arena: &Arena, col: usize, row: usize) -> Option<i64> {
        let l = &self.layout[col];
        if arena.slice(&l.validity)[row / 8] >> (row % 8) & 1 == 0 {
            return None;
        }
        let off = l.values.start + row * 8;
        Some(i64::from_le_bytes(arena.slice(&(off..off + 8)).try_into().unwrap()))
    }

    pub fn read_str<'a>(&self, arena: &'a Arena, col: usize, row: usize) -> Option<&'a str> {
        let l = &self.layout[col];
        if arena.slice(&l.validity)[row / 8] >> (row % 8) & 1 == 0 {
            return None;
        }
        let o = |i: usize| {
            let off = l.values.start + i * 4;
            u32::from_le_bytes(arena.slice(&(off..off + 4)).try_into().unwrap()) as usize
        };
        let (a, b) = (o(row), o(row + 1));
        std::str::from_utf8(arena.slice(&(l.data.start + a..l.data.start + b))).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<ColumnDef> {
        vec![
            ColumnDef { name: "id".into(), ty: ColType::BigInt },
            ColumnDef { name: "name".into(), ty: ColType::Varchar },
        ]
    }

    #[test]
    fn rows_roundtrip_with_nulls() {
        let mut a = Arena::new(1 << 16).unwrap();
        let mut t = Table::create(&mut a, "t".into(), cols(), 3, 8).unwrap();
        t.push_row(&mut a, &[Some(b"1"), Some(b"alice")]).unwrap();
        t.push_row(&mut a, &[None, Some(b"bob")]).unwrap();
        t.push_row(&mut a, &[Some(b"3"), None]).unwrap();
        assert_eq!(t.read_i64(&a, 0, 0), Some(1));
        assert_eq!(t.read_i64(&a, 0, 1), None);
        assert_eq!(t.read_str(&a, 1, 1), Some("bob"));
        assert_eq!(t.read_str(&a, 1, 2), None);
        assert!(t.push_row(&mut a, &[Some(b"4"), Some(b"x")]).is_err()); // over ROWS
    }

    #[test]
    fn varchar_overflow_and_bad_number_are_errors() {
        let mut a = Arena::new(1 << 16).unwrap();
        let mut t = Table::create(&mut a, "t".into(), cols(), 2, 2).unwrap();
        assert!(t.push_row(&mut a, &[Some(b"x"), Some(b"a")]).is_err());
        assert!(t.push_row(&mut a, &[Some(b"1"), Some(b"waytoolong")]).is_err());
    }

    #[test]
    fn table_too_big_for_arena_fails() {
        let mut a = Arena::new(1024).unwrap();
        assert!(Table::create(&mut a, "t".into(), cols(), 1_000_000, 8).is_err());
    }
}
