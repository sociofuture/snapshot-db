//! Text -> binary conversion for the column types that need real parsing.

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

fn num(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `YYYY-MM-DD` -> days since epoch (Arrow Date32).
pub fn parse_date(text: &str) -> Result<i32, String> {
    let bad = || format!("invalid DATE '{text}' (expected YYYY-MM-DD)");
    let mut it = text.split('-');
    let (Some(y), Some(m), Some(d), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(bad());
    };
    let (y, m, d) = (num(y).ok_or_else(bad)?, num(m).ok_or_else(bad)?, num(d).ok_or_else(bad)?);
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) || !(1..=9999).contains(&y) {
        return Err(bad());
    }
    Ok(days_from_civil(y, m, d) as i32)
}

/// `YYYY-MM-DD[ T]HH:MM:SS[.ffffff]` (time part optional) -> microseconds since epoch.
pub fn parse_datetime(text: &str) -> Result<i64, String> {
    let bad = || format!("invalid DATETIME '{text}' (expected YYYY-MM-DD HH:MM:SS[.ffffff])");
    let (date, time) = match text.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (text, None),
    };
    let days = parse_date(date).map_err(|_| bad())? as i64;
    let mut micros = 0i64;
    if let Some(t) = time {
        let (hms, frac) = match t.split_once('.') {
            Some((h, f)) => (h, Some(f)),
            None => (t, None),
        };
        let mut it = hms.split(':');
        let (Some(h), Some(mi), Some(s), None) = (it.next(), it.next(), it.next(), it.next()) else {
            return Err(bad());
        };
        let (h, mi, s) = (num(h).ok_or_else(bad)?, num(mi).ok_or_else(bad)?, num(s).ok_or_else(bad)?);
        if h > 23 || mi > 59 || s > 59 {
            return Err(bad());
        }
        micros = (h * 3600 + mi * 60 + s) * 1_000_000;
        if let Some(f) = frac {
            if f.is_empty() || f.len() > 6 || num(f).is_none() {
                return Err(bad());
            }
            micros += num(f).unwrap() * 10i64.pow(6 - f.len() as u32);
        }
    }
    Ok(days * 86_400_000_000 + micros)
}

/// `-123.45` with precision/scale -> the unscaled integer (Arrow Decimal128).
pub fn parse_decimal(text: &str, precision: u8, scale: i8) -> Result<i128, String> {
    let bad = |why: &str| format!("invalid DECIMAL({precision},{scale}) '{text}': {why}");
    let (neg, body) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    let digits_ok = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if (int.is_empty() && frac.is_empty()) || !digits_ok(int) || !digits_ok(frac) {
        return Err(bad("not a number"));
    }
    let scale = scale.max(0) as usize;
    // Extra fractional digits are accepted only when they are zeros (no silent rounding).
    let (keep, extra) = frac.split_at(frac.len().min(scale));
    if extra.bytes().any(|b| b != b'0') {
        return Err(bad("more fractional digits than the scale"));
    }
    let mut digits = String::from(int.trim_start_matches('0'));
    digits.push_str(keep);
    digits.extend(std::iter::repeat('0').take(scale - keep.len()));
    if digits.len() > precision as usize {
        return Err(bad("too many digits for the precision"));
    }
    let v: i128 = if digits.is_empty() { 0 } else { digits.parse().map_err(|_| bad("overflow"))? };
    Ok(if neg { -v } else { v })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(parse_date("1970-01-01"), Ok(0));
        assert_eq!(parse_date("2000-03-01"), Ok(11017));
        assert_eq!(parse_date("1969-12-31"), Ok(-1));
        assert!(parse_date("2026-02-29").is_err());
        assert!(parse_date("2024-02-29").is_ok());
        assert!(parse_date("0000-00-00").is_err());
        assert!(parse_date("2026-13-01").is_err());
    }

    #[test]
    fn datetimes() {
        assert_eq!(parse_datetime("1970-01-01 00:00:01.5"), Ok(1_500_000));
        assert_eq!(parse_datetime("1970-01-02"), Ok(86_400_000_000));
        assert_eq!(parse_datetime("1970-01-01T01:00:00"), Ok(3_600_000_000));
        assert!(parse_datetime("1970-01-01 25:00:00").is_err());
        assert!(parse_datetime("1970-01-01 10:00").is_err());
    }

    #[test]
    fn decimals() {
        assert_eq!(parse_decimal("123.45", 10, 2), Ok(12345));
        assert_eq!(parse_decimal("-0.5", 10, 2), Ok(-50));
        assert_eq!(parse_decimal("7", 10, 2), Ok(700));
        assert_eq!(parse_decimal("1.500", 10, 2), Ok(150)); // trailing zeros are fine
        assert!(parse_decimal("1.234", 10, 2).is_err()); // would round
        assert!(parse_decimal("123456789.00", 10, 2).is_err()); // too many digits
        assert!(parse_decimal("abc", 10, 2).is_err());
        assert_eq!(parse_decimal("0", 5, 0), Ok(0));
    }
}
