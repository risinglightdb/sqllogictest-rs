use std::fmt;
use std::fmt::Write;
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use futures::{pin_mut, StreamExt};
use pg_interval::Interval;
use postgres_types::{accepts, FromSql, ToSql, Type};
use rust_decimal::Decimal;
use sqllogictest::{DBOutput, DefaultColumnType};

use crate::postgres::error::PgDriverError;

use super::{Extended, Postgres, Result};

// Inspired by postgres_type::Array implementation of Display trait
fn print_array<T: std::fmt::Display>(
    arr: &postgres_array::Array<Option<T>>,
    fmt: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    print_array_helper(0, arr.dimensions(), &mut arr.iter(), fmt)
}

// See https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
// The array output routine will put double quotes around element value if it
// * is empty string
// * equals to NULL (case insensitive)
// Or contains
// * curly braces
// * delimiter characters(comma)
// * double quotes
// * backslashes
// * space
// It'is used (although it's simple protocol specific) to not duplicate tests for simple and extended protocols.
pub fn array_item_need_escape_and_quote(data: &str) -> bool {
    if data.is_empty() || data.eq_ignore_ascii_case("null") {
        return true;
    }

    data.chars()
        .any(|c| matches!(c, '{' | '}' | ',' | '"' | '\\') || c.is_ascii_whitespace())
}

pub fn escape_and_quote(input: &str) -> String {
    debug_assert!(array_item_need_escape_and_quote(input));
    let mut response = String::with_capacity(input.len());
    response.push('"');

    for c in input.chars() {
        if matches!(c, '"' | '\\') {
            response.push('\\');
        }
        response.push(c);
    }
    response.push('"');
    response
}

fn print_array_helper<'a, T: std::fmt::Display + 'a, I: Iterator<Item = &'a Option<T>>>(
    depth: usize,
    dims: &[postgres_array::Dimension],
    data: &mut I,
    fmt: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    if dims.is_empty() {
        return write!(fmt, "{{}}");
    }

    if depth == dims.len() {
        return match data.next().unwrap() {
            Some(value) => {
                let mut item = String::new();
                write!(item, "{}", value)?;
                if array_item_need_escape_and_quote(&item) {
                    item = escape_and_quote(&item);
                }
                assert!(!item.is_empty());
                fmt.write_str(&item)
            }
            None => write!(fmt, "NULL"),
        };
    }

    write!(fmt, "{{")?;
    for i in 0..dims[depth].len {
        if i != 0 {
            write!(fmt, ",")?;
        }
        print_array_helper(depth + 1, dims, data, fmt)?;
    }
    write!(fmt, "}}")
}

struct ArrayFmt<'a, T>(&'a postgres_array::Array<Option<T>>);

impl<'a, T: std::fmt::Display> std::fmt::Display for ArrayFmt<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        print_array(self.0, f)
    }
}

#[derive(Debug)]
struct JsonPreservedValue {
    payload: String,
}

impl<'a> FromSql<'a> for JsonPreservedValue {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(JsonPreservedValue {
            payload: std::str::from_utf8(raw)?.to_string(),
        })
    }

    accepts!(JSON);
}

impl fmt::Display for JsonPreservedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.payload)
    }
}

// It's required to use postgres_array::Array instead of Vec.
// See: https://github.com/rust-postgres/rust-postgres/issues/1186
macro_rules! array_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<postgres_array::Array<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let dimensions: Vec<postgres_array::Dimension> = value.dimensions().into();
                let data: Vec<Option<String>> = value
                    .into_iter()
                    .map(|opt| opt.map(|v| format!("{}", v)))
                    .collect();
                let value = postgres_array::Array::from_parts(data, dimensions);
                let value = ArrayFmt(&value);
                let mut output = String::new();
                write!(output, "{value}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, $convert:ident) => {
        let value: Option<postgres_array::Array<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let dimensions: Vec<postgres_array::Dimension> = value.dimensions().into();
                let data: Vec<Option<String>> = value
                    .into_iter()
                    .map(|opt| opt.map(|v| $convert(&v).to_string()))
                    .collect();
                let value = postgres_array::Array::from_parts(data, dimensions);
                let value = ArrayFmt(&value);
                let mut output = String::new();
                write!(output, "{value}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($self:ident, $row:ident, $row_vec:ident, $idx:ident, $t:ty, $ty_name:expr) => {
        let value: Option<postgres_array::Array<Option<$t>>> = $row.get($idx);
        match value {
            Some(value) => {
                let dimensions: Vec<postgres_array::Dimension> = value.dimensions().into();
                let mut data = Vec::<Option<String>>::new();
                for v in value.iter() {
                    match v {
                        Some(v) => {
                            let sql = format!("select ($1::{})::varchar", stringify!($ty_name));
                            let tmp_rows = $self.client().query(&sql, &[&v]).await.unwrap();
                            let value: &str = tmp_rows.get(0).unwrap().get(0);
                            assert!(value.len() > 0);
                            data.push(Some(value.to_string()));
                        }
                        None => {
                            data.push(Some("NULL".to_string()));
                        }
                    }
                }
                let value = postgres_array::Array::from_parts(data, dimensions);
                let value = ArrayFmt(&value);
                let mut output = String::new();
                write!(output, "{value}").unwrap();
                $row_vec.push(output);
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
}

fn placeholder_if_empty(value: String) -> String {
    if value.is_empty() {
        "(empty)".into()
    } else {
        value
    }
}

macro_rules! single_process {
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                $row_vec.push(placeholder_if_empty(value.to_string()));
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($row:ident, $row_vec:ident, $idx:ident, $t:ty, $convert:ident) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                $row_vec.push(placeholder_if_empty($convert(&value).to_string()));
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
    ($self:ident, $row:ident, $row_vec:ident, $idx:ident, $t:ty, $ty_name:expr) => {
        let value: Option<$t> = $row.get($idx);
        match value {
            Some(value) => {
                let sql = format!("select ($1::{})::varchar", stringify!($ty_name));
                let tmp_rows = $self.client().query(&sql, &[&value]).await.unwrap();
                let value: &str = tmp_rows.get(0).unwrap().get(0);
                assert!(value.len() > 0);
                $row_vec.push(placeholder_if_empty(value.to_string()));
            }
            None => {
                $row_vec.push("NULL".to_string());
            }
        }
    };
}

fn bool_to_str(value: &bool) -> &'static str {
    if *value {
        "t"
    } else {
        "f"
    }
}

fn bytea_to_str(value: &[u8]) -> String {
    // It assumes that 'BYTEA_OUTPUT' variable is set to 'hex' (default value)
    let mut result = String::with_capacity("\\x".len() + 2 * value.len());
    result.push_str("\\x");
    for &b in value {
        result.push_str(&format!("{:02x}", b));
    }
    result
}

fn float4_to_str(value: &f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if *value == f32::INFINITY {
        "Infinity".to_string()
    } else if *value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        value.to_string()
    }
}

fn float8_to_str(value: &f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if *value == f64::INFINITY {
        "Infinity".to_string()
    } else if *value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        value.to_string()
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres<Extended> {
    type Error = PgDriverError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        let mut output = vec![];

        let stmt = self.client().prepare(sql).await?;
        let rows = self
            .client()
            .query_raw(&stmt, std::iter::empty::<&(dyn ToSql + Sync)>())
            .await?;

        pin_mut!(rows);

        let column_names_row: Vec<String> =
            stmt.columns().iter().map(|s| s.name().into()).collect();
        output.push(column_names_row);

        while let Some(row) = rows.next().await {
            let row = row?;
            let mut row_vec = vec![];

            for (idx, column) in row.columns().iter().enumerate() {
                match column.type_().clone() {
                    Type::INT2 => {
                        single_process!(row, row_vec, idx, i16);
                    }
                    Type::INT2_ARRAY => {
                        array_process!(row, row_vec, idx, i16);
                    }
                    Type::INT4 => {
                        single_process!(row, row_vec, idx, i32);
                    }
                    Type::INT4_ARRAY => {
                        array_process!(row, row_vec, idx, i32);
                    }
                    Type::INT8 => {
                        single_process!(row, row_vec, idx, i64);
                    }
                    Type::INT8_ARRAY => {
                        array_process!(row, row_vec, idx, i64);
                    }
                    Type::NUMERIC => {
                        single_process!(row, row_vec, idx, Decimal);
                    }
                    Type::NUMERIC_ARRAY => {
                        array_process!(row, row_vec, idx, Decimal);
                    }
                    Type::DATE => {
                        single_process!(row, row_vec, idx, NaiveDate);
                    }
                    Type::DATE_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveDate);
                    }
                    Type::TIME => {
                        single_process!(row, row_vec, idx, NaiveTime);
                    }
                    Type::TIME_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveTime);
                    }

                    Type::TIMESTAMP => {
                        single_process!(row, row_vec, idx, NaiveDateTime);
                    }
                    Type::TIMESTAMP_ARRAY => {
                        array_process!(row, row_vec, idx, NaiveDateTime);
                    }
                    Type::BOOL => {
                        single_process!(row, row_vec, idx, bool, bool_to_str);
                    }
                    Type::BOOL_ARRAY => {
                        array_process!(row, row_vec, idx, bool, bool_to_str);
                    }
                    Type::FLOAT4 => {
                        single_process!(row, row_vec, idx, f32, float4_to_str);
                    }
                    Type::FLOAT4_ARRAY => {
                        array_process!(row, row_vec, idx, f32, float4_to_str);
                    }
                    Type::FLOAT8 => {
                        single_process!(row, row_vec, idx, f64, float8_to_str);
                    }
                    Type::FLOAT8_ARRAY => {
                        array_process!(row, row_vec, idx, f64, float8_to_str);
                    }
                    Type::VARCHAR | Type::TEXT | Type::BPCHAR => {
                        single_process!(row, row_vec, idx, &str);
                    }
                    Type::VARCHAR_ARRAY | Type::TEXT_ARRAY | Type::BPCHAR_ARRAY => {
                        array_process!(row, row_vec, idx, &str);
                    }
                    Type::INTERVAL => {
                        single_process!(self, row, row_vec, idx, Interval, INTERVAL);
                    }
                    Type::INTERVAL_ARRAY => {
                        array_process!(self, row, row_vec, idx, Interval, INTERVAL);
                    }
                    Type::TIMESTAMPTZ => {
                        single_process!(
                            self,
                            row,
                            row_vec,
                            idx,
                            DateTime<chrono::Utc>,
                            TIMESTAMPTZ
                        );
                    }
                    Type::TIMESTAMPTZ_ARRAY => {
                        array_process!(self, row, row_vec, idx, DateTime<chrono::Utc>, TIMESTAMPTZ);
                    }
                    Type::BYTEA => {
                        single_process!(row, row_vec, idx, &[u8], bytea_to_str);
                    }
                    Type::BYTEA_ARRAY => {
                        array_process!(row, row_vec, idx, &[u8], bytea_to_str);
                    }
                    Type::JSON => {
                        single_process!(row, row_vec, idx, JsonPreservedValue);
                    }
                    Type::JSON_ARRAY => {
                        array_process!(row, row_vec, idx, JsonPreservedValue);
                    }
                    Type::JSONB => {
                        single_process!(row, row_vec, idx, serde_json::Value);
                    }
                    Type::JSONB_ARRAY => {
                        array_process!(row, row_vec, idx, serde_json::Value);
                    }
                    _ => {
                        todo!("Don't support {} type now.", column.type_().name())
                    }
                }
            }
            output.push(row_vec);
        }

        if output.is_empty() {
            match rows.rows_affected() {
                Some(rows) => Ok(DBOutput::StatementComplete(rows)),
                None => Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Any; stmt.columns().len()],
                    rows: vec![],
                }),
            }
        } else {
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    async fn shutdown(&mut self) {
        self.shutdown().await;
    }

    fn engine_name(&self) -> &str {
        "postgres-extended"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: Command) -> std::io::Result<std::process::Output> {
        tokio::process::Command::from(command).output().await
    }

    fn error_sql_state(err: &Self::Error) -> Option<String> {
        err.code().map(|s| s.code().to_owned())
    }
}
