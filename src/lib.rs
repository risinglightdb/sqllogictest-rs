//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use itertools::Itertools;
use log::*;
use tempfile::{tempdir, TempDir};
use async_trait::async_trait;

/// A single directive in a sqllogictest file.
#[derive(Debug, PartialEq, Clone)]
pub enum Record {
    /// A statement is an SQL command that is to be evaluated but from which we do not expect to
    /// get results (other than success or failure).
    Statement {
        line: u32,
        conditions: Vec<Condition>,
        /// The SQL command is expected to fail instead of to succeed.
        error: bool,
        /// The SQL command.
        sql: String,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        line: u32,
        conditions: Vec<Condition>,
        type_string: String,
        sort_mode: SortMode,
        label: Option<String>,
        /// The SQL command.
        sql: String,
        /// The expected results.
        expected_results: String,
    },
    /// Subtest.
    Subtest { name: String },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt,
}

/// The condition to run a query.
#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    /// The statement or query is skipped if an `onlyif` record for a different database engine is
    /// seen.
    OnlyIf { db_name: String },
    /// The statement or query is not evaluated if a `skipif` record for the target database engine
    /// is seen in the prefix.
    SkipIf { db_name: String },
}

/// Whether to apply sorting before checking the results of a query.
#[derive(Debug, PartialEq, Clone)]
pub enum SortMode {
    /// The default option. The results appear in exactly the order in which they were received
    /// from the database engine.
    NoSort,
    /// Gathers all output from the database engine then sorts it by rows.
    RowSort,
    /// It works like rowsort except that it does not honor row groupings. Each individual result
    /// value is sorted on its own.
    ValueSort,
}

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Clone)]
#[error("parse error at line {line}: {kind}")]
pub struct Error {
    kind: ErrorKind,
    line: u32,
}

impl Error {
    /// Returns the corresponding [`ErrorKind`] for this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind.clone()
    }

    /// Returns the line number from which the error originated.
    pub fn line(&self) -> u32 {
        self.line
    }
}

/// The error kind for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Clone)]
pub enum ErrorKind {
    #[error("unexpected token: {0:?}")]
    UnexpectedToken(String),
    #[error("unexpected EOF")]
    UnexpectedEOF,
    #[error("invalid sort mode: {0:?}")]
    InvalidSortMode(String),
    #[error("invalid line: {0:?}")]
    InvalidLine(String),
    #[error("invalid type string: {0:?}")]
    InvalidType(String),
}

impl ErrorKind {
    fn at(self, line: usize) -> Error {
        Error {
            kind: self,
            line: line as u32,
        }
    }
}

/// Parse a sqllogictest script into a list of records.
pub fn parse(script: &str) -> Result<Vec<Record>, Error> {
    let mut lines = script.split('\n').enumerate();
    let mut records = vec![];
    let mut conditions = vec![];
    while let Some((num, line)) = lines.next() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["halt"] => {
                records.push(Record::Halt);
                break;
            }
            ["subtest", name] => {
                records.push(Record::Subtest {
                    name: name.to_string(),
                });
            }
            ["skipif", db_name] => {
                conditions.push(Condition::SkipIf {
                    db_name: db_name.to_string(),
                });
            }
            ["onlyif", db_name] => {
                conditions.push(Condition::OnlyIf {
                    db_name: db_name.to_string(),
                });
            }
            &["statement", res] => {
                let line = num as u32;
                let error = match res {
                    "ok" => false,
                    "error" => true,
                    _ => return Err(ErrorKind::UnexpectedToken(res.into()).at(num)),
                };
                let mut sql = lines
                    .next()
                    .ok_or_else(|| ErrorKind::UnexpectedEOF.at(num + 1))?
                    .1
                    .into();
                for (_, line) in &mut lines {
                    if line.is_empty() {
                        break;
                    }
                    sql += line;
                }
                records.push(Record::Statement {
                    line,
                    conditions: std::mem::take(&mut conditions),
                    error,
                    sql,
                });
            }
            ["query", type_string, res @ ..] => {
                let line = num as u32;
                let sort_mode = match res.get(0) {
                    None | Some(&"nosort") => SortMode::NoSort,
                    Some(&"rowsort") => SortMode::RowSort,
                    Some(&"valuesort") => SortMode::ValueSort,
                    Some(mode) => return Err(ErrorKind::InvalidSortMode(mode.to_string()).at(num)),
                };
                let label = res.get(1).map(|s| s.to_string());
                // The SQL for the query is found on second an subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let mut sql = lines
                    .next()
                    .ok_or_else(|| ErrorKind::UnexpectedEOF.at(num + 1))?
                    .1
                    .into();
                let mut has_result = false;
                for (_, line) in &mut lines {
                    if line.is_empty() || line == "----" {
                        has_result = line == "----";
                        break;
                    }
                    sql += line;
                }
                // Lines following the "----" are expected results of the query, one value per line.
                let mut expected_results = String::new();
                if has_result {
                    for (_, line) in &mut lines {
                        if line.is_empty() {
                            break;
                        }
                        expected_results += line;
                        expected_results.push('\n');
                    }
                }
                records.push(Record::Query {
                    line,
                    conditions: std::mem::take(&mut conditions),
                    type_string: type_string.to_string(),
                    sort_mode,
                    label,
                    sql,
                    expected_results,
                });
            }
            _ => return Err(ErrorKind::InvalidLine(line.into()).at(num)),
        }
    }
    Ok(records)
}

/// The async database to be tested.
#[async_trait]
pub trait AsyncDB {
    /// The error type of SQL execution.
    type Error: std::error::Error;

    /// Async run a SQL query and return the output.
    async fn run(&self, sql: &str) -> Result<String, Self::Error>;
}

/// The database to be tested.
pub trait DB {
    /// The error type of SQL execution.
    type Error: std::error::Error;

    /// Run a SQL query and return the output.
    fn run(&self, sql: &str) -> Result<String, Self::Error>;
}

/// Sqllogictest runner.
pub struct Runner<D: DB> {
    db: D,
    testdir: Option<TempDir>,
}

impl<D: DB> Runner<D> {
    /// Create a new test runner on the database.
    pub fn new(db: D) -> Self {
        Runner { db, testdir: None }
    }

    /// Replace the pattern `__TEST_DIR__` in SQL with a temporary directory path.
    ///
    /// This feature is useful in those tests where data will be written to local
    /// files, e.g. `COPY`.
    pub fn enable_testdir(&mut self) {
        self.testdir = Some(tempdir().expect("failed to create testdir"));
    }

    /// Run a single record.
    pub fn run(&mut self, record: Record) {
        info!("test: {:?}", record);
        match record {
            Record::Statement {
                error, sql, line, ..
            } => {
                let sql = self.replace_keywords(sql);
                let ret = self.db.run(&sql);
                match ret {
                    Ok(_) if error => panic!(
                        "line {}: statement is expected to fail, but actually succeed: {:?}",
                        line, sql
                    ),
                    Err(e) if !error => {
                        panic!("line {}: statement failed: {}\n\tSQL: {:?}", line, e, sql)
                    }
                    _ => {}
                }
            }
            Record::Query {
                line,
                sql,
                expected_results,
                sort_mode,
                ..
            } => {
                let sql = self.replace_keywords(sql);
                let output = match self.db.run(&sql) {
                    Ok(output) => output,
                    Err(e) => panic!("line {}: query failed: {}\nSQL: {}", line, e, sql),
                };
                let mut output = split_lines_and_normalize(&output);
                let mut expected_results = split_lines_and_normalize(&expected_results);
                match sort_mode {
                    SortMode::NoSort => {}
                    SortMode::RowSort => {
                        output.sort_unstable();
                        expected_results.sort_unstable();
                    }
                    SortMode::ValueSort => todo!("value sort"),
                };
                if output != expected_results {
                    panic!(
                        "line {}: query result mismatch:\nSQL:\n{}\n\nExpected:\n{}\nActual:\n{}",
                        line,
                        sql,
                        expected_results.join("\n"),
                        output.join("\n")
                    );
                }
            }
            Record::Halt => {}
            Record::Subtest { .. } => {}
        }
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    pub fn run_multi(&mut self, records: impl IntoIterator<Item = Record>) {
        for record in records.into_iter() {
            if let Record::Halt = record {
                return;
            }
            self.run(record);
        }
    }

    /// Run a sqllogictest script.
    pub fn run_script(&mut self, script: &str) {
        let records = parse(script).expect("failed to parse sqllogictest");
        self.run_multi(records);
    }

    /// Replace all keywords in the SQL.
    fn replace_keywords(&self, sql: String) -> String {
        if let Some(testdir) = &self.testdir {
            sql.replace("__TEST_DIR__", testdir.path().to_str().unwrap())
        } else {
            sql
        }
    }
}

/// Trim and replace multiple whitespaces with one.
fn normalize_string(s: &str) -> String {
    s.trim().split_ascii_whitespace().join(" ")
}

fn split_lines_and_normalize(s: &str) -> Vec<String> {
    s.split('\n')
        .map(normalize_string)
        .filter(|line| !line.is_empty())
        .collect()
}
