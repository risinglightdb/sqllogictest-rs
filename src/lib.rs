//! [Sqllogictest][Sqllogictest] parser and runner.
//!
//! [Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
//!
//! # Usage
//!
//! Implement [`DB`] trait for your database structure:
//!
//! ```ignore
//! struct Database {...}
//!
//! impl sqllogictest::DB for Database {
//!     type Error = ...;
//!     fn run(&self, sql: &str) -> Result<String, Self::Error> {
//!         ...
//!     }
//! }
//! ```
//!
//! Create a [`Runner`] on your database instance, and then run the script:
//!
//! ```ignore
//! let mut tester = sqllogictest::Runner::new(Database::new());
//! let script = std::fs::read_to_string("script.slt").unwrap();
//! tester.run_script(&script);
//! ```
//!
//! You can also parse the script and execute the records separately:
//!
//! ```ignore
//! let records = sqllogictest::parse(&script).unwrap();
//! for record in records {
//!     tester.run(record);
//! }
//! ```

use std::rc::Rc;
use std::time::Duration;
use std::{fmt, path::Path};

use async_trait::async_trait;
use itertools::Itertools;
use log::*;
use tempfile::{tempdir, TempDir};

const DEFAULT_FILENAME: &str = "<entry>";

/// The location in source file.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Location {
    file: Rc<str>,
    line: u32,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.line)
    }
}

impl Location {
    /// File path.
    pub fn file(&self) -> &str {
        &self.file
    }

    /// Line number.
    pub fn line(&self) -> u32 {
        self.line
    }

    fn new(file: impl Into<Rc<str>>, line: u32) -> Self {
        Self {
            file: file.into(),
            line,
        }
    }

    #[must_use]
    fn map_line(self, op: impl Fn(u32) -> u32) -> Self {
        Self {
            file: self.file,
            line: op(self.line),
        }
    }
}

/// A single directive in a sqllogictest file.
#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum Record {
    /// An include copies all records from another files.
    Include { loc: Location, filename: String },
    /// A statement is an SQL command that is to be evaluated but from which we do not expect to
    /// get results (other than success or failure).
    Statement {
        loc: Location,
        conditions: Vec<Condition>,
        /// The SQL command is expected to fail instead of to succeed.
        error: bool,
        /// The SQL command.
        sql: String,
        /// Expected rows affected.
        expected_count: Option<usize>,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        loc: Location,
        conditions: Vec<Condition>,
        type_string: String,
        sort_mode: SortMode,
        label: Option<String>,
        /// The SQL command.
        sql: String,
        /// The expected results.
        expected_results: String,
    },
    /// A sleep period.
    Sleep { loc: Location, duration: Duration },
    /// Subtest.
    Subtest { loc: Location, name: String },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt { loc: Location },
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
#[error("parse error at {loc}: {kind}")]
pub struct Error {
    kind: ErrorKind,
    loc: Location,
}

impl Error {
    /// Returns the corresponding [`ErrorKind`] for this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
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
    #[error("invalid number: {0:?}")]
    InvalidNumber(String),
    #[error("invalid duration: {0:?}")]
    InvalidDuration(String),
}

impl ErrorKind {
    fn at(self, pos: Location) -> Error {
        Error {
            kind: self,
            loc: pos,
        }
    }
}

/// Parse a sqllogictest script into a list of records.
pub fn parse(script: &str) -> Result<Vec<Record>, Error> {
    parse_inner(Rc::from(DEFAULT_FILENAME), script)
}

fn parse_inner(filename: Rc<str>, script: &str) -> Result<Vec<Record>, Error> {
    let mut lines = script.split('\n').enumerate();
    let mut records = vec![];
    let mut conditions = vec![];
    while let Some((num, line)) = lines.next() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let loc = Location::new(filename.clone(), num as u32 + 1);
        let tokens: Vec<&str> = line.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["include", included] => records.push(Record::Include {
                loc,
                filename: included.to_string(),
            }),
            ["halt"] => {
                records.push(Record::Halt { loc });
                break;
            }
            ["subtest", name] => {
                records.push(Record::Subtest {
                    loc,
                    name: name.to_string(),
                });
            }
            ["sleep", dur] => {
                records.push(Record::Sleep {
                    duration: humantime::parse_duration(dur)
                        .map_err(|_| ErrorKind::InvalidDuration(dur.to_string()).at(loc.clone()))?,
                    loc,
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
            ["statement", res @ ..] => {
                let mut expected_count = None;
                let error = match res {
                    ["ok"] => false,
                    ["error"] => true,
                    ["count", count_str] => {
                        expected_count = Some(count_str.parse::<usize>().map_err(|_| {
                            ErrorKind::InvalidNumber((*count_str).into()).at(loc.clone())
                        })?);
                        false
                    }
                    _ => return Err(ErrorKind::InvalidLine(line.into()).at(loc)),
                };
                let mut sql = lines
                    .next()
                    .ok_or_else(|| {
                        ErrorKind::UnexpectedEOF.at(loc.clone().map_line(|line| line + 1))
                    })?
                    .1
                    .into();
                for (_, line) in &mut lines {
                    if line.is_empty() {
                        break;
                    }
                    sql += " ";
                    sql += line;
                }
                records.push(Record::Statement {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    error,
                    sql,
                    expected_count,
                });
            }
            ["query", type_string, res @ ..] => {
                let sort_mode = match res.get(0) {
                    None | Some(&"nosort") => SortMode::NoSort,
                    Some(&"rowsort") => SortMode::RowSort,
                    Some(&"valuesort") => SortMode::ValueSort,
                    Some(mode) => return Err(ErrorKind::InvalidSortMode(mode.to_string()).at(loc)),
                };
                let label = res.get(1).map(|s| s.to_string());
                // The SQL for the query is found on second an subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let mut sql = lines
                    .next()
                    .ok_or_else(|| {
                        ErrorKind::UnexpectedEOF.at(loc.clone().map_line(|line| line + 1))
                    })?
                    .1
                    .into();
                let mut has_result = false;
                for (_, line) in &mut lines {
                    if line.is_empty() || line == "----" {
                        has_result = line == "----";
                        break;
                    }
                    sql += " ";
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
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    type_string: type_string.to_string(),
                    sort_mode,
                    label,
                    sql,
                    expected_results,
                });
            }
            _ => return Err(ErrorKind::InvalidLine(line.into()).at(loc)),
        }
    }
    Ok(records)
}

/// Parse a sqllogictest file and link all included scripts together.
#[doc(hidden)]
pub fn parse_file(filename: impl AsRef<Path>) -> Result<Vec<Record>, Error> {
    parse_file_inner(
        Rc::from(filename.as_ref().to_str().unwrap()),
        filename.as_ref(),
    )
}

fn parse_file_inner(filename: Rc<str>, path: &Path) -> Result<Vec<Record>, Error> {
    let script = std::fs::read_to_string(path).unwrap();
    let mut records = vec![];
    for rec in parse_inner(filename, &script)? {
        if let Record::Include { filename, .. } = rec {
            let mut path_buf = path.to_path_buf();
            path_buf.pop();
            path_buf.push(filename);
            let new_filename = Rc::from(path_buf.as_os_str().to_string_lossy().to_string());
            let new_path = path_buf.as_path();
            records.extend(parse_file_inner(new_filename, new_path)?);
        } else {
            records.push(rec);
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
                error,
                sql,
                loc,
                expected_count,
                ..
            } => {
                let sql = self.replace_keywords(sql);
                let ret = self.db.run(&sql);
                match ret {
                    Ok(_) if error => panic!(
                        "{}: statement is expected to fail, but actually succeed\n\tSQL:{:?}",
                        loc, sql
                    ),
                    Ok(count_str) => {
                        if let Some(expected_count) = expected_count {
                            if expected_count.to_string() != count_str {
                                panic!("{}: statement is expected to affect {} rows, but actually {}\n\tSQL: {:?}", loc, expected_count, count_str, sql)
                            }
                        }
                    }
                    Err(e) if !error => {
                        panic!("{}: statement failed: {}\n\tSQL: {:?}", loc, e, sql)
                    }
                    _ => {}
                }
            }
            Record::Query {
                loc,
                sql,
                expected_results,
                sort_mode,
                ..
            } => {
                let sql = self.replace_keywords(sql);
                let output = match self.db.run(&sql) {
                    Ok(output) => output,
                    Err(e) => panic!("{}: query failed: {}\nSQL: {}", loc, e, sql),
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
                        "{}: query result mismatch:\nSQL:\n{}\n\nExpected:\n{}\nActual:\n{}",
                        loc,
                        sql,
                        expected_results.join("\n"),
                        output.join("\n")
                    );
                }
            }
            Record::Sleep { duration, .. } => std::thread::sleep(duration),
            Record::Halt { .. } => {}
            Record::Subtest { .. } => {}
            Record::Include { loc, .. } => {
                unreachable!("include should be rewritten during link: at {}", loc)
            }
        }
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    pub fn run_multi(&mut self, records: impl IntoIterator<Item = Record>) {
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
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

    /// Run a sqllogictest file.
    pub fn run_file(&mut self, filename: impl AsRef<Path>) {
        let records = parse_file(filename).expect("failed to parse sqllogictest");
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
