//! Sqllogictest runner.

use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use async_trait::async_trait;
use futures::executor::block_on;
use futures::{stream, Future, FutureExt, StreamExt};
use itertools::Itertools;
use md5::Digest;
use owo_colors::OwoColorize;
use regex::Regex;
use similar::{Change, ChangeTag, TextDiff};
use tempfile::{tempdir, TempDir};

use crate::parser::*;
use crate::{ColumnType, Connections, MakeConnection};

#[derive(Debug, Clone)]
pub enum RecordOutput<T: ColumnType> {
    Nothing,
    Query {
        types: Vec<T>,
        rows: Vec<Vec<String>>,
        error: Option<Arc<dyn std::error::Error + Send + Sync>>,
    },
    Statement {
        count: u64,
        error: Option<Arc<dyn std::error::Error + Send + Sync>>,
    },
}

#[non_exhaustive]
pub enum DBOutput<T: ColumnType> {
    Rows {
        types: Vec<T>,
        rows: Vec<Vec<String>>,
    },
    /// A statement in the query has completed.
    ///
    /// The number of rows modified or selected is returned.
    ///
    /// If the test case doesn't specify `statement count <n>`, the number is simply ignored.
    StatementComplete(u64),
}

/// The async database to be tested.
#[async_trait]
pub trait AsyncDB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Async run a SQL query and return the output.
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universial to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        std::thread::sleep(dur);
    }
}

/// The database to be tested.
pub trait DB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Run a SQL query and return the output.
    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }
}

/// Compat-layer for the new AsyncDB and DB trait
#[async_trait]
impl<D> AsyncDB for D
where
    D: DB + Send,
{
    type Error = D::Error;
    type ColumnType = D::ColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        D::run(self, sql)
    }

    fn engine_name(&self) -> &str {
        D::engine_name(self)
    }
}

/// The error type for running sqllogictest.
///
/// For colored error message, use `self.display()`.
#[derive(thiserror::Error, Clone)]
#[error("{kind}\nat {loc}\n")]
pub struct TestError {
    kind: TestErrorKind,
    loc: Location,
}

impl TestError {
    pub fn display(&self, colorize: bool) -> TestErrorDisplay<'_> {
        TestErrorDisplay {
            err: self,
            colorize,
        }
    }
}

/// Overrides the `Display` implementation of [`TestError`] to support controlling colorization.
pub struct TestErrorDisplay<'a> {
    err: &'a TestError,
    colorize: bool,
}

impl<'a> Display for TestErrorDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\nat {}\n",
            self.err.kind.display(self.colorize),
            self.err.loc
        )
    }
}

/// For colored error message, use `self.display()`.
#[derive(Clone, Debug, thiserror::Error)]
pub struct ParallelTestError {
    errors: Vec<TestError>,
}

impl Display for ParallelTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parallel test failed")?;
        write!(f, "Caused by:")?;
        for i in &self.errors {
            writeln!(f, "{i}")?;
        }
        Ok(())
    }
}

/// Overrides the `Display` implementation of [`ParallelTestError`] to support controlling
/// colorization.
pub struct ParallelTestErrorDisplay<'a> {
    err: &'a ParallelTestError,
    colorize: bool,
}

impl<'a> Display for ParallelTestErrorDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parallel test failed")?;
        write!(f, "Caused by:")?;
        for i in &self.err.errors {
            writeln!(f, "{}", i.display(self.colorize))?;
        }
        Ok(())
    }
}

impl ParallelTestError {
    pub fn display(&self, colorize: bool) -> ParallelTestErrorDisplay<'_> {
        ParallelTestErrorDisplay {
            err: self,
            colorize,
        }
    }
}

impl std::fmt::Debug for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl TestError {
    /// Returns the corresponding [`TestErrorKind`] for this error.
    pub fn kind(&self) -> TestErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
    }
}

#[derive(Debug, Clone)]
pub enum RecordKind {
    Statement,
    Query,
}

impl std::fmt::Display for RecordKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordKind::Statement => write!(f, "statement"),
            RecordKind::Query => write!(f, "query"),
        }
    }
}

/// The error kind for running sqllogictest.
///
/// For colored error message, use `self.display()`.
#[derive(thiserror::Error, Debug, Clone)]
pub enum TestErrorKind {
    #[error("parse error: {0}")]
    ParseError(ParseErrorKind),
    #[error("{kind} is expected to fail, but actually succeed:\n[SQL] {sql}")]
    Ok { sql: String, kind: RecordKind },
    #[error("{kind} failed: {err}\n[SQL] {sql}")]
    Fail {
        sql: String,
        err: Arc<dyn std::error::Error + Send + Sync>,
        kind: RecordKind,
    },
    // Remember to also update [`TestErrorKindDisplay`] if this message is changed.
    #[error("{kind} is expected to fail with error:\n\t{expected_err}\nbut got error:\n\t{err}\n[SQL] {sql}")]
    ErrorMismatch {
        sql: String,
        err: Arc<dyn std::error::Error + Send + Sync>,
        expected_err: String,
        kind: RecordKind,
    },
    #[error("statement is expected to affect {expected} rows, but actually {actual}\n[SQL] {sql}")]
    StatementResultMismatch {
        sql: String,
        expected: u64,
        actual: String,
    },
    // Remember to also update [`TestErrorKindDisplay`] if this message is changed.
    #[error(
        "query result mismatch:\n[SQL] {sql}\n[Diff] (-expected|+actual)\n{}",
        TextDiff::from_lines(.expected, .actual).iter_all_changes().format_with("\n", |diff, f| format_diff(&diff, f, false))
    )]
    QueryResultMismatch {
        sql: String,
        expected: String,
        actual: String,
    },
    #[error(
        "query columns mismatch:\n[SQL] {sql}\n{}",
        format_column_diff(expected, actual, false)
    )]
    QueryResultColumnsMismatch {
        sql: String,
        expected: String,
        actual: String,
    },
}

impl From<ParseError> for TestError {
    fn from(e: ParseError) -> Self {
        TestError {
            kind: TestErrorKind::ParseError(e.kind()),
            loc: e.location(),
        }
    }
}

impl TestErrorKind {
    fn at(self, loc: Location) -> TestError {
        TestError { kind: self, loc }
    }

    pub fn display(&self, colorize: bool) -> TestErrorKindDisplay<'_> {
        TestErrorKindDisplay {
            error: self,
            colorize,
        }
    }
}

/// Overrides the `Display` implementation of [`TestErrorKind`] to support controlling colorization.
pub struct TestErrorKindDisplay<'a> {
    error: &'a TestErrorKind,
    colorize: bool,
}

impl<'a> Display for TestErrorKindDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.colorize {
            return write!(f, "{}", self.error);
        }
        match self.error {
            TestErrorKind::ErrorMismatch {
                sql,
                err,
                expected_err,
                kind,
            } => write!(
                f,
                "{kind} is expected to fail with error:\n\t{}\nbut got error:\n\t{}\n[SQL] {sql}",
                expected_err.bright_green(),
                err.bright_red(),
            ),
            TestErrorKind::QueryResultMismatch {
                sql,
                expected,
                actual,
            } => write!(
                f,
                "query result mismatch:\n[SQL] {sql}\n[Diff] ({}|{})\n{}",
                "-expected".bright_red(),
                "+actual".bright_green(),
                TextDiff::from_lines(expected, actual)
                    .iter_all_changes()
                    .format_with("\n", |diff, f| format_diff(&diff, f, true))
            ),
            TestErrorKind::QueryResultColumnsMismatch {
                sql,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "query columns mismatch:\n[SQL] {sql}\n{}",
                    format_column_diff(expected, actual, true)
                )
            }
            _ => write!(f, "{}", self.error),
        }
    }
}

fn format_diff(
    diff: &Change<&str>,
    f: &mut dyn FnMut(&dyn std::fmt::Display) -> std::fmt::Result,
    colorize: bool,
) -> std::fmt::Result {
    match diff.tag() {
        ChangeTag::Equal => f(&diff
            .value()
            .lines()
            .format_with("\n", |line, f| f(&format_args!("    {line}")))),
        ChangeTag::Insert => f(&diff.value().lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("+   {line}").bright_green())
            } else {
                f(&format_args!("+   {line}"))
            }
        })),
        ChangeTag::Delete => f(&diff.value().lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("-   {line}").bright_red())
            } else {
                f(&format_args!("-   {line}"))
            }
        })),
    }
}

fn format_column_diff(expected: &str, actual: &str, colorize: bool) -> String {
    let (expected, actual) = TextDiff::from_chars(expected, actual)
        .iter_all_changes()
        .fold(
            ("".to_string(), "".to_string()),
            |(expected, actual), change| match change.tag() {
                ChangeTag::Equal => (
                    format!("{}{}", expected, change.value()),
                    format!("{}{}", actual, change.value()),
                ),
                ChangeTag::Delete => (
                    if colorize {
                        format!("{}[{}]", expected, change.value().bright_red())
                    } else {
                        format!("{}[{}]", expected, change.value())
                    },
                    actual,
                ),
                ChangeTag::Insert => (
                    expected,
                    if colorize {
                        format!("{}[{}]", actual, change.value().bright_green())
                    } else {
                        format!("{}[{}]", actual, change.value())
                    },
                ),
            },
        );
    format!("[Expected] {expected}\n[Actual  ] {actual}")
}

/// Trim and replace multiple whitespaces with one.
#[allow(clippy::ptr_arg)]
fn normalize_string(s: &String) -> String {
    s.trim().split_ascii_whitespace().join(" ")
}

/// Validator will be used by [`Runner`] to validate the output.
///
/// # Default
///
/// By default ([`default_validator`]), we will use compare normalized results.
pub type Validator = fn(actual: &[Vec<String>], expected: &[String]) -> bool;

pub fn default_validator(actual: &[Vec<String>], expected: &[String]) -> bool {
    let expected_results = expected.iter().map(normalize_string).collect_vec();
    // Default, we compare normalized results. Whitespace characters are ignored.
    let normalized_rows = actual
        .iter()
        .map(|strs| strs.iter().map(normalize_string).join(" "))
        .collect_vec();
    normalized_rows == expected_results
}

/// [`Runner`] uses this validator to check that the expected column types match an actual output.
///
/// # Default
///
/// By default ([`default_column_validator`]), columns are not validated.
pub type ColumnTypeValidator<T> = fn(actual: &Vec<T>, expected: &Vec<T>) -> bool;

/// The default validator always returns success for any inputs of expected and actual sets of
/// columns.
pub fn default_column_validator<T: ColumnType>(_: &Vec<T>, _: &Vec<T>) -> bool {
    true
}

/// The strict validator checks:
/// - the number of columns is as expected
/// - each column has the same type as expected
pub fn strict_column_validator<T: ColumnType>(actual: &Vec<T>, expected: &Vec<T>) -> bool {
    actual.len() == expected.len()
        && !actual
            .iter()
            .zip(expected.iter())
            .any(|(actual_column, expected_column)| actual_column != expected_column)
}

/// Sqllogictest runner.
pub struct Runner<M: MakeConnection> {
    conn: Connections<M>,
    // validator is used for validate if the result of query equals to expected.
    validator: Validator,
    column_type_validator: ColumnTypeValidator<<M::Conn as AsyncDB>::ColumnType>,
    testdir: Option<TempDir>,
    sort_mode: Option<SortMode>,
    /// 0 means never hashing
    hash_threshold: usize,
    /// Labels for condition `skipif` and `onlyif`.
    labels: HashSet<String>,
}

impl<M: MakeConnection> Runner<M> {
    /// Create a new test runner on the database, with the given connection maker.
    ///
    /// See [`MakeConnection`] for more details.
    pub fn new(make_conn: M) -> Self {
        Runner {
            validator: default_validator,
            column_type_validator: default_column_validator,
            testdir: None,
            sort_mode: None,
            hash_threshold: 0,
            labels: HashSet::new(),
            conn: Connections::new(make_conn),
        }
    }

    /// Add a label for condition `skipif` and `onlyif`.
    pub fn add_label(&mut self, label: &str) {
        self.labels.insert(label.to_string());
    }

    /// Replace the pattern `__TEST_DIR__` in SQL with a temporary directory path.
    ///
    /// This feature is useful in those tests where data will be written to local
    /// files, e.g. `COPY`.
    pub fn enable_testdir(&mut self) {
        self.testdir = Some(tempdir().expect("failed to create testdir"));
    }

    pub fn with_validator(&mut self, validator: Validator) {
        self.validator = validator;
    }

    pub fn with_column_validator(
        &mut self,
        validator: ColumnTypeValidator<<M::Conn as AsyncDB>::ColumnType>,
    ) {
        self.column_type_validator = validator;
    }

    pub fn with_hash_threshold(&mut self, hash_threshold: usize) {
        self.hash_threshold = hash_threshold;
    }

    pub async fn apply_record(
        &mut self,
        record: Record<<M::Conn as AsyncDB>::ColumnType>,
    ) -> RecordOutput<<M::Conn as AsyncDB>::ColumnType> {
        /// Returns whether we should skip this record, according to given `conditions`.
        fn should_skip(
            labels: &HashSet<String>,
            engine_name: &str,
            conditions: &[Condition],
        ) -> bool {
            conditions.iter().any(|c| {
                c.should_skip(
                    labels
                        .iter()
                        .map(|l| l.as_str())
                        // attach the engine name to the labels
                        .chain(Some(engine_name).filter(|n| !n.is_empty())),
                )
            })
        }

        match record {
            Record::Statement {
                conditions,
                connection,
                sql,

                // compare result in run_async
                expected_error: _,
                expected_count: _,
                loc: _,
            } => {
                let sql = self.replace_keywords(sql);

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(Arc::new(e)),
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let ret = conn.run(&sql).await;
                match ret {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => RecordOutput::Query {
                            types,
                            rows,
                            error: None,
                        },
                        DBOutput::StatementComplete(count) => {
                            RecordOutput::Statement { count, error: None }
                        }
                    },
                    Err(e) => RecordOutput::Statement {
                        count: 0,
                        error: Some(Arc::new(e)),
                    },
                }
            }
            Record::Query {
                conditions,
                connection,
                sql,
                sort_mode,

                // compare result in run_async
                expected_types: _,
                expected_error: _,
                expected_results: _,
                loc: _,

                // not handle yet,
                label: _,
            } => {
                let sql = self.replace_keywords(sql);

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let (types, mut rows) = match conn.run(&sql).await {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => (types, rows),
                        DBOutput::StatementComplete(count) => {
                            return RecordOutput::Statement { count, error: None };
                        }
                    },
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        };
                    }
                };

                match sort_mode.as_ref().or(self.sort_mode.as_ref()) {
                    None | Some(SortMode::NoSort) => {}
                    Some(SortMode::RowSort) => {
                        rows.sort_unstable();
                    }
                    Some(SortMode::ValueSort) => todo!("value sort"),
                };

                if self.hash_threshold > 0 && rows.len() * types.len() > self.hash_threshold {
                    let mut md5 = md5::Md5::new();
                    for line in &rows {
                        for value in line {
                            md5.update(value.as_bytes());
                            md5.update(b"\n");
                        }
                    }
                    let hash = format!("{:2x}", md5.finalize());
                    rows = vec![vec![format!(
                        "{} values hashing to {}",
                        rows.len() * rows[0].len(),
                        hash
                    )]];
                }

                RecordOutput::Query {
                    error: None,
                    types,
                    rows,
                }
            }
            Record::Sleep { duration, .. } => {
                <M::Conn as AsyncDB>::sleep(duration).await;
                RecordOutput::Nothing
            }
            Record::Control(control) => match control {
                Control::SortMode(sort_mode) => {
                    self.sort_mode = Some(sort_mode);
                    RecordOutput::Nothing
                }
            },
            Record::HashThreshold { loc: _, threshold } => {
                self.hash_threshold = threshold as usize;
                RecordOutput::Nothing
            }
            Record::Include { .. }
            | Record::Comment(_)
            | Record::Newline
            | Record::Subtest { .. }
            | Record::Halt { .. }
            | Record::Injected(_)
            | Record::Condition(_)
            | Record::Connection(_) => RecordOutput::Nothing,
        }
    }

    /// Run a single record.
    pub async fn run_async(
        &mut self,
        record: Record<<M::Conn as AsyncDB>::ColumnType>,
    ) -> Result<(), TestError> {
        tracing::debug!(?record, "testing");

        match (record.clone(), self.apply_record(record).await) {
            (_, RecordOutput::Nothing) => {}
            // Tolerate the mismatched return type...
            (Record::Statement { .. }, RecordOutput::Query { error: None, .. }) => {}
            (
                Record::Query {
                    expected_results,
                    loc,
                    sql,
                    ..
                },
                RecordOutput::Statement { error: None, .. },
            ) => {
                if !expected_results.is_empty() {
                    return Err(TestErrorKind::QueryResultMismatch {
                        sql,
                        expected: expected_results.join("\n"),
                        actual: "".to_string(),
                    }
                    .at(loc));
                }
            }
            (
                Record::Statement {
                    loc,
                    connection: _,
                    conditions: _,
                    expected_error,
                    sql,
                    expected_count,
                },
                RecordOutput::Statement { count, error },
            ) => match (error, expected_error) {
                (None, Some(_)) => {
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Statement,
                    }
                    .at(loc))
                }
                (None, None) => {
                    if let Some(expected_count) = expected_count {
                        if expected_count != count {
                            return Err(TestErrorKind::StatementResultMismatch {
                                sql,
                                expected: expected_count,
                                actual: format!("affected {count} rows"),
                            }
                            .at(loc));
                        }
                    }
                }
                (Some(e), Some(expected_error)) => {
                    if !expected_error.is_match(&e.to_string()) {
                        return Err(TestErrorKind::ErrorMismatch {
                            sql,
                            err: Arc::new(e),
                            expected_err: expected_error.to_string(),
                            kind: RecordKind::Statement,
                        }
                        .at(loc));
                    }
                }
                (Some(e), None) => {
                    return Err(TestErrorKind::Fail {
                        sql,
                        err: Arc::new(e),
                        kind: RecordKind::Statement,
                    }
                    .at(loc));
                }
            },
            (
                Record::Query {
                    loc,
                    conditions: _,
                    connection: _,
                    expected_types,
                    sort_mode: _,
                    label: _,
                    expected_error,
                    sql,
                    expected_results,
                },
                RecordOutput::Query { types, rows, error },
            ) => {
                match (error, expected_error) {
                    (None, Some(_)) => {
                        return Err(TestErrorKind::Ok {
                            sql,
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                    (None, None) => {}
                    (Some(e), Some(expected_error)) => {
                        if !expected_error.is_match(&e.to_string()) {
                            return Err(TestErrorKind::ErrorMismatch {
                                sql,
                                err: Arc::new(e),
                                expected_err: expected_error.to_string(),
                                kind: RecordKind::Query,
                            }
                            .at(loc));
                        }
                        return Ok(());
                    }
                    (Some(e), None) => {
                        return Err(TestErrorKind::Fail {
                            sql,
                            err: Arc::new(e),
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                };

                if !(self.column_type_validator)(&types, &expected_types) {
                    return Err(TestErrorKind::QueryResultColumnsMismatch {
                        sql,
                        expected: expected_types.iter().map(|c| c.to_char()).join(""),
                        actual: types.iter().map(|c| c.to_char()).join(""),
                    }
                    .at(loc));
                }

                if !(self.validator)(&rows, &expected_results) {
                    let output_rows = rows
                        .into_iter()
                        .map(|strs| strs.iter().join(" "))
                        .collect_vec();
                    return Err(TestErrorKind::QueryResultMismatch {
                        sql,
                        expected: expected_results.join("\n"),
                        actual: output_rows.join("\n"),
                    }
                    .at(loc));
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    /// Run a single record.
    pub fn run(
        &mut self,
        record: Record<<M::Conn as AsyncDB>::ColumnType>,
    ) -> Result<(), TestError> {
        futures::executor::block_on(self.run_async(record))
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    pub async fn run_multi_async(
        &mut self,
        records: impl IntoIterator<Item = Record<<M::Conn as AsyncDB>::ColumnType>>,
    ) -> Result<(), TestError> {
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
                break;
            }
            self.run_async(record).await?;
        }
        Ok(())
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    pub fn run_multi(
        &mut self,
        records: impl IntoIterator<Item = Record<<M::Conn as AsyncDB>::ColumnType>>,
    ) -> Result<(), TestError> {
        block_on(self.run_multi_async(records))
    }

    /// Run a sqllogictest script.
    pub async fn run_script_async(&mut self, script: &str) -> Result<(), TestError> {
        let records = parse(script).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script with a given script name.
    pub async fn run_script_with_name_async(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        let records = parse_with_name(script, name).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest file.
    pub async fn run_file_async(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        let records = parse_file(filename)?;
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script.
    pub fn run_script(&mut self, script: &str) -> Result<(), TestError> {
        block_on(self.run_script_async(script))
    }

    /// Run a sqllogictest script with a given script name.
    pub fn run_script_with_name(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        block_on(self.run_script_with_name_async(script, name))
    }

    /// Run a sqllogictest file.
    pub fn run_file(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        block_on(self.run_file_async(filename))
    }

    /// accept the tasks, spawn jobs task to run slt test. the tasks are (AsyncDB, slt filename)
    /// pairs.
    pub async fn run_parallel_async<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), ParallelTestError>
    where
        Fut: Future<Output = M::Conn>,
    {
        let files = glob::glob(glob).expect("failed to read glob pattern");
        let mut tasks = vec![];
        // let conn_builder = Arc::new(conn_builder);

        for (idx, file) in files.enumerate() {
            // for every slt file, we create a database against table conflict
            let file = file.unwrap();
            let db_name = file
                .file_name()
                .expect("not a valid filename")
                .to_str()
                .expect("not a UTF-8 filename");
            let db_name = db_name.replace([' ', '.', '-'], "_");

            self.conn
                .run_default(&format!("CREATE DATABASE {db_name};"))
                .await
                .expect("create db failed");
            let target = hosts[idx % hosts.len()].clone();
            tasks.push(async move {
                let mut tester =
                    Runner::new(move || conn_builder(target.clone(), db_name.clone()).map(Ok));
                let filename = file.to_string_lossy().to_string();
                tester.run_file_async(filename).await
            })
        }

        let tasks = stream::iter(tasks).buffer_unordered(jobs);
        let errors: Vec<_> = tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ParallelTestError { errors })
        }
    }

    /// sync version of `run_parallel_async`
    pub fn run_parallel<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), ParallelTestError>
    where
        Fut: Future<Output = M::Conn>,
    {
        block_on(self.run_parallel_async(glob, hosts, conn_builder, jobs))
    }

    /// Replace all keywords in the SQL.
    fn replace_keywords(&self, sql: String) -> String {
        if let Some(testdir) = &self.testdir {
            sql.replace("__TEST_DIR__", testdir.path().to_str().unwrap())
        } else {
            sql
        }
    }

    /// Updates a test file with the output produced by a Database. It is an utility function
    /// wrapping [`update_test_file_with_runner`].
    ///
    /// Specifically, it will create `"{filename}.temp"` to buffer the updated records and then
    /// override the original file with it.
    ///
    /// Some other notes:
    /// - empty lines at the end of the file are cleaned.
    /// - `halt` and `include` are correctly handled.
    pub async fn update_test_file(
        &mut self,
        filename: impl AsRef<Path>,
        col_separator: &str,
        validator: Validator,
        column_type_validator: ColumnTypeValidator<<M::Conn as AsyncDB>::ColumnType>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{Read, Seek, SeekFrom, Write};
        use std::path::PathBuf;

        use fs_err::{File, OpenOptions};

        fn create_outfile(filename: impl AsRef<Path>) -> std::io::Result<(PathBuf, File)> {
            let filename = filename.as_ref();
            let outfilename = filename.file_name().unwrap().to_str().unwrap().to_owned() + ".temp";
            let outfilename = filename.parent().unwrap().join(outfilename);
            // create a temp file in read-write mode
            let outfile = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .read(true)
                .open(&outfilename)?;
            Ok((outfilename, outfile))
        }

        fn override_with_outfile(
            filename: &String,
            outfilename: &PathBuf,
            outfile: &mut File,
        ) -> std::io::Result<()> {
            // check whether outfile ends with multiple newlines, which happens if
            // - the last record is statement/query
            // - the original file ends with multiple newlines

            const N: usize = 8;
            let mut buf = [0u8; N];
            loop {
                outfile.seek(SeekFrom::End(-(N as i64))).unwrap();
                outfile.read_exact(&mut buf).unwrap();
                let num_newlines = buf.iter().rev().take_while(|&&b| b == b'\n').count();
                assert!(num_newlines > 0);

                if num_newlines > 1 {
                    // if so, remove the last ones
                    outfile
                        .set_len(outfile.metadata().unwrap().len() - num_newlines as u64 + 1)
                        .unwrap();
                }

                if num_newlines == 1 || num_newlines < N {
                    break;
                }
            }

            outfile.flush()?;
            fs_err::rename(outfilename, filename)?;

            Ok(())
        }

        struct Item {
            filename: String,
            outfilename: PathBuf,
            outfile: File,
            halt: bool,
        }

        let filename = filename.as_ref();
        let records = parse_file(filename)?;

        let (outfilename, outfile) = create_outfile(filename)?;
        let mut stack = vec![Item {
            filename: filename.to_string_lossy().to_string(),
            outfilename,
            outfile,
            halt: false,
        }];

        for record in records {
            let Item {
                filename,
                outfilename,
                outfile,
                halt,
            } = stack.last_mut().unwrap();

            match &record {
                Record::Injected(Injected::BeginInclude(filename)) => {
                    let (outfilename, outfile) = create_outfile(filename)?;
                    stack.push(Item {
                        filename: filename.clone(),
                        outfilename,
                        outfile,
                        halt: false,
                    });
                }
                Record::Injected(Injected::EndInclude(_)) => {
                    override_with_outfile(filename, outfilename, outfile)?;
                    stack.pop();
                }
                _ => {
                    if *halt {
                        writeln!(outfile, "{record}")?;
                        continue;
                    }
                    if matches!(record, Record::Halt { .. }) {
                        *halt = true;
                        writeln!(outfile, "{record}")?;
                        continue;
                    }
                    let record_output = self.apply_record(record.clone()).await;
                    let record = update_record_with_output(
                        &record,
                        &record_output,
                        col_separator,
                        validator,
                        column_type_validator,
                    )
                    .unwrap_or(record);
                    writeln!(outfile, "{record}")?;
                }
            }
        }

        let Item {
            filename,
            outfilename,
            outfile,
            halt: _,
        } = stack.last_mut().unwrap();
        override_with_outfile(filename, outfilename, outfile)?;

        Ok(())
    }
}

/// Updates the specified [`Record`] with the [`QueryOutput`] produced
/// by a Database, returning `Some(new_record)`.
///
/// If an update is not supported, returns `None`
pub fn update_record_with_output<T: ColumnType>(
    record: &Record<T>,
    record_output: &RecordOutput<T>,
    col_separator: &str,
    validator: Validator,
    column_type_validator: ColumnTypeValidator<T>,
) -> Option<Record<T>> {
    match (record.clone(), record_output) {
        (_, RecordOutput::Nothing) => None,
        // statement, query
        (
            Record::Statement {
                sql,
                loc,
                conditions,
                connection,
                expected_error: None,
                expected_count,
            },
            RecordOutput::Query { error: None, .. },
        ) => {
            // statement ok
            // SELECT ...
            //
            // This case can be used when we want to only ensure the query succeeds,
            // but don't care about the output.
            // DuckDB has a few of these.

            Some(Record::Statement {
                sql,
                expected_error: None,
                loc,
                conditions,
                connection,
                expected_count,
            })
        }
        // query, statement
        (
            Record::Query {
                sql,
                loc,
                conditions,
                connection,
                ..
            },
            RecordOutput::Statement { error: None, .. },
        ) => Some(Record::Statement {
            sql,
            expected_error: None,
            loc,
            conditions,
            connection,
            expected_count: None,
        }),
        // statement, statement
        (
            Record::Statement {
                loc,
                conditions,
                connection,
                expected_error,
                sql,
                expected_count,
            },
            RecordOutput::Statement { count, error },
        ) => match (error, expected_error) {
            // Ok
            (None, _) => Some(Record::Statement {
                sql,
                expected_error: None,
                loc,
                conditions,
                connection,
                expected_count: expected_count.map(|_| *count),
            }),
            // Error match
            (Some(e), Some(expected_error)) if expected_error.is_match(&e.to_string()) => {
                Some(Record::Statement {
                    sql,
                    expected_error: Some(expected_error),
                    loc,
                    conditions,
                    connection,
                    expected_count: None,
                })
            }
            // Error mismatch, update expected error
            (Some(e), _) => Some(Record::Statement {
                sql,
                expected_error: Some(Regex::new(&regex::escape(&e.to_string())).unwrap()),
                loc,
                conditions,
                connection,
                expected_count: None,
            }),
        },
        // query, query
        (
            Record::Query {
                loc,
                conditions,
                connection,
                expected_types,
                sort_mode,
                label,
                expected_error,
                sql,
                expected_results,
            },
            RecordOutput::Query { types, rows, error },
        ) => {
            match (error, expected_error) {
                (None, _) => {}
                // Error match
                (Some(e), Some(expected_error)) if expected_error.is_match(&e.to_string()) => {
                    return Some(Record::Query {
                        sql,
                        expected_error: Some(expected_error),
                        loc,
                        conditions,
                        connection,
                        expected_types: vec![],
                        sort_mode,
                        label,
                        expected_results: vec![],
                    });
                }
                // Error mismatch
                (Some(e), _) => {
                    return Some(Record::Query {
                        sql,
                        expected_error: Some(Regex::new(&regex::escape(&e.to_string())).unwrap()),
                        loc,
                        conditions,
                        connection,
                        expected_types: vec![],
                        sort_mode,
                        label,
                        expected_results: vec![],
                    });
                }
            };

            let results = if validator(rows, &expected_results) {
                // If validation is successful, we respect the original file's expected results.
                expected_results
            } else {
                rows.iter().map(|cols| cols.join(col_separator)).collect()
            };

            let types = if column_type_validator(types, &expected_types) {
                // If validation is successful, we respect the original file's expected types.
                expected_types
            } else {
                types.clone()
            };

            Some(Record::Query {
                sql,
                expected_error: None,
                loc,
                conditions,
                connection,
                expected_types: types,
                sort_mode,
                label,
                expected_results: results,
            })
        }

        // No update possible, return the original record
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultColumnType;

    #[test]
    fn test_query_replacement_no_changes() {
        let record = "query   I?\n\
                    select * from foo;\n\
                    ----\n\
                    3      4";
        TestCase {
            // keep the input values
            input: record,

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(record),
        }
        .run()
    }

    #[test]
    fn test_query_replacement() {
        TestCase {
            // input should be ignored
            input: "query III\n\
                    select * from foo;\n\
                    ----\n\
                    1 2",

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "query I?\n\
                 select * from foo;\n\
                 ----\n\
                 3 4",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_no_input() {
        TestCase {
            // input has no query results
            input: "query\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "query I?\n\
                 select * from foo;\n\
                 ----\n\
                 3 4",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_no_output() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model nothing was output
            record_output: RecordOutput::Nothing,

            // No update
            expected: None,
        }
        .run()
    }

    #[test]
    fn test_query_replacement_error() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a "MyAwesomeDB Error"
            record_output: query_output_error("MyAwesomeDB Error"),

            expected: Some(
                "query error TestError: MyAwesomeDB Error\n\
                 select * from foo;\n",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_query_output() {
        TestCase {
            // input has no query results
            input: "statement ok\n\
                    create table foo;",

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "statement ok\n\
                 create table foo;",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_statement_output() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            expected: Some(
                "statement ok\n\
                 select * from foo;",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_output() {
        TestCase {
            // statement that has no output
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            // Note the the output does not include 3 (statement
            // count) Rationale is if the record is statement count
            // <n>, n will be updated to real count. If the record is
            // statement ok (which means we don't care the number of
            // affected rows), it won't be updated.
            expected: Some(
                "statement ok\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_to_ok() {
        TestCase {
            // statement expected error
            input: "statement error\n\
                    insert into foo values(2);",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            expected: Some(
                "statement ok\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_no_error() {
        TestCase {
            // statement expected error
            input: "statement error\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // Input didn't have an expected error, so output is not to expect the message
            expected: Some(
                "statement error\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_new_error() {
        TestCase {
            // statement expected error
            input: "statement error bar\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: foo\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_ok_to_error() {
        TestCase {
            // statement was ok
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: foo\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_special_chars() {
        TestCase {
            // statement expected error
            input: "statement error tbd\n\
                    inser into foo values(2);",

            // Model a run that produced an error message that contains regex special characters
            record_output: statement_output_error("The operation (inser) is not supported. Did you mean [insert]?"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: The operation \\(inser\\) is not supported\\. Did you mean \\[insert\\]\\?\n\
                 inser into foo values(2);",
            ),
        }
            .run()
    }

    #[test]
    fn test_statement_keep_error_regex_when_matches() {
        TestCase {
            // statement expected error
            input: "statement error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                    inser into foo values(2);",

            // Model a run that produced an error message that contains regex special characters
            record_output: statement_output_error(
                "The operation (inser) is not supported. Did you mean [insert]?",
            ),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                 inser into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_error_special_chars() {
        TestCase {
            // statement expected error
            input: "query error tbd\n\
                    selec *;",

            // Model a run that produced an error message that contains regex special characters
            record_output: query_output_error("The operation (selec) is not supported. Did you mean [select]?"),

            // expect the output includes foo
            expected: Some(
                "query error TestError: The operation \\(selec\\) is not supported\\. Did you mean \\[select\\]\\?\n\
                 selec *;",
            ),
        }
            .run()
    }

    #[test]
    fn test_query_error_special_chars_when_matches() {
        TestCase {
            // statement expected error
            input: "query error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                    selec *;",

            // Model a run that produced an error message that contains regex special characters
            record_output: query_output_error(
                "The operation (selec) is not supported. Did you mean [select]?",
            ),

            // expect the output includes foo
            expected: Some(
                "query error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                 selec *;",
            ),
        }
        .run()
    }

    #[derive(Debug)]
    struct TestCase {
        input: &'static str,
        record_output: RecordOutput<DefaultColumnType>,
        expected: Option<&'static str>,
    }

    impl TestCase {
        fn run(self) {
            let Self {
                input,
                record_output,
                expected,
            } = self;
            println!("TestCase");
            println!("**input:\n{input}\n");
            println!("**record_output:\n{record_output:#?}\n");
            println!("**expected:\n{}\n", expected.unwrap_or(""));
            let input = parse_to_record(input);
            let expected = expected.map(parse_to_record);
            let output = update_record_with_output(
                &input,
                &record_output,
                " ",
                default_validator,
                strict_column_validator,
            );

            assert_eq!(
                &output,
                &expected,
                "\n\noutput:\n\n{}\n\nexpected:\n\n{}",
                output
                    .as_ref()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "None".into()),
                expected
                    .as_ref()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "None".into()),
            );
        }
    }

    fn parse_to_record(s: &str) -> Record<DefaultColumnType> {
        let mut records = parse(s).unwrap();
        assert_eq!(records.len(), 1);
        records.pop().unwrap()
    }

    /// Returns a RecordOutput that models the successful execution of a query
    fn query_output(
        rows: &[&[&str]],
        types: Vec<DefaultColumnType>,
    ) -> RecordOutput<DefaultColumnType> {
        let rows = rows
            .iter()
            .map(|cols| cols.iter().map(|c| c.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        RecordOutput::Query {
            types,
            rows,
            error: None,
        }
    }

    /// Returns a RecordOutput that models the error of a query
    fn query_output_error(error_message: &str) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Query {
            types: vec![],
            rows: vec![],
            error: Some(Arc::new(TestError(error_message.to_string()))),
        }
    }

    fn statement_output(count: u64) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Statement { count, error: None }
    }

    /// RecordOutput that models a statement with error
    fn statement_output_error(error_message: &str) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Statement {
            count: 0,
            error: Some(Arc::new(TestError(error_message.to_string()))),
        }
    }

    #[derive(Debug)]
    struct TestError(String);
    impl std::error::Error for TestError {}
    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError: {}", self.0)
        }
    }
}
