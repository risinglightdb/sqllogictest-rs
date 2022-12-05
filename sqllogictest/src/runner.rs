//! Sqllogictest runner.

use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use async_trait::async_trait;
use difference::Difference;
use futures::executor::block_on;
use futures::{stream, Future, StreamExt};
use itertools::Itertools;
use owo_colors::OwoColorize;
use tempfile::{tempdir, TempDir};

use crate::parser::*;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[non_exhaustive]
pub enum ColumnType {
    Text,
    Integer,
    FloatingPoint,
    /// Do not check the type of the column.
    Any,
}

impl TryFrom<char> for ColumnType {
    type Error = ParseErrorKind;

    fn try_from(c: char) -> Result<Self, Self::Error> {
        match c {
            'T' => Ok(Self::Text),
            'I' => Ok(Self::Integer),
            'F' => Ok(Self::FloatingPoint),
            // FIXME:
            // _ => Err(ParseErrorKind::InvalidType(c)),
            _ => Ok(Self::Any),
        }
    }
}

#[non_exhaustive]
pub enum DBOutput {
    Rows {
        types: Vec<ColumnType>,
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
pub trait AsyncDB: Send {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Async run a SQL query and return the output.
    async fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error>;

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
pub trait DB: Send {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Run a SQL query and return the output.
    fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error>;

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }
}

/// Compat-layer for the new AsyncDB and DB trait
#[async_trait]
impl<D> AsyncDB for D
where
    D: DB,
{
    type Error = <D as DB>::Error;

    async fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        <D as DB>::run(self, sql)
    }

    fn engine_name(&self) -> &str {
        <D as DB>::engine_name(self)
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
            writeln!(f, "{}", i)?;
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
        write!(f, "{}", self)
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
        "query result mismatch:\n[SQL] {sql}\n[Diff] (-excepted|+actual)\n{}",
        difference::Changeset::new(.expected, .actual, "\n").diffs.iter().format_with("\n", |diff, f| format_diff(diff, f, false))
    )]
    QueryResultMismatch {
        sql: String,
        expected: String,
        actual: String,
    },
    #[error("expected results are invalid: expected {expected} columns, got {actual} columns\n[SQL] {sql}")]
    QueryResultColumnCountMismatch {
        sql: String,
        expected: usize,
        actual: usize,
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
                "-excepted".bright_red(),
                "+actual".bright_green(),
                difference::Changeset::new(expected, actual, "\n")
                    .diffs
                    .iter()
                    .format_with("\n", |diff, f| format_diff(diff, f, true))
            ),
            _ => write!(f, "{}", self.error),
        }
    }
}

fn format_diff(
    diff: &Difference,
    f: &mut dyn FnMut(&dyn std::fmt::Display) -> std::fmt::Result,
    colorize: bool,
) -> std::fmt::Result {
    match *diff {
        Difference::Same(ref x) => f(&x
            .lines()
            .format_with("\n", |line, f| f(&format_args!("    {line}")))),
        Difference::Add(ref x) => f(&x.lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("+   {line}").bright_green())
            } else {
                f(&format_args!("+   {line}"))
            }
        })),
        Difference::Rem(ref x) => f(&x.lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("-   {line}").bright_red())
            } else {
                f(&format_args!("-   {line}"))
            }
        })),
    }
}

/// Validator will be used by `Runner` to validate the output.
///
/// # Default
///
/// By default, we will use `|x, y| x == y`.
pub type Validator = fn(&Vec<String>, &Vec<String>) -> bool;

/// A collection of hook functions.
#[async_trait]
pub trait Hook: Send {
    /// Called after each statement completes.
    async fn on_stmt_complete(&mut self, _sql: &str) {}

    /// Called after each query completes.
    async fn on_query_complete(&mut self, _sql: &str) {}
}

/// Sqllogictest runner.
pub struct Runner<D: AsyncDB> {
    db: D,
    // validator is used for validate if the result of query equals to expected.
    validator: Validator,
    testdir: Option<TempDir>,
    sort_mode: Option<SortMode>,
    hook: Option<Box<dyn Hook>>,
    /// 0 means never hashing
    hash_threshold: usize,
}

impl<D: AsyncDB> Runner<D> {
    /// Create a new test runner on the database.
    pub fn new(db: D) -> Self {
        Runner {
            db,
            validator: |x, y| x == y,
            testdir: None,
            sort_mode: None,
            hook: None,
            hash_threshold: 0,
        }
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

    /// Run a single record.
    pub async fn run_async(&mut self, record: Record) -> Result<(), TestError> {
        tracing::info!(?record, "testing");
        match record {
            Record::Statement { conditions, .. } if self.should_skip(&conditions) => {}
            Record::Statement {
                conditions: _,

                expected_error,
                sql,
                loc,
                expected_count,
            } => {
                let sql = self.replace_keywords(sql);
                let ret = self.db.run(&sql).await;
                match (ret, expected_error) {
                    (Ok(_), Some(_)) => {
                        return Err(TestErrorKind::Ok {
                            sql,
                            kind: RecordKind::Statement,
                        }
                        .at(loc))
                    }
                    (Ok(result), None) => {
                        if let Some(expected_count) = expected_count {
                            let count = match result {
                                DBOutput::Rows { types: _, rows } => {
                                    return Err(TestErrorKind::StatementResultMismatch {
                                        sql,
                                        expected: expected_count,
                                        actual: format!("got rows {:?}", rows),
                                    }
                                    .at(loc));
                                }
                                DBOutput::StatementComplete(count) => count,
                            };

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
                    (Err(e), Some(expected_error)) => {
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
                    (Err(e), None) => {
                        return Err(TestErrorKind::Fail {
                            sql,
                            err: Arc::new(e),
                            kind: RecordKind::Statement,
                        }
                        .at(loc));
                    }
                }
                if let Some(hook) = &mut self.hook {
                    hook.on_stmt_complete(&sql).await;
                }
            }
            Record::Query { conditions, .. } if self.should_skip(&conditions) => {}
            Record::Query {
                conditions: _,

                loc,
                sql,
                expected_error,
                mut expected_results,
                sort_mode,
                type_string,

                // not handle yet,
                label: _,
            } => {
                let sql = self.replace_keywords(sql);
                let output = match (self.db.run(&sql).await, expected_error) {
                    (Ok(_), Some(_)) => {
                        return Err(TestErrorKind::Ok {
                            sql,
                            kind: RecordKind::Query,
                        }
                        .at(loc))
                    }
                    (Ok(output), None) => output,
                    (Err(e), Some(expected_error)) => {
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
                    (Err(e), None) => {
                        return Err(TestErrorKind::Fail {
                            sql,
                            err: Arc::new(e),
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                };

                let (types, mut output) = match output {
                    DBOutput::Rows { types, rows } => (types, rows),
                    DBOutput::StatementComplete(_) => {
                        return Err(TestErrorKind::QueryResultMismatch {
                            sql,
                            expected: expected_results.join("\n"),
                            actual: "statement complete".to_string(),
                        }
                        .at(loc))
                    }
                };

                // check number of columns
                if types.len() != type_string.len() {
                    // FIXME: do not validate type-string now
                    // return Err(TestErrorKind::QueryResultColumnCountMismatch {
                    //     sql,
                    //     expected: type_string.len(),
                    //     actual: types.len(),
                    // }
                    // .at(loc));
                }
                for (t_actual, t_expected) in types.iter().zip(type_string.iter()) {
                    if t_actual != &ColumnType::Any
                        && t_expected != &ColumnType::Any
                        && t_actual != t_expected
                    {
                        // FIXME: do not validate type-string now
                    }
                }

                match sort_mode.as_ref().or(self.sort_mode.as_ref()) {
                    None | Some(SortMode::NoSort) => {}
                    Some(SortMode::RowSort) => {
                        output.sort_unstable();
                        expected_results.sort_unstable();
                    }
                    Some(SortMode::ValueSort) => todo!("value sort"),
                };

                if self.hash_threshold > 0 && output.len() > self.hash_threshold {
                    let mut md5 = md5::Context::new();
                    for line in &output {
                        for value in line {
                            md5.consume(value.as_bytes());
                            md5.consume(b"\n");
                        }
                    }
                    let hash = md5.compute();
                    output = vec![vec![format!(
                        "{} values hashing to {:?}",
                        output.len() * output[0].len(),
                        hash
                    )]];
                }

                // We compare normalized results. Whitespace characters are ignored.
                let output = output
                    .into_iter()
                    .map(|strs| strs.iter().map(normalize_string).join(" "))
                    .collect_vec();
                let expected_results = expected_results.iter().map(normalize_string).collect_vec();

                if !(self.validator)(&output, &expected_results) {
                    return Err(TestErrorKind::QueryResultMismatch {
                        sql,
                        expected: expected_results.join("\n"),
                        actual: output.join("\n"),
                    }
                    .at(loc));
                }
                if let Some(hook) = &mut self.hook {
                    hook.on_query_complete(&sql).await;
                }
            }
            Record::Sleep { duration, .. } => D::sleep(duration).await,
            Record::Halt { .. } => {}
            Record::Subtest { .. } => {}
            Record::Include { loc, .. } => {
                unreachable!("include should be rewritten during link: at {}", loc)
            }
            Record::Control(control) => match control {
                Control::SortMode(sort_mode) => {
                    self.sort_mode = Some(sort_mode);
                }
                Control::BeginInclude(_) | Control::EndInclude(_) => {}
            },
            Record::HashThreshold { loc: _, threshold } => self.hash_threshold = threshold as usize,
        }
        Ok(())
    }

    /// Run a single record.
    pub fn run(&mut self, record: Record) -> Result<(), TestError> {
        futures::executor::block_on(self.run_async(record))
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    pub async fn run_multi_async(
        &mut self,
        records: impl IntoIterator<Item = Record>,
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
        records: impl IntoIterator<Item = Record>,
    ) -> Result<(), TestError> {
        block_on(self.run_multi_async(records))
    }

    /// Run a sqllogictest script.
    pub async fn run_script_async(&mut self, script: &str) -> Result<(), TestError> {
        let records = parse(script).expect("failed to parse sqllogictest");
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

    /// Run a sqllogictest file.
    pub fn run_file(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        block_on(self.run_file_async(filename))
    }

    /// aceept the tasks, spawn jobs task to run slt test. the tasks are (AsyncDB, slt filename)
    /// pairs.
    pub async fn run_parallel_async<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), ParallelTestError>
    where
        Fut: Future<Output = D>,
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

            self.db
                .run(&format!("CREATE DATABASE {};", db_name))
                .await
                .expect("create db failed");
            let target = hosts[idx % hosts.len()].clone();
            tasks.push(async move {
                let db = conn_builder(target, db_name).await;
                let mut tester = Runner::new(db);
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
        Fut: Future<Output = D>,
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

    /// Returns whether we should skip this record, according to given `conditions`.
    fn should_skip(&self, conditions: &[Condition]) -> bool {
        conditions
            .iter()
            .any(|c| c.should_skip(self.db.engine_name()))
    }

    /// Set hook functions.
    pub fn set_hook(&mut self, hook: impl Hook + 'static) {
        self.hook = Some(Box::new(hook));
    }
}

/// Trim and replace multiple whitespaces with one.
#[allow(clippy::ptr_arg)]
fn normalize_string(s: &String) -> String {
    s.trim().split_ascii_whitespace().join(" ")
}
