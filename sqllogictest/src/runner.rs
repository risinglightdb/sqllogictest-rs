//! Sqllogictest runner.

use std::fmt::Display;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;
use std::vec;

use async_trait::async_trait;
use futures::executor::block_on;
use futures::{stream, Future, StreamExt};
use itertools::Itertools;
use tempfile::{tempdir, TempDir};

use crate::parser::*;

/// The async database to be tested.
#[async_trait]
pub trait AsyncDB: Send {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + 'static;

    /// Async run a SQL query and return the output.
    async fn run(&mut self, sql: &str) -> Result<String, Self::Error>;

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
    type Error: std::error::Error + Send + 'static;

    /// Run a SQL query and return the output.
    fn run(&mut self, sql: &str) -> Result<String, Self::Error>;

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

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        <D as DB>::run(self, sql)
    }

    fn engine_name(&self) -> &str {
        <D as DB>::engine_name(self)
    }
}

/// The error type for running sqllogictest.
#[derive(thiserror::Error, Clone)]
#[error("test error at {loc}: {kind}")]
pub struct TestError {
    kind: TestErrorKind,
    loc: Location,
}

#[derive(thiserror::Error, Debug, Clone)]
#[error("test({filename}): {error}")]
// TODO(wrj): merge it to TestError
struct TestFileError {
    filename: String,
    error: String,
}

#[derive(Clone, Debug, thiserror::Error)]
pub struct ParallelTestError {
    errors: Vec<TestFileError>,
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

/// The error kind for running sqllogictest.
#[derive(thiserror::Error, Debug, Clone)]
pub enum TestErrorKind {
    #[error("parse error: {0}")]
    ParseError(ParseErrorKind),
    #[error("statement is expected to fail, but actually succeed:\n[SQL] {sql}")]
    StatementOk { sql: String },
    #[error("statement failed: {err}\n[SQL] {sql}")]
    StatementFail {
        sql: String,
        err: Rc<dyn std::error::Error>,
    },
    #[error("statement is expected to affect {expected} rows, but actually {actual}\n[SQL] {sql}")]
    StatementResultMismatch {
        sql: String,
        expected: u64,
        actual: String,
    },
    #[error("query failed: {err}\n[SQL] {sql}")]
    QueryFail {
        sql: String,
        err: Rc<dyn std::error::Error>,
    },
    #[error("query result mismatch:\n[SQL] {sql}\n[Diff]\n{}", difference::Changeset::new(.expected, .actual, "\n"))]
    QueryResultMismatch {
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
}

/// Validator will be used by `Runner` to validate the output.
///
/// # Default
///
/// By default, we will use `|x, y| x == y`.
pub type Validator = fn(&Vec<String>, &Vec<String>) -> bool;

/// Sqllogictest runner.
pub struct Runner<D: AsyncDB> {
    db: D,
    // validator is used for validate if the result of query equals to expected.
    validator: Validator,
    testdir: Option<TempDir>,
    sort_mode: Option<SortMode>,
}

impl<D: AsyncDB> Runner<D> {
    /// Create a new test runner on the database.
    pub fn new(db: D) -> Self {
        Runner {
            db,
            validator: |x, y| x == y,
            testdir: None,
            sort_mode: None,
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
        info!("test: {:?}", record);
        match record {
            Record::Statement { conditions, .. } if self.should_skip(&conditions) => {}
            Record::Statement {
                error,
                sql,
                loc,
                expected_count,
                ..
            } => {
                let sql = self.replace_keywords(sql);
                let ret = self.db.run(&sql).await;
                match ret {
                    Ok(_) if error => return Err(TestErrorKind::StatementOk { sql }.at(loc)),
                    Ok(count_str) => {
                        if let Some(expected_count) = expected_count {
                            if expected_count.to_string() != count_str {
                                return Err(TestErrorKind::StatementResultMismatch {
                                    sql,
                                    expected: expected_count,
                                    actual: count_str,
                                }
                                .at(loc));
                            }
                        }
                    }
                    Err(e) if !error => {
                        return Err(TestErrorKind::StatementFail {
                            sql,
                            err: Rc::new(e),
                        }
                        .at(loc));
                    }
                    _ => {}
                }
            }
            Record::Query { conditions, .. } if self.should_skip(&conditions) => {}
            Record::Query {
                loc,
                sql,
                expected_results,
                sort_mode,
                ..
            } => {
                let sql = self.replace_keywords(sql);
                let output = match self.db.run(&sql).await {
                    Ok(output) => output,
                    Err(e) => {
                        return Err(TestErrorKind::QueryFail {
                            sql,
                            err: Rc::new(e),
                        }
                        .at(loc));
                    }
                };
                let mut output = split_lines_and_normalize(&output);
                let mut expected_results = split_lines_and_normalize(&expected_results);
                match sort_mode.as_ref().or(self.sort_mode.as_ref()) {
                    None | Some(SortMode::NoSort) => {}
                    Some(SortMode::RowSort) => {
                        output.sort_unstable();
                        expected_results.sort_unstable();
                    }
                    Some(SortMode::ValueSort) => todo!("value sort"),
                };
                if !(self.validator)(&output, &expected_results) {
                    return Err(TestErrorKind::QueryResultMismatch {
                        sql,
                        expected: expected_results.join("\n"),
                        actual: output.join("\n"),
                    }
                    .at(loc));
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
            let db_name = db_name
                .replace(' ', "_")
                .replace('.', "_")
                .replace('-', "_");

            self.db
                .run(&format!("CREATE DATABASE {};", db_name))
                .await
                .expect("create db failed");
            let target = hosts[idx % hosts.len()].clone();
            tasks.push(async move {
                let db = conn_builder(target, db_name).await;
                let mut tester = Runner::new(db);
                let filename = file.to_string_lossy().to_string();
                (filename.clone(), tester.run_file_async(filename).await)
            })
        }

        let tasks = stream::iter(tasks).buffer_unordered(jobs);
        let errors: Vec<_> = tasks
            .map(|result| match result {
                (filename, Err(error)) => Some(TestFileError {
                    filename,
                    error: error.to_string(),
                }),
                _ => None,
            })
            .collect()
            .await;
        let errors = errors.into_iter().flatten().collect_vec();
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
