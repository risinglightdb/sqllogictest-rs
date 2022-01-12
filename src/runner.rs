//! Sqllogictest runner.

use crate::parser::*;
use async_trait::async_trait;
use itertools::Itertools;
use std::path::Path;
use tempfile::{tempdir, TempDir};

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
