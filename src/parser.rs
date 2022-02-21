//! Sqllogictest parser.

use std::rc::Rc;
use std::time::Duration;
use std::{fmt, path::Path};

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
    Include {
        loc: Location,
        filename: String,
    },
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
        expected_count: Option<u64>,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        loc: Location,
        conditions: Vec<Condition>,
        type_string: String,
        sort_mode: Option<SortMode>,
        label: Option<String>,
        /// The SQL command.
        sql: String,
        /// The expected results.
        expected_results: String,
    },
    /// A sleep period.
    Sleep {
        loc: Location,
        duration: Duration,
    },
    /// Subtest.
    Subtest {
        loc: Location,
        name: String,
    },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt {
        loc: Location,
    },
    Control(Control),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Control {
    SortMode(SortMode),
}

/// The condition to run a query.
#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    /// The statement or query is skipped if an `onlyif` record for a different database engine is
    /// seen.
    OnlyIf { engine_name: String },
    /// The statement or query is not evaluated if a `skipif` record for the target database engine
    /// is seen in the prefix.
    SkipIf { engine_name: String },
}

impl Condition {
    /// Evaluate condition on given `targe_name`, returns whether to skip this record.
    pub fn should_skip(&self, target_name: &str) -> bool {
        match self {
            Condition::OnlyIf { engine_name } => engine_name != target_name,
            Condition::SkipIf { engine_name } => engine_name == target_name,
        }
    }
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

impl SortMode {
    pub fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "nosort" => Ok(Self::NoSort),
            "rowsort" => Ok(Self::RowSort),
            "valuesort" => Ok(Self::ValueSort),
            _ => Err(ParseErrorKind::InvalidSortMode(s.to_string())),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoSort => "nosort",
            Self::RowSort => "rowsort",
            Self::ValueSort => "valuesort",
        }
    }
}

/// The error type for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Clone)]
#[error("parse error at {loc}: {kind}")]
pub struct ParseError {
    kind: ParseErrorKind,
    loc: Location,
}

impl ParseError {
    /// Returns the corresponding [`ParseErrorKind`] for this error.
    pub fn kind(&self) -> ParseErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
    }
}

/// The error kind for parsing sqllogictest.
#[derive(thiserror::Error, Debug, PartialEq, Clone)]
pub enum ParseErrorKind {
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
    #[error("invalid control: {0:?}")]
    InvalidControl(String),
}

impl ParseErrorKind {
    fn at(self, loc: Location) -> ParseError {
        ParseError { kind: self, loc }
    }
}

const DEFAULT_FILENAME: &str = "<entry>";

/// Parse a sqllogictest script into a list of records.
pub fn parse(script: &str) -> Result<Vec<Record>, ParseError> {
    parse_inner(Rc::from(DEFAULT_FILENAME), script)
}

#[allow(clippy::collapsible_match)]
fn parse_inner(filename: Rc<str>, script: &str) -> Result<Vec<Record>, ParseError> {
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
                    duration: humantime::parse_duration(dur).map_err(|_| {
                        ParseErrorKind::InvalidDuration(dur.to_string()).at(loc.clone())
                    })?,
                    loc,
                });
            }
            ["skipif", engine_name] => {
                conditions.push(Condition::SkipIf {
                    engine_name: engine_name.to_string(),
                });
            }
            ["onlyif", engine_name] => {
                conditions.push(Condition::OnlyIf {
                    engine_name: engine_name.to_string(),
                });
            }
            ["statement", res @ ..] => {
                let mut expected_count = None;
                let error = match res {
                    ["ok"] => false,
                    ["error"] => true,
                    ["count", count_str] => {
                        expected_count = Some(count_str.parse::<u64>().map_err(|_| {
                            ParseErrorKind::InvalidNumber((*count_str).into()).at(loc.clone())
                        })?);
                        false
                    }
                    _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
                };
                let mut sql = lines
                    .next()
                    .ok_or_else(|| {
                        ParseErrorKind::UnexpectedEOF.at(loc.clone().map_line(|line| line + 1))
                    })?
                    .1
                    .into();
                for (_, line) in &mut lines {
                    if line.is_empty() {
                        break;
                    }
                    sql += "\n";
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
                let sort_mode = match res.get(0).map(|&s| SortMode::try_from_str(s)).transpose() {
                    Ok(sm) => sm,
                    Err(k) => return Err(k.at(loc)),
                };
                let label = res.get(1).map(|s| s.to_string());
                // The SQL for the query is found on second an subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let mut sql = lines
                    .next()
                    .ok_or_else(|| {
                        ParseErrorKind::UnexpectedEOF.at(loc.clone().map_line(|line| line + 1))
                    })?
                    .1
                    .into();
                let mut has_result = false;
                for (_, line) in &mut lines {
                    if line.is_empty() {
                        break;
                    }
                    if line == "----" {
                        has_result = true;
                        break;
                    }
                    sql += "\n";
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
            ["control", res @ ..] => match res {
                ["sortmode", sort_mode] => match SortMode::try_from_str(sort_mode) {
                    Ok(sort_mode) => records.push(Record::Control(Control::SortMode(sort_mode))),
                    Err(k) => return Err(k.at(loc)),
                },
                _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
            },
            _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
        }
    }
    Ok(records)
}

/// Parse a sqllogictest file and link all included scripts together.
pub fn parse_file(filename: impl AsRef<Path>) -> Result<Vec<Record>, ParseError> {
    parse_file_inner(
        Rc::from(filename.as_ref().to_str().unwrap()),
        filename.as_ref(),
    )
}

fn parse_file_inner(filename: Rc<str>, path: &Path) -> Result<Vec<Record>, ParseError> {
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
