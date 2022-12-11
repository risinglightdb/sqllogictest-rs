//! Sqllogictest parser.
use derivative::Derivative;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::ColumnType;
use crate::ParseErrorKind::InvalidIncludeFile;
use itertools::Itertools;
use regex::Regex;

/// The location in source file.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Location {
    file: Arc<str>,
    line: u32,
    upper: Option<Arc<Location>>,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.line)?;
        if let Some(upper) = &self.upper {
            write!(f, "\nat {}", upper)?;
        }
        Ok(())
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

    fn new(file: impl Into<Arc<str>>, line: u32) -> Self {
        Self {
            file: file.into(),
            line,
            upper: None,
        }
    }

    /// Returns the location of next line.
    #[must_use]
    fn next_line(mut self) -> Self {
        self.line += 1;
        self
    }

    /// Returns the location of next level file.
    fn include(&self, file: &str) -> Self {
        Self {
            file: file.into(),
            line: 0,
            upper: Some(Arc::new(self.clone())),
        }
    }
}

/// A single directive in a sqllogictest file.
#[derive(Debug, Clone, Derivative)]
#[derivative(PartialEq)]
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
        /// The SQL command is expected to fail with an error messages that matches the given
        /// regex. If the regex is an empty string, any error message is accepted.
        #[derivative(PartialEq(compare_with = "cmp_regex"))]
        expected_error: Option<Regex>,
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
        type_string: Vec<ColumnType>,
        sort_mode: Option<SortMode>,
        label: Option<String>,
        /// The SQL command is expected to fail with an error messages that matches the given
        /// regex. If the regex is an empty string, any error message is accepted.
        #[derivative(PartialEq(compare_with = "cmp_regex"))]
        expected_error: Option<Regex>,
        /// The SQL command.
        sql: String,
        /// The expected results.
        expected_results: Vec<String>,
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
    /// Control statements.
    Control(Control),
    /// Set the maximum number of result values that will be accepted
    /// for a query.  If the number of result values exceeds this number,
    /// then an MD5 hash is computed of all values, and the resulting hash
    /// is the only result.
    ///
    /// If the threshold is 0, then hashing is never used.
    HashThreshold {
        loc: Location,
        threshold: u64,
    },
    Condition(Condition),
    Comment(Vec<String>),
    Newline,
    /// Internally injected record which should not occur in the test file.
    Injected(Injected),
}

/// Use string representation to determine if two regular
/// expressions came from the same text (rather than something
/// more deeply meaningful)
fn cmp_regex(l: &Option<Regex>, r: &Option<Regex>) -> bool {
    match (l, r) {
        (Some(l), Some(r)) => l.as_str().eq(r.as_str()),
        (None, None) => true,
        _ => false,
    }
}

impl Record {
    /// Unparses the record to its string representation in the test file.
    ///
    /// # Panics
    /// If the record is an internally injected record which should not occur in the test file.
    pub fn unparse(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        write!(w, "{}", self)
    }
}

/// As is the standard for Display, does not print any trailing
/// newline except for records that always end with a blank line such
/// as Query and Statement.
impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Record::Include { loc: _, filename } => {
                write!(f, "include {}", filename)
            }
            Record::Statement {
                loc: _,
                conditions: _,
                expected_error,
                sql,
                expected_count,
            } => {
                write!(f, "statement ")?;
                match (expected_count, expected_error) {
                    (None, None) => write!(f, "ok")?,
                    (None, Some(err)) => {
                        if err.as_str().is_empty() {
                            write!(f, "error")?;
                        } else {
                            write!(f, "error {}", err)?;
                        }
                    }
                    (Some(cnt), None) => write!(f, "count {}", cnt)?,
                    (Some(_), Some(_)) => unreachable!(),
                }
                writeln!(f)?;
                // statement always end with a blank line
                writeln!(f, "{}", sql)
            }
            Record::Query {
                loc: _,
                conditions: _,
                type_string,
                sort_mode,
                label,
                expected_error,
                sql,
                expected_results,
            } => {
                write!(f, "query")?;
                if let Some(err) = expected_error {
                    writeln!(f, " error {}", err)?;
                    return writeln!(f, "{}", sql);
                }

                write!(
                    f,
                    " {}",
                    type_string.iter().map(|c| format!("{c}")).join("")
                )?;
                if let Some(sort_mode) = sort_mode {
                    write!(f, " {}", sort_mode.as_str())?;
                }
                if let Some(label) = label {
                    write!(f, " {}", label)?;
                }
                writeln!(f)?;
                writeln!(f, "{}", sql)?;

                write!(f, "----")?;
                for result in expected_results {
                    write!(f, "\n{}", result)?;
                }
                // query always ends with a blank line
                writeln!(f)
            }
            Record::Sleep { loc: _, duration } => {
                write!(f, "sleep {}", humantime::format_duration(*duration))
            }
            Record::Subtest { loc: _, name } => {
                write!(f, "subtest {}", name)
            }
            Record::Halt { loc: _ } => {
                write!(f, "halt")
            }
            Record::Control(c) => match c {
                Control::SortMode(m) => write!(f, "control sortmode {}", m.as_str()),
            },
            Record::Condition(cond) => match cond {
                Condition::OnlyIf { engine_name } => {
                    write!(f, "onlyif {}", engine_name)
                }
                Condition::SkipIf { engine_name } => {
                    write!(f, "skipif {}", engine_name)
                }
            },
            Record::HashThreshold { loc: _, threshold } => {
                write!(f, "hash-threshold {}", threshold)
            }
            Record::Comment(comment) => {
                let mut iter = comment.iter();
                write!(f, "#{}", iter.next().unwrap().trim_end())?;
                for line in iter {
                    write!(f, "\n#{}", line.trim_end())?;
                }
                Ok(())
            }
            Record::Newline => Ok(()), // Display doesn't end with newline
            Record::Injected(p) => panic!("unexpected injected record: {:?}", p),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Control {
    /// Control sort mode.
    SortMode(SortMode),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Injected {
    /// Pseudo control command to indicate the begin of an include statement. Automatically
    /// injected by sqllogictest parser.
    BeginInclude(String),
    /// Pseudo control command to indicate the end of an include statement. Automatically injected
    /// by sqllogictest parser.
    EndInclude(String),
}

/// The condition to run a query.
#[derive(Debug, PartialEq, Eq, Clone)]
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
#[derive(Debug, PartialEq, Eq, Clone)]
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
#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
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
#[derive(thiserror::Error, Debug, Eq, PartialEq, Clone)]
pub enum ParseErrorKind {
    #[error("unexpected token: {0:?}")]
    UnexpectedToken(String),
    #[error("unexpected EOF")]
    UnexpectedEOF,
    #[error("invalid sort mode: {0:?}")]
    InvalidSortMode(String),
    #[error("invalid line: {0:?}")]
    InvalidLine(String),
    #[error("invalid type character: {0:?} in type string")]
    InvalidType(char),
    #[error("invalid number: {0:?}")]
    InvalidNumber(String),
    #[error("invalid error message: {0:?}")]
    InvalidErrorMessage(String),
    #[error("invalid duration: {0:?}")]
    InvalidDuration(String),
    #[error("invalid control: {0:?}")]
    InvalidControl(String),
    #[error("invalid include file pattern: {0:?}")]
    InvalidIncludeFile(String),
    #[error("no such file")]
    FileNotFound,
}

impl ParseErrorKind {
    fn at(self, loc: Location) -> ParseError {
        ParseError { kind: self, loc }
    }
}

/// Parse a sqllogictest script into a list of records.
pub fn parse(script: &str) -> Result<Vec<Record>, ParseError> {
    parse_inner(&Location::new("<unknown>", 0), script)
}

#[allow(clippy::collapsible_match)]
fn parse_inner(loc: &Location, script: &str) -> Result<Vec<Record>, ParseError> {
    let mut lines = script.split('\n').enumerate();
    let mut records = vec![];
    let mut conditions = vec![];

    while let Some((mut num, mut line)) = lines.next() {
        if let Some(text) = line.strip_prefix('#') {
            let mut comments = vec![text.to_string()];
            for (num_, line_) in lines.by_ref() {
                num = num_;
                line = line_;
                if let Some(text) = line.strip_prefix('#') {
                    comments.push(text.to_string());
                } else {
                    break;
                }
            }

            records.push(Record::Comment(comments));
        }

        if line.is_empty() {
            records.push(Record::Newline);
            continue;
        }

        let mut loc = loc.clone();
        loc.line = num as u32 + 1;

        let tokens: Vec<&str> = line.split_whitespace().collect();
        match tokens.as_slice() {
            [] => continue,
            ["include", included] => records.push(Record::Include {
                loc,
                filename: included.to_string(),
            }),
            ["halt"] => {
                records.push(Record::Halt { loc });
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
                let cond = Condition::SkipIf {
                    engine_name: engine_name.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["onlyif", engine_name] => {
                let cond = Condition::OnlyIf {
                    engine_name: engine_name.to_string(),
                };
                conditions.push(cond.clone());
                records.push(Record::Condition(cond));
            }
            ["statement", res @ ..] => {
                let mut expected_count = None;
                let mut expected_error = None;
                match res {
                    ["ok"] => {}
                    ["error", err_str @ ..] => {
                        let err_str = err_str.join(" ");
                        expected_error = Some(Regex::new(&err_str).map_err(|_| {
                            ParseErrorKind::InvalidErrorMessage(err_str).at(loc.clone())
                        })?);
                    }
                    ["count", count_str] => {
                        expected_count = Some(count_str.parse::<u64>().map_err(|_| {
                            ParseErrorKind::InvalidNumber((*count_str).into()).at(loc.clone())
                        })?);
                    }
                    _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
                };
                let mut sql = match lines.next() {
                    Some((_, line)) => line.into(),
                    None => return Err(ParseErrorKind::UnexpectedEOF.at(loc.next_line())),
                };
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
                    expected_error,
                    sql,
                    expected_count,
                });
            }
            ["query", res @ ..] => {
                let mut type_string = vec![];
                let mut sort_mode = None;
                let mut label = None;
                let mut expected_error = None;
                match res {
                    ["error", err_str @ ..] => {
                        let err_str = err_str.join(" ");
                        expected_error = Some(Regex::new(&err_str).map_err(|_| {
                            ParseErrorKind::InvalidErrorMessage(err_str).at(loc.clone())
                        })?);
                    }
                    [type_str, res @ ..] => {
                        type_string = type_str
                            .chars()
                            .map(ColumnType::try_from)
                            .try_collect()
                            .map_err(|e| e.at(loc.clone()))?;
                        sort_mode = res
                            .first()
                            .map(|&s| SortMode::try_from_str(s))
                            .transpose()
                            .map_err(|e| e.at(loc.clone()))?;
                        label = res.get(1).map(|s| s.to_string());
                    }
                    _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
                }

                // The SQL for the query is found on second an subsequent lines of the record
                // up to first line of the form "----" or until the end of the record.
                let mut sql = match lines.next() {
                    Some((_, line)) => line.into(),
                    None => return Err(ParseErrorKind::UnexpectedEOF.at(loc.next_line())),
                };
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
                let mut expected_results = vec![];
                if has_result {
                    for (_, line) in &mut lines {
                        if line.is_empty() {
                            break;
                        }
                        expected_results.push(line.to_string());
                    }
                }
                records.push(Record::Query {
                    loc,
                    conditions: std::mem::take(&mut conditions),
                    type_string,
                    sort_mode,
                    label,
                    sql,
                    expected_results,
                    expected_error,
                });
            }
            ["control", res @ ..] => match res {
                ["sortmode", sort_mode] => match SortMode::try_from_str(sort_mode) {
                    Ok(sort_mode) => records.push(Record::Control(Control::SortMode(sort_mode))),
                    Err(k) => return Err(k.at(loc)),
                },
                _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
            },
            ["hash-threshold", threshold] => {
                records.push(Record::HashThreshold {
                    loc: loc.clone(),
                    threshold: threshold.parse::<u64>().map_err(|_| {
                        ParseErrorKind::InvalidNumber((*threshold).into()).at(loc.clone())
                    })?,
                });
            }
            _ => return Err(ParseErrorKind::InvalidLine(line.into()).at(loc)),
        }
    }
    Ok(records)
}

/// Parse a sqllogictest file. The included scripts are inserted after the `include` record.
pub fn parse_file(filename: impl AsRef<Path>) -> Result<Vec<Record>, ParseError> {
    let filename = filename.as_ref().to_str().unwrap();
    parse_file_inner(Location::new(filename, 0))
}

fn parse_file_inner(loc: Location) -> Result<Vec<Record>, ParseError> {
    let path = Path::new(loc.file());
    if !path.exists() {
        return Err(ParseErrorKind::FileNotFound.at(loc.clone()));
    }
    let script = std::fs::read_to_string(path).unwrap();
    let mut records = vec![];
    for rec in parse_inner(&loc, &script)? {
        records.push(rec.clone());

        if let Record::Include { filename, loc } = rec {
            let complete_filename = {
                let mut path_buf = path.to_path_buf();
                path_buf.pop();
                path_buf.push(filename.clone());
                path_buf.as_os_str().to_string_lossy().to_string()
            };

            for included_file in glob::glob(&complete_filename)
                .map_err(|e| InvalidIncludeFile(format!("{:?}", e)).at(loc.clone()))?
                .filter_map(Result::ok)
            {
                let included_file = included_file.as_os_str().to_string_lossy().to_string();

                records.push(Record::Injected(Injected::BeginInclude(
                    included_file.clone(),
                )));
                records.extend(parse_file_inner(loc.include(&included_file))?);
                records.push(Record::Injected(Injected::EndInclude(included_file)));
            }
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_include_glob() {
        let records = parse_file("../examples/include/include_1.slt").unwrap();
        assert_eq!(16, records.len());
    }

    #[test]
    fn test_basic() {
        parse_roundtrip("../examples/basic/basic.slt")
    }

    #[test]
    fn test_condition() {
        parse_roundtrip("../examples/condition/condition.slt")
    }

    #[test]
    fn test_file_level_sort_mode() {
        parse_roundtrip("../examples/file_level_sort_mode/file_level_sort_mode.slt")
    }

    #[test]
    fn test_rowsort() {
        parse_roundtrip("../examples/rowsort/rowsort.slt")
    }

    #[test]
    fn test_test_dir_escape() {
        parse_roundtrip("../examples/test_dir_escape/test_dir_escape.slt")
    }

    #[test]
    fn test_validator() {
        parse_roundtrip("../examples/validator/validator.slt")
    }

    /// Verifies Display impl is consistent with parsing by ensuring
    /// roundtrip parse(unparse(parse())) is consistent
    fn parse_roundtrip(filename: impl AsRef<Path>) {
        let filename = filename.as_ref();
        let records = parse_file(filename).expect("parsing to complete");

        let unparsed = records
            .iter()
            .map(|record| record.to_string())
            .collect::<Vec<_>>();

        let output_contents = unparsed.join("\n");

        // The orignal and parsed records should be logically requivalent
        let mut output_file = tempfile::NamedTempFile::new().expect("Error creating tempfile");
        output_file
            .write_all(output_contents.as_bytes())
            .expect("Unable to write file");
        output_file.flush().unwrap();

        let output_path = output_file.into_temp_path();
        let reparsed_records =
            parse_file(&output_path).expect("reparsing to complete successfully");

        let records = normalize_filename(records);
        let reparsed_records = normalize_filename(reparsed_records);

        assert_eq!(
            records, reparsed_records,
            "Mismatch in reparsed records\n\
                    *********\n\
                    original:\n\
                    *********\n\
                    {:#?}\n\n\
                    *********\n\
                    reparsed:\n\
                    *********\n\
                    {:#?}\n\n",
            records, reparsed_records,
        );
    }

    /// Replaces the actual filename in all Records with
    /// "__FILENAME__" so different files with the same contents can
    /// compare equal
    fn normalize_filename(records: Vec<Record>) -> Vec<Record> {
        records
            .into_iter()
            .map(|mut record| {
                match &mut record {
                    Record::Include { loc, .. } => normalize_loc(loc),
                    Record::Statement { loc, .. } => normalize_loc(loc),
                    Record::Query { loc, .. } => normalize_loc(loc),
                    Record::Sleep { loc, .. } => normalize_loc(loc),
                    Record::Subtest { loc, .. } => normalize_loc(loc),
                    Record::Halt { loc, .. } => normalize_loc(loc),
                    Record::HashThreshold { loc, .. } => normalize_loc(loc),
                    // even though these variants don't include a
                    // location include them in this match statement
                    // so if new variants are added, this match
                    // statement must be too.
                    Record::Condition(_)
                    | Record::Comment(_)
                    | Record::Control(_)
                    | Record::Newline
                    | Record::Injected(_) => {}
                };
                record
            })
            .collect()
    }

    // Normalize a location
    fn normalize_loc(loc: &mut Location) {
        loc.file = Arc::from("__FILENAME__");
    }
}
