use crate::parser::*;
use std::io::{Result, Write};

/// Format a sqllogictest script.
pub fn format(script: &str) -> String {
    let mut formatted = vec![];
    let records = parse(script).expect("failed to parse");
    for record in records {
        record.fmt(&mut formatted).expect("failed to format");
    }
    String::from_utf8(formatted).expect("invalid utf8")
}

impl Record {
    fn fmt(&self, f: &mut impl Write) -> Result<()> {
        match self {
            Record::Comment { text, .. } => {
                writeln!(f, "#{}", text.trim_end())?;
            }
            Record::Include { filename, .. } => {
                writeln!(f, "include {filename}")?;
            }
            Record::Statement {
                conditions,
                error,
                expected_count,
                sql,
                ..
            } => {
                for condition in conditions {
                    condition.fmt(f)?;
                }
                write!(f, "statement ")?;
                if *error {
                    writeln!(f, "error")?;
                } else if let Some(count) = expected_count {
                    writeln!(f, "count {count}")?;
                } else {
                    writeln!(f, "ok")?;
                }
                let formatted_sql = sqlformat::format(
                    sql,
                    &sqlformat::QueryParams::None,
                    sqlformat::FormatOptions::default(),
                );
                writeln!(f, "{formatted_sql}\n")?;
            }
            Record::Query {
                conditions,
                type_string,
                sort_mode,
                label,
                sql,
                expected_results,
                ..
            } => {
                for condition in conditions {
                    condition.fmt(f)?;
                }
                write!(f, "query {type_string}")?;
                if let Some(mode) = sort_mode {
                    write!(f, " {mode}")?;
                }
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                let formatted_sql = sqlformat::format(
                    sql,
                    &sqlformat::QueryParams::None,
                    sqlformat::FormatOptions::default(),
                );
                writeln!(f, "\n{formatted_sql}")?;
                writeln!(f, "----\n{expected_results}\n")?;
            }
            Record::Sleep { duration, .. } => {
                writeln!(f, "{duration:?}\n")?;
            }
            Record::Subtest { name, .. } => {
                writeln!(f, "subtest {name}\n")?;
            }
            Record::Halt { .. } => {
                writeln!(f, "halt\n")?;
            }
            Record::Control(_) => todo!(),
        }
        Ok(())
    }
}

impl Condition {
    fn fmt(&self, f: &mut impl Write) -> Result<()> {
        match self {
            Condition::OnlyIf { engine_name } => writeln!(f, "onlyif {}", engine_name),
            Condition::SkipIf { engine_name } => writeln!(f, "skipif {}", engine_name),
        }
    }
}
