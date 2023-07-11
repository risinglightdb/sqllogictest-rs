use rusqlite::{types::ValueRef, Connection, Error};

use sqllogictest::{harness, ColumnType, DBOutput, Runner, DB};

fn hash_threshold(filename: &str) -> usize {
    match filename {
        "sqlite/select1.test" => 8,
        "sqlite/select4.test" => 8,
        "sqlite/select5.test" => 8,
        _ => 0,
    }
}

fn main() {
    let paths = harness::glob("sqllogictest-sqlite/test/**/select*.test").unwrap();
    let mut tests = vec![];
    for entry in paths {
        let path = entry.unwrap();
        let filename = path.to_str().unwrap().to_string();
        tests.push(harness::Trial::test(filename.clone(), move || {
            let mut tester = Runner::new(db_fn());
            tester.with_hash_threshold(hash_threshold(&filename));
            tester.run_file(path)?;
            Ok(())
        }));
    }
    harness::run(&harness::Arguments::from_args(), tests).exit();
}

struct ConnectionWrapper(Connection);

fn db_fn() -> ConnectionWrapper {
    let c = Connection::open_in_memory().unwrap();
    ConnectionWrapper(c)
}

fn value_to_string(v: ValueRef) -> String {
    match v {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(r) => r.to_string(),
        ValueRef::Text(s) => std::str::from_utf8(s).unwrap().to_string(),
        ValueRef::Blob(_) => todo!(),
    }
}

impl DB for ConnectionWrapper {
    type Error = Error;

    fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        let mut output = vec![];

        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };

        if is_query_sql {
            let mut stmt = self.0.prepare(sql)?;
            let column_count = stmt.column_count();
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let mut row_output = vec![];
                for i in 0..column_count {
                    let row = row.get_ref(i)?;
                    row_output.push(value_to_string(row));
                }
                output.push(row_output);
            }
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Any; column_count],
                rows: output,
            })
        } else {
            let cnt = self.0.execute(sql, [])?;
            Ok(DBOutput::StatementComplete(cnt as u64))
        }
    }

    fn engine_name(&self) -> &str {
        "sqlite"
    }
}
