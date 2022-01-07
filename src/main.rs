use libtest_mimic::{run_tests, Arguments, Outcome, Test};
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt()]
struct Opt {
    /// Port of the remote DB server.
    #[structopt(short, long, default_value = "5432")]
    port: u16,

    /// Glob of a set of test files.
    ///
    /// For example: `./test/**/*.slt`
    #[structopt(long)]
    files: String,

    /// The database name to connect.
    #[structopt(long, default_value = "postgres")]
    pgdb: String,

    /// The database username.
    #[structopt(long, default_value = "postgres")]
    pguser: String,

    /// The database password.
    #[structopt(long, default_value = "postgres")]
    pgpass: String,
    // /// The arguments to test harness.
    // #[structopt(long, default_value = "")]
    // test_args: String,
}

fn main() {
    env_logger::init();

    let opt = Opt::from_args();

    let files = glob::glob(&opt.files).expect("failed to read glob pattern");
    let client = postgres::Config::new()
        .user(&opt.pguser)
        .password(&opt.pgpass)
        .dbname(&opt.pgdb)
        .host("localhost")
        .port(opt.port)
        .connect(postgres::NoTls)
        .expect("failed to connect to postgres");

    let pg = Postgres {
        client: Arc::new(Mutex::new(client)),
    };
    let tests = files
        .map(|file| Test {
            name: file.unwrap().to_str().unwrap().into(),
            kind: String::new(),
            is_ignored: false,
            is_bench: false,
            data: pg.clone(),
        })
        .collect();

    // Parse command line arguments
    let mut args = Arguments::from_iter(std::env::args().take(1));
    args.num_threads = Some(1);

    // Run all tests and exit the application appropriatly (in this case, the
    // test runner is a dummy runner which does nothing and says that all tests
    // passed).
    run_tests(&args, tests, run_test).exit();
}

fn run_test(test: &Test<Postgres>) -> Outcome {
    let mut runner = sqllogictest::Runner::new(test.data.clone());
    runner.run_file(&test.name);
    Outcome::Passed
}

#[derive(Clone)]
struct Postgres {
    client: Arc<Mutex<postgres::Client>>,
}

impl sqllogictest::DB for Postgres {
    type Error = postgres::error::Error;

    fn run(&self, sql: &str) -> Result<String, Self::Error> {
        use postgres::types::Type;
        use std::fmt::Write;

        let mut output = String::new();
        let rows = self.client.lock().unwrap().query(sql, &[])?;
        for row in rows {
            let columns = row.columns();
            for (i, col) in columns.iter().enumerate() {
                write!(output, " ").unwrap();
                match col.type_() {
                    &Type::BOOL => match row.get::<_, Option<bool>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::CHAR => match row.get::<_, Option<i8>>(i) {
                        Some(v) => write!(output, "{}", v as u8 as char),
                        None => write!(output, "NULL"),
                    },
                    &Type::INT2 => match row.get::<_, Option<i16>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::INT4 => match row.get::<_, Option<i32>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::INT8 => match row.get::<_, Option<i64>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::FLOAT4 => match row.get::<_, Option<f32>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::FLOAT8 => match row.get::<_, Option<f64>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::TEXT | &Type::BPCHAR | &Type::VARCHAR => {
                        match row.get::<_, Option<&str>>(i) {
                            Some(v) => write!(output, "{}", v),
                            None => write!(output, "NULL"),
                        }
                    }
                    &Type::NUMERIC => match row.get::<_, Option<Decimal>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::DATE => match row.get::<_, Option<chrono::NaiveDate>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::TIME => match row.get::<_, Option<chrono::NaiveTime>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::TIMESTAMP => match row.get::<_, Option<chrono::NaiveDateTime>>(i) {
                        Some(v) => write!(output, "{}", v),
                        None => write!(output, "NULL"),
                    },
                    &Type::TIMESTAMPTZ => {
                        match row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(i) {
                            Some(v) => write!(output, "{}", v),
                            None => write!(output, "NULL"),
                        }
                    }
                    t => todo!("not supported type: {}", t),
                }
                .unwrap();
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }
}
