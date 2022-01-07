use libtest_mimic::{run_tests, Arguments, Outcome, Test};
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
    #[structopt(short, long)]
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
        use std::fmt::Write;

        let mut output = String::new();
        // NOTE:
        // We use `simple_query` API which returns the query results as strings.
        // This means that we can not reformat values based on their type,
        // and we have to follow the format given by the specific database (pg).
        // For example, postgres will output `t` as true and `f` as false,
        // thus we have to write `t`/`f` in the expected results.
        let rows = self.client.lock().unwrap().simple_query(sql)?;
        for row in rows {
            match row {
                postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        if i != 0 {
                            write!(output, " ").unwrap();
                        }
                        match row.get(i) {
                            Some(v) => write!(output, "{}", v).unwrap(),
                            None => write!(output, "NULL").unwrap(),
                        }
                    }
                }
                postgres::SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }
}
