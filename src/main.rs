use std::collections::BTreeMap;
use std::io::{stdout, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use clap::{ArgEnum, Parser};
use console::style;
use futures::StreamExt;
use itertools::Itertools;
use sqllogictest::{Control, Record};

#[derive(Copy, Clone, Debug, PartialEq, ArgEnum)]
#[must_use]
pub enum Color {
    Auto,
    Always,
    Never,
}

impl Default for Color {
    fn default() -> Self {
        Color::Auto
    }
}

#[derive(Parser, Debug, Clone)]
#[clap(about, version, author)]
struct Opt {
    /// Glob of a set of test files.
    /// For example: `./test/**/*.slt`
    #[clap()]
    files: String,

    /// The database engine name, used by the record conditions.
    #[clap(short, long, default_value = "postgresql")]
    engine: String,

    /// The database server host.
    #[clap(short, long, default_value = "localhost")]
    host: String,

    /// The database server port.
    #[clap(short, long, default_value = "5432")]
    port: u16,

    /// The database name to connect.
    #[clap(short, long, default_value = "postgres")]
    db: String,

    /// The database username.
    #[clap(short, long, default_value = "postgres")]
    user: String,

    /// The database password.
    #[clap(short = 'w', long, default_value = "postgres")]
    pass: String,

    #[clap(
        long,
        arg_enum,
        default_value_t,
        value_name = "WHEN",
        env = "CARGO_TERM_COLOR"
    )]
    color: Color,

    /// Whether to enable parallel test. The `db` option will be used to create databases, and one
    /// database will be created for each test file.
    #[clap(long, short)]
    jobs: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let opt = Opt::parse();

    match opt.color {
        Color::Always => {
            console::set_colors_enabled(true);
            console::set_colors_enabled_stderr(true);
        }
        Color::Never => {
            console::set_colors_enabled(false);
            console::set_colors_enabled_stderr(false);
        }
        Color::Auto => {}
    }

    let files = glob::glob(&opt.files).expect("failed to read glob pattern");

    let (client, connection) = tokio_postgres::Config::new()
        .host(&opt.host)
        .port(opt.port)
        .dbname(&opt.db)
        .user(&opt.user)
        .password(&opt.pass)
        .connect(tokio_postgres::NoTls)
        .await
        .context("failed to connect to postgres")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("Postgres connection error: {:?}", e);
        }
    });

    let pg = Postgres {
        client: Arc::new(client),
        engine_name: opt.engine.clone(),
    };

    let files = files.into_iter().try_collect::<_, Vec<_>, _>()?;

    if files.is_empty() {
        return Err(anyhow!("no test case found"));
    }

    if let Some(job) = &opt.jobs {
        let mut create_databases = BTreeMap::new();
        for file in files {
            let db_name = file
                .file_name()
                .ok_or_else(|| anyhow!("not a valid filename"))?
                .to_str()
                .ok_or_else(|| anyhow!("not a UTF-8 filename"))?;
            let db_name = db_name
                .replace(' ', "_")
                .replace('.', "_")
                .replace('-', "_");
            eprintln!("+ Discovered Test: {}", db_name);
            if create_databases.insert(db_name.to_string(), file).is_some() {
                return Err(anyhow!("duplicated file name found: {}", db_name));
            }
        }

        for db_name in create_databases.keys() {
            let query = format!("CREATE DATABASE {};", db_name);
            eprintln!("+ {}", query);
            if let Err(err) = pg.client.simple_query(&query).await {
                eprintln!("  ignore error: {}", err);
            }
        }

        let mut stream = futures::stream::iter(create_databases.into_iter())
            .map(|(db_name, filename)| {
                let opt = opt.clone();
                let file = filename.to_string_lossy().to_string();
                async move {
                    let (buf, res) = tokio::spawn(async {
                        let mut buf = vec![];
                        let res = run_test_file_on_db(&mut buf, filename, db_name, opt).await;
                        (buf, res)
                    })
                    .await
                    .unwrap();
                    (file, res, buf)
                }
            })
            .buffer_unordered(*job);

        eprintln!("{}", style("[TEST IN PROGRESS]").blue().bold());

        let mut failed_case = vec![];

        let start = Instant::now();

        while let Some((file, res, mut buf)) = stream.next().await {
            if let Err(e) = res {
                writeln!(buf, "{}\n\n{:?}", style("[FAILED]").red().bold(), e)?;
                writeln!(buf)?;
                failed_case.push(file);
            }
            tokio::task::block_in_place(|| stdout().write_all(&buf))?;
        }

        eprintln!(
            "\n All test cases finished in {} ms",
            start.elapsed().as_millis()
        );

        if !failed_case.is_empty() {
            Err(anyhow!("some test case failed:\n{:#?}", failed_case))
        } else {
            Ok(())
        }
    } else {
        // Run test one be one

        let mut failed_case = vec![];

        for file in files {
            if let Err(e) = run_test_file(&mut std::io::stdout(), pg.clone(), &file).await {
                println!("{}\n\n{:?}", style("[FAILED]").red().bold(), e);
                println!();
                failed_case.push(file.to_string_lossy().to_string());
            }
        }

        if !failed_case.is_empty() {
            Err(anyhow!("some test case failed:\n{:#?}", failed_case))
        } else {
            Ok(())
        }
    }
}

async fn flush(out: &mut impl std::io::Write) -> std::io::Result<()> {
    tokio::task::block_in_place(|| out.flush())
}

async fn run_test_file_on_db(
    out: &mut impl std::io::Write,
    filename: PathBuf,
    db_name: String,
    opt: Opt,
) -> Result<()> {
    let (client, connection) = tokio_postgres::Config::new()
        .host(&opt.host)
        .port(opt.port)
        .dbname(&db_name)
        .user(&opt.user)
        .password(&opt.pass)
        .connect(tokio_postgres::NoTls)
        .await
        .context("failed to connect to postgres")?;

    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("Postgres connection error: {:?}", e);
        }
    });

    let pg = Postgres {
        client: Arc::new(client),
        engine_name: opt.engine.clone(),
    };

    run_test_file(out, pg, filename).await?;

    handle.abort();

    Ok(())
}

async fn run_test_file<T: std::io::Write>(
    out: &mut T,
    engine: Postgres,
    filename: impl AsRef<Path>,
) -> Result<()> {
    let filename = filename.as_ref();
    let mut runner = sqllogictest::Runner::new(engine);
    let records = tokio::task::block_in_place(|| {
        sqllogictest::parse_file(&filename).map_err(|e| anyhow!("{:?}", e))
    })
    .context("failed to parse sqllogictest file")?;

    let mut begin_times = vec![];
    let mut did_pop = false;

    write!(out, "{: <60} .. ", filename.to_string_lossy())?;
    flush(out).await?;

    begin_times.push(Instant::now());

    let finish = |out: &mut T, time_stack: &mut Vec<Instant>, did_pop: &mut bool, file: &str| {
        let begin_time = time_stack.pop().unwrap();

        if *did_pop {
            // start a new line if the result is not immediately after the item
            write!(
                out,
                "\n{}{} {: <54} .. {} in {} ms",
                "| ".repeat(time_stack.len()),
                style("[END]").blue().bold(),
                file,
                style("[OK]").green().bold(),
                begin_time.elapsed().as_millis()
            )?;
        } else {
            // otherwise, append time to the previous line
            write!(
                out,
                "{} in {} ms",
                style("[OK]").green().bold(),
                begin_time.elapsed().as_millis()
            )?;
        }

        *did_pop = true;

        Ok::<_, anyhow::Error>(())
    };

    for record in records {
        match &record {
            Record::Control(Control::BeginInclude(file)) => {
                begin_times.push(Instant::now());
                if !did_pop {
                    writeln!(out, "{}", style("[BEGIN]").blue().bold())?;
                } else {
                    writeln!(out)?;
                }
                did_pop = false;
                write!(
                    out,
                    "{}{: <60} .. ",
                    "| ".repeat(begin_times.len() - 1),
                    file
                )?;
                flush(out).await?;
            }
            Record::Control(Control::EndInclude(file)) => {
                finish(out, &mut begin_times, &mut did_pop, file)?;
            }
            _ => {}
        }
        runner
            .run_async(record)
            .await
            .map_err(|e| anyhow!("{:?}", e))
            .context(format!(
                "failed to run `{}`",
                style(filename.to_string_lossy()).bold()
            ))?;
    }

    finish(
        out,
        &mut begin_times,
        &mut did_pop,
        &*filename.to_string_lossy(),
    )?;

    writeln!(out)?;

    Ok(())
}

#[derive(Clone)]
struct Postgres {
    client: Arc<tokio_postgres::Client>,
    engine_name: String,
}

#[async_trait]
impl sqllogictest::AsyncDB for Postgres {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        use std::fmt::Write;

        let mut output = String::new();
        // NOTE:
        // We use `simple_query` API which returns the query results as strings.
        // This means that we can not reformat values based on their type,
        // and we have to follow the format given by the specific database (pg).
        // For example, postgres will output `t` as true and `f` as false,
        // thus we have to write `t`/`f` in the expected results.
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        if i != 0 {
                            write!(output, " ").unwrap();
                        }
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    write!(output, "(empty)").unwrap()
                                } else {
                                    write!(output, "{}", v).unwrap()
                                }
                            }
                            None => write!(output, "NULL").unwrap(),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }

    fn engine_name(&self) -> &str {
        &self.engine_name
    }
}
