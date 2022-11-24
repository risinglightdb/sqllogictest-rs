mod engines;

use std::collections::BTreeMap;
use std::io::{stdout, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Local;
use clap::{ArgEnum, Parser};
use console::style;
use engines::{EngineConfig, EngineType};
use futures::StreamExt;
use itertools::Itertools;
use quick_junit::{NonSuccessKind, Report, TestCase, TestCaseStatus, TestSuite};
use rand::seq::SliceRandom;
use sqllogictest::{AsyncDB, Control, Record, Runner};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ArgEnum)]
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
    #[clap(short, long, arg_enum, default_value = "postgres")]
    engine: EngineType,

    /// Example: "java -cp a.jar com.risingwave.sqllogictest.App
    /// jdbc:postgresql://{host}:{port}/{db} {user}" The items in `{}` will be replaced by
    /// [`DBConfig`].
    #[clap(long, env)]
    external_engine_command_template: Option<String>,

    /// Whether to enable colorful output.
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

    /// Report to junit XML.
    #[clap(long)]
    junit: Option<String>,

    /// The database server host.
    /// If multiple addresses are specified, one will be chosen randomly per session.
    #[clap(short, long, default_value = "localhost")]
    host: Vec<String>,
    /// The database server port.
    /// If multiple addresses are specified, one will be chosen randomly per session.
    #[clap(short, long, default_value = "5432")]
    port: Vec<u16>,
    /// The database name to connect.
    #[clap(short, long, default_value = "postgres")]
    db: String,
    /// The database username.
    #[clap(short, long, default_value = "postgres")]
    user: String,
    /// The database password.
    #[clap(short = 'w', long, default_value = "postgres")]
    pass: String,
}

/// Connection configuration.
#[derive(Clone)]
struct DBConfig {
    /// The database server host and port. Will randomly choose one if multiple are given.
    addrs: Vec<(String, u16)>,
    /// The database name to connect.
    db: String,
    /// The database username.
    user: String,
    /// The database password.
    pass: String,
}

impl DBConfig {
    fn random_addr(&self) -> (&str, u16) {
        self.addrs
            .choose(&mut rand::thread_rng())
            .map(|(host, port)| (host.as_ref(), *port))
            .unwrap()
    }
}

pub async fn main_okk() -> Result<()> {
    env_logger::init();

    let Opt {
        files,
        engine,
        external_engine_command_template,
        color,
        jobs,
        junit,
        host,
        port,
        db,
        user,
        pass,
    } = Opt::parse();

    if host.len() != port.len() {
        bail!(
            "{} hosts are provided while {} ports are provided",
            host.len(),
            port.len(),
        );
    }
    let addrs = host.into_iter().zip_eq(port).collect();

    let engine = match engine {
        EngineType::Postgres => EngineConfig::Postgres,
        EngineType::PostgresExtended => EngineConfig::PostgresExtended,
        EngineType::External => {
            if let Some(external_engine_command_template) = external_engine_command_template {
                EngineConfig::External(external_engine_command_template)
            } else {
                bail!("`--external-engine-command-template` is required for `--engine=external`")
            }
        }
    };

    match color {
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

    let files = glob::glob(&files).context("failed to read glob pattern")?;
    let files = files.into_iter().try_collect::<_, Vec<_>, _>()?;
    if files.is_empty() {
        bail!("no test case found");
    }

    let config = DBConfig {
        addrs,
        db,
        user,
        pass,
    };

    let mut report = Report::new(junit.clone().unwrap_or_else(|| "sqllogictest".to_string()));
    report.set_timestamp(Local::now());

    let mut test_suite = TestSuite::new("sqllogictest");
    test_suite.set_timestamp(Local::now());

    let result = if let Some(jobs) = jobs {
        run_parallel(jobs, &mut test_suite, files, &engine, config, junit.clone()).await
    } else {
        run_serial(&mut test_suite, files, &engine, config, junit.clone()).await
    };

    report.add_test_suite(test_suite);

    if let Some(junit_file) = junit {
        tokio::fs::write(format!("{}-junit.xml", junit_file), report.to_string()?).await?;
    }

    result
}

async fn run_parallel(
    jobs: usize,
    test_suite: &mut TestSuite,
    files: Vec<PathBuf>,
    engine: &EngineConfig,
    config: DBConfig,
    junit: Option<String>,
) -> Result<()> {
    let mut create_databases = BTreeMap::new();
    for file in files {
        let db_name = file
            .file_name()
            .ok_or_else(|| anyhow!("not a valid filename"))?
            .to_str()
            .ok_or_else(|| anyhow!("not a UTF-8 filename"))?;
        let db_name = db_name.replace([' ', '.', '-'], "_");
        eprintln!("+ Discovered Test: {}", db_name);
        if create_databases.insert(db_name.to_string(), file).is_some() {
            return Err(anyhow!("duplicated file name found: {}", db_name));
        }
    }

    let mut db = engines::connect(engine, &config).await?;

    let db_names: Vec<String> = create_databases.keys().cloned().collect();
    for db_name in &db_names {
        let query = format!("CREATE DATABASE {};", db_name);
        eprintln!("+ {}", query);
        if let Err(err) = db.run(&query).await {
            eprintln!("  ignore error: {}", err);
        }
    }

    let mut stream = futures::stream::iter(create_databases.into_iter())
        .map(|(db_name, filename)| {
            let mut config = config.clone();
            config.db = db_name;
            let file = filename.to_string_lossy().to_string();
            let engine = engine.clone();
            async move {
                let (buf, res) = tokio::spawn(async move {
                    let mut buf = vec![];
                    let res = connect_and_run_test_file(&mut buf, filename, &engine, config).await;
                    (buf, res)
                })
                .await
                .unwrap();
                (file, res, buf)
            }
        })
        .buffer_unordered(jobs);

    eprintln!("{}", style("[TEST IN PROGRESS]").blue().bold());

    let mut failed_case = vec![];

    let start = Instant::now();

    while let Some((file, res, mut buf)) = stream.next().await {
        let test_case_name = file.replace(['/', ' ', '.', '-'], "_");
        let case = match res {
            Ok(duration) => {
                let mut case = TestCase::new(test_case_name, TestCaseStatus::success());
                case.set_time(duration);
                case.set_timestamp(Local::now());
                case.set_classname(junit.as_deref().unwrap_or_default());
                case
            }
            Err(e) => {
                writeln!(buf, "{}\n\n{:?}", style("[FAILED]").red().bold(), e)?;
                writeln!(buf)?;
                failed_case.push(file.clone());
                let mut status = TestCaseStatus::non_success(NonSuccessKind::Failure);
                status.set_type("test failure");
                let mut case = TestCase::new(test_case_name, status);
                case.set_system_err(e.to_string());
                case.set_time(Duration::from_millis(0));
                case.set_system_out("");
                case.set_timestamp(Local::now());
                case.set_classname(junit.as_deref().unwrap_or_default());
                case
            }
        };
        test_suite.add_test_case(case);
        tokio::task::block_in_place(|| stdout().write_all(&buf))?;
    }

    eprintln!(
        "\n All test cases finished in {} ms",
        start.elapsed().as_millis()
    );

    for db_name in db_names {
        let query = format!("DROP DATABASE {};", db_name);
        eprintln!("+ {}", query);
        if let Err(err) = db.run(&query).await {
            eprintln!("  ignore error: {}", err);
        }
    }

    if !failed_case.is_empty() {
        Err(anyhow!("some test case failed:\n{:#?}", failed_case))
    } else {
        Ok(())
    }
}

// Run test one be one
async fn run_serial(
    test_suite: &mut TestSuite,
    files: Vec<PathBuf>,
    engine: &EngineConfig,
    config: DBConfig,
    junit: Option<String>,
) -> Result<()> {
    let mut failed_case = vec![];

    for file in files {
        let engine = engines::connect(engine, &config).await?;
        let runner = Runner::new(engine);

        let filename = file.to_string_lossy().to_string();
        let test_case_name = filename.replace(['/', ' ', '.', '-'], "_");
        let case = match run_test_file(&mut std::io::stdout(), runner, &file).await {
            Ok(duration) => {
                let mut case = TestCase::new(test_case_name, TestCaseStatus::success());
                case.set_time(duration);
                case.set_timestamp(Local::now());
                case.set_classname(junit.as_deref().unwrap_or_default());
                case
            }
            Err(e) => {
                println!("{}\n\n{:?}", style("[FAILED]").red().bold(), e);
                println!();
                failed_case.push(filename.clone());
                let mut status = TestCaseStatus::non_success(NonSuccessKind::Failure);
                status.set_type("test failure");
                let mut case = TestCase::new(test_case_name, status);
                case.set_timestamp(Local::now());
                case.set_classname(junit.as_deref().unwrap_or_default());
                case.set_system_err(e.to_string());
                case.set_time(Duration::from_millis(0));
                case.set_system_out("");
                case
            }
        };
        test_suite.add_test_case(case);
    }

    if !failed_case.is_empty() {
        Err(anyhow!("some test case failed:\n{:#?}", failed_case))
    } else {
        Ok(())
    }
}

async fn flush(out: &mut impl std::io::Write) -> std::io::Result<()> {
    tokio::task::block_in_place(|| out.flush())
}

async fn connect_and_run_test_file(
    out: &mut impl std::io::Write,
    filename: PathBuf,
    engine: &EngineConfig,
    config: DBConfig,
) -> Result<Duration> {
    let engine = engines::connect(engine, &config).await?;
    let runner = Runner::new(engine);
    let result = run_test_file(out, runner, filename).await?;

    Ok(result)
}

async fn run_test_file<T: std::io::Write, D: AsyncDB>(
    out: &mut T,
    mut runner: Runner<D>,
    filename: impl AsRef<Path>,
) -> Result<Duration> {
    let filename = filename.as_ref();
    let records = tokio::task::block_in_place(|| {
        sqllogictest::parse_file(filename).map_err(|e| anyhow!("{:?}", e))
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
            .map_err(|e| anyhow!("{}", e.display(console::colors_enabled())))
            .context(format!(
                "failed to run `{}`",
                style(filename.to_string_lossy()).bold()
            ))?;
    }

    let duration = begin_times[0].elapsed();

    finish(
        out,
        &mut begin_times,
        &mut did_pop,
        &filename.to_string_lossy(),
    )?;

    writeln!(out)?;

    Ok(duration)
}
