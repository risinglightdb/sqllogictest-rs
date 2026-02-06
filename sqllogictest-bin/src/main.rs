mod engines;

use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{self, stdout, Read, Seek, SeekFrom, Stdout, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Local;
use clap::{Arg, ArgAction, CommandFactory, FromArgMatches, Parser, ValueEnum};
use console::style;
use engines::{EngineConfig, EngineType};
use fancy_regex::Regex;
use fs_err::{File, OpenOptions};
use futures::StreamExt;
use itertools::Itertools;
use quick_junit::{NonSuccessKind, Report, TestCase, TestCaseStatus, TestSuite};
use rand::distributions::DistString;
use rand::seq::SliceRandom;
use sqllogictest::substitution::well_known;
use sqllogictest::{
    default_column_validator, default_validator, trim_normalizer, update_record_with_output,
    AsyncDB, Injected, MakeConnection, Partitioner, Record, Runner, TestError, UpdateMode,
};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Default, Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
#[must_use]
pub enum Color {
    #[default]
    Auto,
    Always,
    Never,
}

// Env keys for partitioning.
const PARTITION_ID_ENV_KEY: &str = "SLT_PARTITION_ID";
const PARTITION_COUNT_ENV_KEY: &str = "SLT_PARTITION_COUNT";

#[derive(Parser, Debug, Clone)]
#[clap(about, version, author)]
struct Opt {
    /// Glob(s) of a set of test files.
    /// For example: `./test/**/*.slt`
    #[clap(required = true, num_args = 1..)]
    files: Vec<String>,

    /// The database engine name, used by the record conditions.
    #[clap(short, long, value_enum, default_value = "postgres")]
    engine: EngineType,

    /// Example: "java -cp a.jar com.risingwave.sqllogictest.App
    /// jdbc:postgresql://{host}:{port}/{db} {user}" The items in `{}` will be replaced by
    /// [`DBConfig`].
    #[clap(long, env)]
    external_engine_command_template: Option<String>,

    /// Whether to enable colorful output.
    #[clap(
        long,
        value_enum,
        default_value_t,
        value_name = "WHEN",
        env = "CARGO_TERM_COLOR"
    )]
    color: Color,

    /// Whether to enable parallel test. The `db` option will be used to create databases, and one
    /// database will be created for each test file.
    ///
    /// You can use `$__DATABASE__` in the test file to get the current database.
    #[clap(long, short)]
    jobs: Option<usize>,
    /// When using `-j`, whether to keep the temporary database when a test case fails.
    #[clap(long, default_value = "false", env = "SLT_KEEP_DB_ON_FAILURE")]
    keep_db_on_failure: bool,
    /// Show all errors
    #[clap(long, default_value = "false", env = "SLT_SHOW_ALL_ERRORS")]
    show_all_errors: bool,

    /// Whether to exit immediately when a test case fails.
    #[clap(long, default_value = "false", env = "SLT_FAIL_FAST")]
    fail_fast: bool,

    /// Report to junit XML.
    #[clap(long)]
    junit: Option<String>,

    /// The database server host.
    /// If multiple addresses are specified, one will be chosen randomly per session.
    #[clap(short, long, default_value = "localhost", env = "SLT_HOST")]
    host: Vec<String>,
    /// The database server port.
    /// If multiple addresses are specified, one will be chosen randomly per session.
    #[clap(short, long, default_value = "5432", env = "SLT_PORT")]
    port: Vec<u16>,
    /// The database name to connect.
    #[clap(short, long, default_value = "postgres", env = "SLT_DB")]
    db: String,
    /// The database username.
    #[clap(short, long, default_value = "postgres", env = "SLT_USER")]
    user: String,
    /// The database password.
    #[clap(short = 'w', long, default_value = "postgres", env = "SLT_PASSWORD")]
    pass: String,
    /// The database options.
    #[clap(long)]
    options: Option<String>,

    /// Overrides the test files with the actual output of the database.
    #[clap(long, conflicts_with_all = ["force_override", "format", "skip_failed"])]
    r#override: bool,
    /// Overrides the test files with the actual output of the database,
    /// and normalizes formatting (e.g., converts spaces to tabs and despite <slt:ignore>) even when
    /// the logical content matches.
    #[clap(long, conflicts_with_all = ["override", "format", "skip_failed"])]
    force_override: bool,
    /// Reformats the test files.
    #[clap(long, conflicts_with_all = ["override", "force_override", "skip_failed"])]
    format: bool,
    /// When a test fails, prepend a skipif directive instead of updating the expected output.
    /// Format: LABEL,REASON (e.g., "postgres,not implemented yet")
    #[clap(long, conflicts_with_all = ["override", "force_override", "format"])]
    skip_failed: Option<String>,

    /// Add a label for conditions.
    ///
    /// Records with `skipif label` will be skipped if the label is present.
    /// Records with `onlyif label` will be executed only if the label is present.
    ///
    /// The engine name is a label by default.
    #[clap(long = "label")]
    labels: Vec<String>,

    /// Partition ID for sharding the test files. When used with `partition_count`,
    /// divides the test files into shards based on the hash of the file path.
    ///
    /// Useful for running tests in parallel across multiple CI jobs. Currently
    /// automatically configured in Buildkite.
    #[clap(long, env = PARTITION_ID_ENV_KEY)]
    partition_id: Option<u64>,

    /// Total number of partitions for test sharding. More details in `partition_id`.
    #[clap(long, env = PARTITION_COUNT_ENV_KEY)]
    partition_count: Option<u64>,

    /// Timeout in seconds for shutting down the connections to the database after a
    /// test file is finished. By default, this is unspecified, meaning to wait forever.
    #[clap(long = "shutdown-timeout", env = "SLT_SHUTDOWN_TIMEOUT")]
    shutdown_timeout_secs: Option<u64>,

    /// Skip tests that matches the given regex.
    #[clap(long)]
    skip: Option<String>,
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
    /// Command line options.
    options: Option<String>,
}

impl DBConfig {
    fn random_addr(&self) -> (&str, u16) {
        self.addrs
            .choose(&mut rand::thread_rng())
            .map(|(host, port)| (host.as_ref(), *port))
            .unwrap()
    }
}

struct HashPartitioner {
    count: u64,
    id: u64,
}

impl HashPartitioner {
    fn new(count: u64, id: u64) -> Result<Self> {
        if count == 0 {
            bail!("partition count must be greater than zero");
        }
        if id >= count {
            bail!("partition id (zero-based) must be less than count");
        }
        Ok(Self { count, id })
    }
}

impl Partitioner for HashPartitioner {
    fn matches(&self, file_name: &str) -> bool {
        let mut hasher = DefaultHasher::new();
        file_name.hash(&mut hasher);
        hasher.finish() % self.count == self.id
    }
}

#[allow(clippy::needless_return)]
fn import_partition_config_from_ci() {
    if std::env::var_os(PARTITION_ID_ENV_KEY).is_some()
        || std::env::var_os(PARTITION_COUNT_ENV_KEY).is_some()
    {
        // Ignore if already set.
        return;
    }

    // Buildkite
    {
        const ID: &str = "BUILDKITE_PARALLEL_JOB";
        const COUNT: &str = "BUILDKITE_PARALLEL_JOB_COUNT";

        if let (Some(id), Some(count)) = (std::env::var_os(ID), std::env::var_os(COUNT)) {
            std::env::set_var(PARTITION_ID_ENV_KEY, id);
            std::env::set_var(PARTITION_COUNT_ENV_KEY, count);
            eprintln!("Imported partition config from Buildkite.");
            return;
        }
    }

    // TODO: more CI providers
}

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    import_partition_config_from_ci();

    let cli = Opt::command().disable_help_flag(true).arg(
        Arg::new("help")
            .long("help")
            .help("Print help information")
            .action(ArgAction::Help),
    );
    let matches = cli.get_matches();
    let Opt {
        files,
        engine,
        external_engine_command_template,
        color,
        jobs,
        keep_db_on_failure,
        show_all_errors,
        fail_fast,
        junit,
        host,
        port,
        db,
        user,
        pass,
        options,
        r#override,
        force_override,
        format,
        skip_failed,
        labels,
        partition_count,
        partition_id,
        shutdown_timeout_secs,
        skip,
    } = Opt::from_arg_matches(&matches)
        .map_err(|err| err.exit())
        .unwrap();

    if host.len() != port.len() {
        bail!(
            "{} hosts are provided while {} ports are provided",
            host.len(),
            port.len(),
        );
    }
    let addrs = host.into_iter().zip_eq(port).collect();

    let engine = match engine {
        EngineType::Mysql => EngineConfig::MySql,
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

    let partitioner = if let Some(count) = partition_count {
        let id = partition_id.context("parallel job count is specified but job id is not")?;
        Some(HashPartitioner::new(count, id)?)
    } else {
        None
    };

    let glob_patterns = files;
    let mut all_files = Vec::new();

    let re = skip
        .map(|s| Regex::new(&s))
        .transpose()
        .context("invalid regex")?;

    for glob_pattern in glob_patterns {
        let mut files: Vec<PathBuf> = glob::glob(&glob_pattern)
            .context("failed to read glob pattern")?
            .try_collect()?;

        if let Some(re) = &re {
            files.retain(|path| {
                !re.is_match(&path.to_string_lossy())
                    .context("invalid regex")
                    .unwrap()
            });
        }
    
        // Skip directories
        files.retain(|path| !path.is_dir());

        // Test against partitioner only if there are multiple files matched, e.g., expanded from an `*`.
        if files.len() > 1 {
            if let Some(partitioner) = &partitioner {
                let len = files.len();
                files.retain(|path| partitioner.matches(path.to_str().unwrap()));
                let len_after = files.len();
                eprintln!(
                    "Running {len_after} out of {len} test cases for glob pattern \"{glob_pattern}\" based on partitioning.",
                );
            }
        }

        all_files.extend(files);
    }

    let config = DBConfig {
        addrs,
        db,
        user,
        pass,
        options,
    };

    let update_mode = if format {
        Some(UpdateMode::Format)
    } else if force_override {
        Some(UpdateMode::OverrideWithFormat)
    } else if r#override {
        Some(UpdateMode::Override)
    } else if let Some(skip_failed_arg) = skip_failed {
        let parts: Vec<&str> = skip_failed_arg.splitn(2, ',').collect();
        if parts.len() != 2 {
            bail!("--skip-failed must be in format LABEL,REASON (e.g., 'postgres,not implemented yet')");
        }

        Some(UpdateMode::SkipFailed {
            label: parts[0].trim().to_string(),
            reason: parts[1].trim().to_string(),
        })
    } else {
        None
    };

    if let Some(update_mode) = update_mode {
        return update_test_files(
            all_files,
            &engine,
            config,
            &update_mode,
            jobs,
            keep_db_on_failure,
            labels,
        )
        .await;
    }

    let mut report = Report::new(junit.clone().unwrap_or_else(|| "sqllogictest".to_string()));
    report.set_timestamp(Local::now());

    let mut test_suite = TestSuite::new("sqllogictest");
    test_suite.set_timestamp(Local::now());

    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            match tokio::signal::ctrl_c().await {
                Ok(_) => {
                    eprintln!("Ctrl-C received, cancelling...");
                    cancel.cancel();
                }
                Err(err) => eprintln!("Failed to listen for Ctrl-C signal: {}", err),
            }
        }
    });

    let run_config = RunConfig {
        labels,
        junit: junit.clone(),
        fail_fast,
        show_all_errors,
        cancel,
        shutdown_timeout: shutdown_timeout_secs.map(Duration::from_secs),
    };

    let result = if let Some(jobs) = jobs {
        run_parallel(
            jobs,
            keep_db_on_failure,
            &mut test_suite,
            all_files,
            &engine,
            config,
            run_config,
        )
        .await
    } else {
        run_serial(&mut test_suite, all_files, &engine, config, run_config).await
    };

    report.add_test_suite(test_suite);

    if let Some(junit_file) = junit {
        tokio::fs::write(format!("{junit_file}-junit.xml"), report.to_string()?).await?;
    }

    result
}

struct RunConfig {
    labels: Vec<String>,
    junit: Option<String>,
    fail_fast: bool,
    show_all_errors: bool,
    cancel: CancellationToken,
    shutdown_timeout: Option<Duration>,
}

fn test_db_name(test_case_name: String) -> String {
    // Because PostgreSQL database names are < 64
    const MAX_DATABASE_NAME_LEN: usize = 63;
    const RANDOM_LEN: usize = 8;

    let mut test_case_prefix = test_case_name;

    if test_case_prefix.len() > MAX_DATABASE_NAME_LEN - RANDOM_LEN - 1 {
        test_case_prefix = test_case_prefix[..MAX_DATABASE_NAME_LEN - RANDOM_LEN - 1].to_string();
    }

    let random_id: String = rand::distributions::Alphanumeric
        .sample_string(&mut rand::thread_rng(), RANDOM_LEN)
        .to_lowercase();
    format!("{test_case_prefix}_{random_id}")
}

fn test_db_names(files: Vec<PathBuf>) -> Result<Vec<(String, PathBuf)>> {
    let mut test_databases = Vec::new();
    let mut test_cases = HashSet::new();
    for file in files {
        let filename = file
            .to_str()
            .ok_or_else(|| anyhow!("not a UTF-8 filename"))?;
        let test_case_name = filename.to_test_case_name();

        eprintln!("+ Discovered Test: {test_case_name}");
        if !test_cases.insert(test_case_name.clone()) {
            return Err(anyhow!("duplicated test case found: {}", test_case_name));
        }

        let db_name = test_db_name(test_case_name);
        test_databases.push((db_name, file));
    }
    Ok(test_databases)
}

struct TestJob {
    db_name: String,
    filename: PathBuf,
}

struct TestResultMessage {
    db_name: String,
    file: String,
    result: RunResult,
}

enum DropMessage {
    Drop(String),
    ConnectionRefused,
}

async fn run_parallel(
    jobs: usize,
    keep_db_on_failure: bool,
    test_suite: &mut TestSuite,
    files: Vec<PathBuf>,
    engine: &EngineConfig,
    config: DBConfig,
    RunConfig {
        labels,
        junit,
        fail_fast,
        show_all_errors,
        cancel,
        shutdown_timeout,
    }: RunConfig,
) -> Result<()> {
    let test_databases = test_db_names(files)?;
    let total_tests = test_databases.len();

    let (job_tx, job_rx) = mpsc::channel::<TestJob>(jobs);
    let (result_tx, mut result_rx) = mpsc::channel::<TestResultMessage>(jobs);
    let (drop_tx, drop_rx) = mpsc::channel::<DropMessage>(jobs);

    let labels = Arc::new(labels);

    let worker_handle = {
        let engine = engine.clone();
        let config = config.clone();
        let labels = Arc::clone(&labels);
        let result_tx = result_tx.clone();
        tokio::spawn(execution_task(
            jobs,
            job_rx,
            result_tx,
            engine,
            config,
            labels,
            show_all_errors,
            cancel.clone(),
            shutdown_timeout,
        ))
    };

    let dropper_handle = {
        let engine = engine.clone();
        let config = config.clone();
        tokio::spawn(drop_task(drop_rx, engine, config))
    };

    let creator_handle = {
        let engine = engine.clone();
        let config = config.clone();
        tokio::spawn(create_task(test_databases, engine, config, job_tx))
    };

    drop(result_tx);

    eprintln!("{}", style("[TEST IN PROGRESS]").blue().bold());

    let mut failed_cases = vec![];
    let mut failed_dbs: HashSet<String> = HashSet::new();
    let mut connection_refused = false;
    let mut connection_refused_notified = false;
    let mut processed = 0usize;

    let start = Instant::now();

    while processed < total_tests {
        let Some(message) = result_rx.recv().await else {
            break;
        };
        processed += 1;

        let TestResultMessage {
            db_name,
            file,
            result,
        } = message;
        let test_case_name = file.to_test_case_name();
        let case = result.to_junit(&test_case_name, junit.as_deref().unwrap_or_default());
        test_suite.add_test_case(case);

        match result {
            RunResult::Ok(_) => {}
            RunResult::Err(e) => {
                if format!("{:?}", e).contains("Connection refused") && !connection_refused {
                    connection_refused = true;
                    eprintln!("Connection refused. The server may be down.");
                }
                if fail_fast || connection_refused {
                    eprintln!("Cancelling remaining tests...");
                    cancel.cancel();
                }

                failed_cases.push(test_case_name.clone());
                failed_dbs.insert(db_name.clone());
            }
            RunResult::Skipped | RunResult::Cancelled => {}
        };

        if connection_refused && !connection_refused_notified {
            let _ = drop_tx.send(DropMessage::ConnectionRefused).await;
            connection_refused_notified = true;
        }

        if !connection_refused {
            if keep_db_on_failure && failed_dbs.contains(&db_name) {
                eprintln!(
                    "+ {}",
                    style(format!(
                        "DATABASE {db_name} contains failed cases, kept for debugging"
                    ))
                    .red()
                    .bold()
                );
            } else {
                let _ = drop_tx.send(DropMessage::Drop(db_name.clone())).await;
            }
        }
    }

    eprintln!("\n Finished in {} ms", start.elapsed().as_millis());

    drop(drop_tx);

    creator_handle.await??;
    worker_handle.await??;
    dropper_handle.await??;

    if processed < total_tests {
        return Err(anyhow!("worker pool terminated early"));
    }

    if !failed_cases.is_empty() {
        Err(anyhow!("some test cases failed:\n{:#?}", failed_cases))
    } else if cancel.is_cancelled() {
        Err(anyhow!("some test cases skipped or cancelled"))
    } else {
        Ok(())
    }
}

async fn create_task(
    tests: Vec<(String, PathBuf)>,
    engine: EngineConfig,
    config: DBConfig,
    job_tx: mpsc::Sender<TestJob>,
) -> Result<()> {
    let mut db = engines::connect(&engine, &config).await?;

    for (db_name, filename) in tests {
        let query = format!("CREATE DATABASE {db_name};");
        if let Err(err) = db.run(&query).await {
            eprintln!("({})  ignore error: {err}", query);
        }

        if job_tx.send(TestJob { db_name, filename }).await.is_err() {
            break;
        }
    }

    db.shutdown().await;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execution_task(
    concurrency: usize,
    mut job_rx: mpsc::Receiver<TestJob>,
    result_tx: mpsc::Sender<TestResultMessage>,
    engine: EngineConfig,
    config: DBConfig,
    labels: Arc<Vec<String>>,
    show_all_errors: bool,
    cancel: CancellationToken,
    shutdown_timeout: Option<Duration>,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut join_set = JoinSet::new();

    while let Some(job) = job_rx.recv().await {
        let permit = semaphore.clone().acquire_owned().await?;
        let result_tx = result_tx.clone();
        let engine = engine.clone();
        let config = config.clone();
        let labels = Arc::clone(&labels);
        let cancel = cancel.clone();

        join_set.spawn(async move {
            let TestJob { db_name, filename } = job;
            let mut job_config = config;
            job_config.db = db_name.clone();

            let result = connect_and_run_test_file(
                Vec::new(),
                filename.clone(),
                &engine,
                job_config,
                labels.as_ref(),
                show_all_errors,
                cancel,
                shutdown_timeout,
            )
            .await;

            let file = filename.to_string_lossy().to_string();
            let message = TestResultMessage {
                db_name,
                file,
                result,
            };

            let _ = result_tx.send(message).await;
            drop(permit);
        });
    }

    drop(result_tx);

    while let Some(join_result) = join_set.join_next().await {
        join_result?;
    }

    Ok(())
}

async fn drop_task(
    mut drop_rx: mpsc::Receiver<DropMessage>,
    engine: EngineConfig,
    config: DBConfig,
) -> Result<()> {
    const MAX_RETRIES: usize = 1;
    const RETRY_DELAY: Duration = Duration::from_secs(1);

    let mut db = engines::connect(&engine, &config).await?;

    while let Some(message) = drop_rx.recv().await {
        match message {
            DropMessage::ConnectionRefused => {
                eprintln!("  Connection refused. The server may be down. Exiting...");
                break;
            }
            DropMessage::Drop(db_name) => {
                match drop_database_with_retry(
                    &mut db,
                    &db_name,
                    &engine,
                    &config,
                    MAX_RETRIES,
                    RETRY_DELAY,
                )
                .await
                {
                    Ok(()) => {} // Success, maybe reconnected
                    Err(e) => {
                        eprintln!("  {e}");
                        break;
                    }
                }
            }
        }
    }

    db.shutdown().await;
    Ok(())
}

/// Attempts to drop a database with retry logic.
/// Uses `DROP DATABASE IF EXISTS` for idempotency and retries on any error.
/// Reconnects to the database before each retry. Fails fast if reconnection fails.
async fn drop_database_with_retry(
    db: &mut engines::Engines,
    db_name: &str,
    engine: &EngineConfig,
    config: &DBConfig,
    max_retries: usize,
    retry_delay: Duration,
) -> Result<()> {
    let query = format!("DROP DATABASE IF EXISTS {db_name};");
    let total_attempts = max_retries + 1;

    for attempt in 1..=total_attempts {
        match db.run(&query).await {
            Ok(_) => {
                if attempt > 1 {
                    eprintln!("  Succeed");
                }
                return Ok(());
            }
            Err(err) => {
                eprintln!("({query}) error: {err}");

                if attempt < total_attempts {
                    eprintln!(
                        "  Retrying with new connection ({}/{})...",
                        attempt, max_retries
                    );
                    tokio::time::sleep(retry_delay).await;

                    match engines::connect(engine, config).await {
                        Ok(new_db) => {
                            *db = new_db;
                            continue;
                        }
                        Err(_) => {
                            // Just try again to reconnect
                        }
                    }
                } else {
                    return Err(anyhow!(
                        "Failed to drop database {db_name} after {max_retries} retries"
                    ));
                }
            }
        }
    }

    unreachable!("Loop should always return via Ok or Err branches")
}

// Run test one be one
#[allow(clippy::too_many_arguments)]
async fn run_serial(
    test_suite: &mut TestSuite,
    files: Vec<PathBuf>,
    engine: &EngineConfig,
    config: DBConfig,
    RunConfig {
        labels,
        junit,
        fail_fast,
        show_all_errors,
        cancel,
        shutdown_timeout,
    }: RunConfig,
) -> Result<()> {
    let mut failed_cases = vec![];
    let mut connection_refused = false;

    for file in files {
        let test_case_name = file.to_string_lossy().to_test_case_name();
        let res = connect_and_run_test_file(
            stdout(),
            file,
            engine,
            config.clone(),
            &labels,
            show_all_errors,
            cancel.clone(),
            shutdown_timeout,
        )
        .await;
        stdout().flush()?;

        let case = res.to_junit(&test_case_name, junit.as_deref().unwrap_or_default());
        test_suite.add_test_case(case);

        match res {
            RunResult::Ok(_) => {}
            RunResult::Err(e) => {
                if format!("{:?}", e).contains("Connection refused") {
                    connection_refused = true;
                    eprintln!("Connection refused. The server may be down.");
                }
                if fail_fast || connection_refused {
                    eprintln!("Cancelling remaining tests...");
                    cancel.cancel();
                }

                failed_cases.push(test_case_name.clone());
            }
            RunResult::Skipped | RunResult::Cancelled => {}
        };
    }

    if !failed_cases.is_empty() {
        Err(anyhow!("some test case failed:\n{:#?}", failed_cases))
    } else if cancel.is_cancelled() {
        Err(anyhow!("some test cases skipped or cancelled"))
    } else {
        Ok(())
    }
}

async fn update_test_files(
    files: Vec<PathBuf>,
    engine: &EngineConfig,
    config: DBConfig,
    update_mode: &UpdateMode,
    jobs: Option<usize>,
    keep_db_on_failure: bool,
    labels: Vec<String>,
) -> Result<()> {
    let mut db = engines::connect(engine, &config).await?;
    let test_databases = if jobs.is_some() {
        test_db_names(files)?
    } else {
        files
            .iter()
            .map(|path| (config.db.clone(), path.clone()))
            .collect()
    };

    let db_names: Vec<String> = test_databases
        .iter()
        .map(|(db_name, _)| db_name)
        .cloned()
        .collect();
    if jobs.is_some() {
        for db_name in &db_names {
            let query = format!("CREATE DATABASE {db_name};");
            eprintln!("+ {query}");
            if let Err(err) = db.run(&query).await {
                eprintln!("  ignore error: {err}");
            }
        }
    }

    let failed_dbs: Arc<Mutex<HashSet<String>>> = Arc::default();

    let mut stream = futures::stream::iter(test_databases)
        .map(|(db_name, file)| {
            let mut config = config.clone();
            config.db = db_name.clone();

            let failed_dbs = failed_dbs.clone();
            let labels = &labels;
            async move {
                let mut runner = Runner::new(|| engines::connect(engine, &config));
                for label in labels {
                    runner.add_label(label);
                }
                runner.set_var(well_known::DATABASE.to_owned(), db_name.clone());

                let mut buffer = vec![];
                if let Err(e) = update_test_file(&mut buffer, &mut runner, &file, update_mode).await
                {
                    writeln!(buffer, "{}\n\n{:?}\n", style("[FAILED]").red().bold(), e)
                        .expect("cannot write to buffer");
                    if keep_db_on_failure {
                        failed_dbs.lock().await.insert(db_name);
                    }
                };

                runner.shutdown_async().await;
                buffer
            }
        })
        .buffer_unordered(jobs.unwrap_or(1));

    while let Some(output) = stream.next().await {
        io::stdout().write_all(&output)?;
    }

    if jobs.is_some() {
        let failed_dbs_guard = failed_dbs.lock().await;
        for db_name in db_names.iter() {
            if keep_db_on_failure && failed_dbs_guard.contains(db_name) {
                eprintln!(
                    "+ {}",
                    style(format!(
                        "DATABASE {db_name} contains failed cases, kept for debugging"
                    ))
                    .red()
                    .bold()
                );
                continue;
            }
            let query = format!("DROP DATABASE {db_name};");
            eprintln!("+ {query}");
            if let Err(err) = db.run(&query).await {
                let err = err.to_string();
                if err.contains("Connection refused") {
                    eprintln!("  Connection refused. The server may be down. Exiting...");
                    break;
                }
                eprintln!("  ignore DROP DATABASE error: {err}");
            }
        }
    }
    Ok(())
}

async fn flush(out: &mut impl io::Write) -> io::Result<()> {
    tokio::task::block_in_place(|| out.flush())
}

/// The result of running a test file.
enum RunResult {
    /// The test file ran successfully in the given duration.
    Ok(Duration),
    /// The test file failed with an error.
    Err(anyhow::Error),
    /// The test file was cancelled during execution, typically due to a Ctrl-C.
    Cancelled,
    /// The test file was skipped because it was cancelled before execution, typically
    /// due to a Ctrl-C or a failure with `--fail-fast`.
    Skipped,
}

impl From<Result<Duration>> for RunResult {
    fn from(res: Result<Duration>) -> Self {
        match res {
            Ok(duration) => RunResult::Ok(duration),
            Err(e) => RunResult::Err(e),
        }
    }
}

impl RunResult {
    /// Convert the result to a JUnit test case.
    fn to_junit(&self, test_case_name: &str, junit: &str) -> TestCase {
        match self {
            RunResult::Ok(duration) => {
                let mut case = TestCase::new(test_case_name, TestCaseStatus::success());
                case.set_time(*duration);
                case.set_timestamp(Local::now());
                case.set_classname(junit);
                case
            }
            RunResult::Err(e) => {
                let mut status = TestCaseStatus::non_success(NonSuccessKind::Failure);
                status.set_type("test failure");

                let mut case = TestCase::new(test_case_name, status);
                case.set_system_err(e.to_string());
                case.set_time(Duration::from_millis(0));
                case.set_system_out("");
                case.set_timestamp(Local::now());
                case.set_classname(junit);
                case
            }
            RunResult::Skipped | RunResult::Cancelled => {
                // TODO: what status should we use for cancelled tests?
                let mut case = TestCase::new(test_case_name, TestCaseStatus::skipped());
                case.set_time(Duration::from_millis(0));
                case.set_timestamp(Local::now());
                case.set_classname(junit);
                case
            }
        }
    }
}

trait Output: Write {
    fn finish(&mut self) -> io::Result<()>;
}

/// In serial mode, we directly write to stdout.
impl Output for Stdout {
    fn finish(&mut self) -> io::Result<()> {
        self.flush()
    }
}

/// In parallel mode, we write to a buffer and flush it to stdout at the end
/// to avoid interleaving output from different parallelism.
impl Output for Vec<u8> {
    fn finish(&mut self) -> io::Result<()> {
        let mut stdout = stdout().lock();
        stdout.write_all(self)?;
        stdout.flush()
    }
}

#[allow(clippy::too_many_arguments)]
async fn connect_and_run_test_file(
    out: impl Output,
    filename: PathBuf,
    engine: &EngineConfig,
    config: DBConfig,
    labels: &[String],
    show_all_errors: bool,
    cancel: CancellationToken,
    shutdown_timeout: Option<Duration>,
) -> RunResult {
    struct OutputGuard<O: Output>(O);
    impl<O: Output> Drop for OutputGuard<O> {
        fn drop(&mut self) {
            self.0.finish().unwrap();
        }
    }
    let mut out = OutputGuard(out);

    static RUNNING_TESTS: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

    // If the run is already cancelled, skip it.
    if cancel.is_cancelled() {
        // Ensure that all running tests are cancelled before we return `Skipped`.
        let _ = RUNNING_TESTS.write().await;

        writeln!(
            out.0,
            "{: <60} .. {}",
            filename.to_string_lossy(),
            style("[SKIPPED]").dim().bold(),
        )
        .unwrap();
        return RunResult::Skipped;
    }

    // Hold until the current test is finished or cancelled.
    let _running = RUNNING_TESTS.read().await;

    let mut runner = Runner::new(|| engines::connect(engine, &config));
    for label in labels {
        runner.add_label(label);
    }
    runner.set_var(well_known::DATABASE.to_owned(), config.db.clone());

    let begin = Instant::now();

    // Note: we don't use `CancellationToken::run_until_cancelled` here because it always
    // poll the wrapped future first, while we want cancellation to be more responsive.
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            writeln!(
                out.0,
                "{} after {} ms",
                style("[CANCELLED]").yellow().bold(),
                begin.elapsed().as_millis(),
            )
            .unwrap();
            RunResult::Cancelled
        }
        result = run_test_file(&mut out.0, &mut runner, filename.clone(), show_all_errors) => {
            if let Err(err) = &result {
                writeln!(
                    out.0,
                    "{} after {} ms\n\n{:?}\n",
                    style("[FAILED]").red().bold(),
                    begin.elapsed().as_millis(),
                    err,
                ).unwrap();
            }
            result.into()
        }
    };

    drop(out); // flush the output before shutting down the runner

    match shutdown_timeout {
        None => runner.shutdown_async().await,
        Some(timeout) => {
            if tokio::time::timeout(timeout, runner.shutdown_async())
                .await
                .is_err()
            {
                eprintln!(
                    "shutting down connection to database for test {} timed out",
                    filename.display()
                )
            }
        }
    }

    result
}

/// Different from [`Runner::run_file_async`], we re-implement it here to print some progress
/// information.
async fn run_test_file<T: io::Write, M: MakeConnection>(
    out: &mut T,
    runner: &mut Runner<M::Conn, M>,
    filename: impl AsRef<Path>,
    show_all_errors: bool,
) -> Result<Duration> {
    let filename = filename.as_ref();

    write!(out, "{: <60} .. ", filename.to_string_lossy())?;
    flush(out).await?;

    let records = tokio::task::block_in_place(|| sqllogictest::parse_file(filename))
        .context("failed to parse sqllogictest file")?;

    let mut begin_times = vec![];
    let mut did_pop = false;

    begin_times.push(Instant::now());

    let mut errors = vec![];
    let mut locations = vec![];
    for record in records {
        if let Record::Halt { .. } = record {
            break;
        }
        match &record {
            Record::Injected(Injected::BeginInclude(file)) => {
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
            Record::Injected(Injected::EndInclude(file)) => {
                finish_test_file(out, &mut begin_times, &mut did_pop, file)?;
            }
            _ => {}
        }

        let res = runner.run_async(record).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                if show_all_errors {
                    errors.push(e.kind());
                    locations.push(e.location());
                } else {
                    return Err(e)
                        .map_err(|e| anyhow!("{}", e.display(console::colors_enabled())))
                        .context(format!(
                            "failed to run `{}`",
                            style(filename.to_string_lossy()).bold()
                        ));
                }
            }
        }
    }
    if !errors.is_empty() {
        let e = Err(TestError::new_composite(errors, locations));
        e.map_err(|e| anyhow!("{}", e.display(console::colors_enabled())))
            .context(format!(
                "failed to run `{}`",
                style(filename.to_string_lossy()).bold()
            ))?;
    }

    let duration = begin_times[0].elapsed();

    finish_test_file(
        out,
        &mut begin_times,
        &mut did_pop,
        &filename.to_string_lossy(),
    )?;

    writeln!(out)?;

    Ok(duration)
}

fn finish_test_file<T: io::Write>(
    out: &mut T,
    time_stack: &mut Vec<Instant>,
    did_pop: &mut bool,
    file: &str,
) -> Result<()> {
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
}

/// Different from [`sqllogictest::update_test_file`], we re-implement it here to print some
/// progress information.
async fn update_test_file<T: io::Write, M: MakeConnection>(
    out: &mut T,
    runner: &mut Runner<M::Conn, M>,
    filename: impl AsRef<Path>,
    update_mode: &UpdateMode,
) -> Result<()> {
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

    fn create_outfile(filename: impl AsRef<Path>) -> io::Result<(PathBuf, File)> {
        let filename = filename.as_ref();
        let outfilename = filename.file_name().unwrap().to_str().unwrap().to_owned() + ".temp";
        let outfilename = filename.parent().unwrap().join(outfilename);
        // create a temp file in read-write mode
        let outfile = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open(&outfilename)?;
        Ok((outfilename, outfile))
    }

    fn override_with_outfile(
        filename: &String,
        outfilename: &PathBuf,
        outfile: &mut File,
    ) -> io::Result<()> {
        // check whether outfile ends with multiple newlines, which happens if
        // - the last record is statement/query
        // - the original file ends with multiple newlines

        const N: usize = 8;
        let mut buf = [0u8; N];
        loop {
            outfile.seek(SeekFrom::End(-(N as i64))).unwrap();
            outfile.read_exact(&mut buf).unwrap();
            let num_newlines = buf.iter().rev().take_while(|&&b| b == b'\n').count();
            assert!(num_newlines > 0);

            if num_newlines > 1 {
                // if so, remove the last ones
                outfile
                    .set_len(outfile.metadata().unwrap().len() - num_newlines as u64 + 1)
                    .unwrap();
            }

            if num_newlines == 1 || num_newlines < N {
                break;
            }
        }

        let metadata = fs_err::symlink_metadata(filename)?;
        if metadata.is_symlink() {
            fs_err::copy(outfilename, filename)?;
            fs_err::remove_file(outfilename)?;
        } else {
            fs_err::rename(outfilename, filename)?;
        }

        Ok(())
    }

    struct Item {
        filename: String,
        outfilename: PathBuf,
        outfile: File,
        halt: bool,
    }
    let (outfilename, outfile) = create_outfile(filename)?;
    let mut stack = vec![Item {
        filename: filename.to_string_lossy().to_string(),
        outfilename,
        outfile,
        halt: false,
    }];

    for record in records {
        let Item {
            filename,
            outfilename,
            outfile,
            halt,
        } = stack.last_mut().unwrap();

        match &record {
            Record::Injected(Injected::BeginInclude(filename)) => {
                let (outfilename, outfile) = create_outfile(filename)?;
                stack.push(Item {
                    filename: filename.clone(),
                    outfilename,
                    outfile,
                    halt: false,
                });

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
                    filename
                )?;
                flush(out).await?;
            }
            Record::Injected(Injected::EndInclude(file)) => {
                override_with_outfile(filename, outfilename, outfile)?;
                stack.pop();
                finish_test_file(out, &mut begin_times, &mut did_pop, file)?;
            }
            _ => {
                if *halt {
                    writeln!(outfile, "{record}")?;
                    continue;
                }
                if matches!(record, Record::Halt { .. }) {
                    *halt = true;
                    writeln!(outfile, "{record}")?;
                    continue;
                }
                update_record(outfile, runner, record, update_mode)
                    .await
                    .context(format!("failed to run `{}`", style(filename).bold()))?;
            }
        }
    }

    finish_test_file(
        out,
        &mut begin_times,
        &mut did_pop,
        &filename.to_string_lossy(),
    )?;

    writeln!(out)?;

    let Item {
        filename,
        outfilename,
        outfile,
        halt: _,
    } = stack.last_mut().unwrap();
    override_with_outfile(filename, outfilename, outfile)?;

    Ok(())
}

async fn update_record<M: MakeConnection>(
    outfile: &mut File,
    runner: &mut Runner<M::Conn, M>,
    record: Record<<M::Conn as AsyncDB>::ColumnType>,
    update_mode: &UpdateMode,
) -> Result<()> {
    assert!(!matches!(record, Record::Injected(_)));

    if *update_mode == UpdateMode::Format {
        writeln!(outfile, "{record}")?;
        return Ok(());
    }

    let record_output = runner.apply_record(record.clone()).await;
    match update_record_with_output(
        &record,
        &record_output,
        "\t",
        default_validator,
        trim_normalizer,
        default_column_validator,
        update_mode,
    ) {
        Some(new_record) => {
            // Check if skipif was added (new conditions > old conditions)
            let old_conditions_len = match &record {
                Record::Statement { conditions, .. } => conditions.len(),
                Record::Query { conditions, .. } => conditions.len(),
                _ => 0,
            };
            let new_conditions = match &new_record {
                Record::Statement { conditions, .. } => conditions,
                Record::Query { conditions, .. } => conditions,
                _ => &vec![],
            };

            // Write any new conditions that were added
            if new_conditions.len() > old_conditions_len {
                for condition in &new_conditions[..new_conditions.len() - old_conditions_len] {
                    writeln!(
                        outfile,
                        "{}",
                        Record::<<M::Conn as AsyncDB>::ColumnType>::Condition(condition.clone())
                    )?;
                }
            }

            writeln!(outfile, "{new_record}")?;
        }
        None => {
            writeln!(outfile, "{record}")?;
        }
    }

    Ok(())
}

#[easy_ext::ext]
impl<T: AsRef<str>> T {
    /// Normalize the path to the test case name.
    pub fn to_test_case_name(&self) -> String {
        self.as_ref().replace([' ', '.', '-', '/'], "_")
    }
}
