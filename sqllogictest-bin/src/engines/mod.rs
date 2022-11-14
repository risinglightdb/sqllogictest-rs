mod postgres;
use clap::ArgEnum;
use postgres::Postgres;
use tokio::process::Command;
mod postgres_extended;
use std::fmt::Display;
mod external;

use async_trait::async_trait;
use postgres_extended::PostgresExtended;
use sqllogictest::AsyncDB;

use self::external::ExternalDriver;
use super::{DBConfig, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ArgEnum)]
pub enum EngineType {
    Postgres,
    PostgresExtended,
    External,
}

#[derive(Clone, Debug)]
pub enum EngineConfig {
    Postgres,
    PostgresExtended,
    External(String),
}

enum Engines {
    Postgres(Postgres),
    PostgresExtended(PostgresExtended),
    External(ExternalDriver),
}

pub(super) async fn connect(engine: &EngineConfig, config: &DBConfig) -> Result<impl AsyncDB> {
    Ok(match engine {
        EngineConfig::Postgres => Engines::Postgres(Postgres::connect(config).await?),
        EngineConfig::PostgresExtended => {
            Engines::PostgresExtended(PostgresExtended::connect(config).await?)
        }
        EngineConfig::External(cmd_tmpl) => {
            let (host, port) = config.random_addr();
            let cmd_str = cmd_tmpl
                .replace("{db}", &config.db)
                .replace("{host}", host)
                .replace("{port}", &port.to_string())
                .replace("{user}", &config.user)
                .replace("{pass}", &config.pass);
            let mut cmd = Command::new("bash");
            let cmd = cmd.args([
                "-c",
                &cmd_str,
            ]);
            Engines::External(ExternalDriver::connect(cmd).await?)
        }
    })
}

#[derive(Debug)]
struct AnyhowError(anyhow::Error);

impl Display for AnyhowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for AnyhowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl Engines {
    async fn run(&mut self, sql: &str) -> Result<String, anyhow::Error> {
        Ok(match self {
            Engines::Postgres(e) => e.run(sql).await?,
            Engines::PostgresExtended(e) => e.run(sql).await?,
            Engines::External(e) => e.run(sql).await?,
        })
    }
}

#[async_trait]
impl AsyncDB for Engines {
    type Error = AnyhowError;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        self.run(sql).await.map_err(AnyhowError)
    }
}
