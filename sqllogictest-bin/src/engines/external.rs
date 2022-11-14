use std::io;
use std::marker::PhantomData;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqllogictest::AsyncDB;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_util::codec::{Decoder, FramedRead};

pub struct ExternalDriver {
    child: Child,
    stdin: ChildStdin,
    stdout: FramedRead<ChildStdout, JsonDecoder<Output>>,
}

#[derive(Serialize)]
struct Input {
    sql: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Output {
    Success { result: String },
    Failed { err: String },
}

#[derive(Debug, Error)]
pub enum ExternalDriverError {
    #[error("ser/de failed")]
    Json(#[from] serde_json::Error),
    #[error("io failed")]
    Io(#[from] io::Error),
    #[error("sql failed {0}")]
    Sql(String),
}

type Result<T> = std::result::Result<T, ExternalDriverError>;

impl ExternalDriver {
    pub async fn connect(cmd: &mut Command) -> Result<Self> {
        let cmd = cmd.stdin(Stdio::piped()).stdout(Stdio::piped());

        let mut child = cmd.spawn()?;

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stdout = FramedRead::new(stdout, JsonDecoder::default());

        Ok(Self {
            child,
            stdin,
            stdout,
        })
    }
}

impl Drop for ExternalDriver {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[async_trait]
impl AsyncDB for ExternalDriver {
    type Error = ExternalDriverError;

    async fn run(&mut self, sql: &str) -> Result<String> {
        let input = Input {
            sql: sql.to_string(),
        };
        let input = serde_json::to_string(&input)?;
        self.stdin.write_all(input.as_bytes()).await?;
        let output = match self.stdout.next().await {
            Some(Ok(output)) => output,
            Some(Err(e)) => return Err(e.into()),
            None => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
        };
        match output {
            Output::Success { result } => Ok(result),
            Output::Failed { err } => Err(ExternalDriverError::Sql(err)),
        }
    }

    fn engine_name(&self) -> &str {
        "external"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}

struct JsonDecoder<T>(PhantomData<T>);

impl<T> Default for JsonDecoder<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> Decoder for JsonDecoder<T>
where
    T: for<'de> serde::de::Deserialize<'de>,
{
    type Item = T;
    type Error = ExternalDriverError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let mut inner = serde_json::Deserializer::from_slice(src.as_ref()).into_iter::<T>();
        match inner.next() {
            None => Ok(None),
            Some(Err(e)) if e.is_eof() => Ok(None),
            Some(Err(e)) => Err(e.into()),
            Some(Ok(v)) => {
                let len = inner.byte_offset();
                src.advance(len);
                Ok(Some(v))
            }
        }
    }
}
