mod error;
mod extended;
mod simple;

use std::marker::PhantomData;

use tokio::task::JoinHandle;

type Result<T> = std::result::Result<T, error::PgDriverError>;

/// Marker type for the Postgres simple query protocol.
pub struct Simple;
/// Marker type for the Postgres extended query protocol.
pub struct Extended;

/// Generic Postgres engine based on the client from [`tokio_postgres`]. The protocol `P` can be
/// either [`Simple`] or [`Extended`].
pub struct Postgres<P> {
    /// `None` means the connection is closed.
    conn: Option<(tokio_postgres::Client, JoinHandle<()>)>,
    _protocol: PhantomData<P>,
}

/// Postgres engine using the simple query protocol.
pub type PostgresSimple = Postgres<Simple>;
/// Postgres engine using the extended query protocol.
pub type PostgresExtended = Postgres<Extended>;

/// Connection configuration. This is a re-export of [`tokio_postgres::Config`].
pub type PostgresConfig = tokio_postgres::Config;

impl<P> Postgres<P> {
    /// Connects to the Postgres server with the given `config`.
    pub async fn connect(config: PostgresConfig) -> Result<Self> {
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;

        let connection = tokio::spawn(async move {
            if let Err(e) = connection.await {
                if e.is_closed() {
                    log::info!("Postgres connection closed");
                } else {
                    log::error!("Postgres connection error: {:?}", e);
                }
            }
        });

        Ok(Self {
            conn: Some((client, connection)),
            _protocol: PhantomData,
        })
    }

    /// Returns a reference of the inner Postgres client.
    pub fn client(&self) -> &tokio_postgres::Client {
        &self.conn.as_ref().expect("connection is shutdown").0
    }

    /// Shutdown the Postgres connection.
    async fn shutdown(&mut self) {
        if let Some((client, connection)) = self.conn.take() {
            if let Err(e) = client
                .cancel_token()
                .cancel_query(tokio_postgres::NoTls)
                .await
            {
                log::warn!("Failed to cancel query during shutdown: {:?}", e);
            }

            drop(client);
            connection.await.ok();
        }
    }
}
