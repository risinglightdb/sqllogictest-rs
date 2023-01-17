use rand::seq::SliceRandom;

pub mod postgres;

/// Connection configuration.
#[derive(Clone)]
pub struct DBConfig {
    /// The database server host and port. Will randomly choose one if multiple are given.
    pub addrs: Vec<(String, u16)>,
    /// The database name to connect.
    pub db: String,
    /// The database username.
    pub user: String,
    /// The database password.
    pub pass: String,
}

impl DBConfig {
    pub fn random_addr(&self) -> (&str, u16) {
        self.addrs
            .choose(&mut rand::thread_rng())
            .map(|(host, port)| (host.as_ref(), *port))
            .unwrap()
    }
}
