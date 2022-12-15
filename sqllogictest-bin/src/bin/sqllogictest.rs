//! We use `bin/sqllogictest.rs` instead of `main.rs` so that the installed binary
//! is named `sqllogictest` instead of `sqllogictest-bin`.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    sqllogictest_bin::main_okk().await
}
