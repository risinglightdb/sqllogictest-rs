use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    sqllogictest_bin::main_okk().await
}
