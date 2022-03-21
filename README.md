# Sqllogictest-rs

[![Crate](https://img.shields.io/crates/v/sqllogictest.svg)](https://crates.io/crates/sqllogictest)
[![Docs](https://docs.rs/sqllogictest/badge.svg)](https://docs.rs/sqllogictest)
[![CI](https://github.com/singularity-data/sqllogictest-rs/workflows/CI/badge.svg?branch=main)](https://github.com/singularity-data/sqllogictest-rs/actions)

[Sqllogictest][Sqllogictest] is a testing framework to verify the correctness of an SQL database.

This crate implements a sqllogictest parser and runner in Rust.

[Sqllogictest]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

## Using as Library

Add the following lines to your `Cargo.toml` file:

```toml
[dependencies]
sqllogictest = "0.3"
```

Implement `DB` trait for your database structure:

```rust
struct Database {...}

impl sqllogictest::DB for Database {
    type Error = ...;
    fn run(&self, sql: &str) -> Result<String, Self::Error> {
        ...
    }
}
```

It should take an SQL query string as input, and output the query result as a string.
The runner verifies the results by comparing the string after normalization.

Finally, create a `Runner` on your database instance, and then run the script:

```rust
let mut tester = sqllogictest::Runner::new(Database::new());
let script = std::fs::read_to_string("script.slt").unwrap();
tester.run_script(&script);
```

You can also parse the script and execute the records separately:

```rust
let records = sqllogictest::parse(&script).unwrap();
for record in records {
    tester.run(record);
}
```

See [examples](./examples) directory for more usages.

## Using as CLI

This crate can also be used as a command-line tool.

To install the binary, the `bin` feature is required:

```sh
cargo install sqllogictest --features bin
```

You can use it as follows:

```sh
sqllogictest './test/**/*.slt'
```

This command will run scripts in `test` directory against postgres with default connection settings.

You can find more options in `sqllogictest --help`.

Note that only postgres is supported now.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

Contributors should add a Signed-off-by line for [Developer Certificate of Origin](https://github.com/probot/dco#how-it-works)
in their commits. Use `git commit -s` to sign off commits.
