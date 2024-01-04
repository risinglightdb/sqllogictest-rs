# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.19.1] - 2024-01-04

* parser: `include` now returns error if no file is matched. 

## [0.19.0] - 2023-11-11

* parser: refactor `expect` field in sqllogictest parser to make it easier to work with.

## [0.18.0] - 2023-11-08

* Support matching multiline error message under `----` for both `statement error` and `query error`.
  ```
  query error
  SELECT 1/0;
  ----
  db error: ERROR: Failed to execute query

  Caused by these errors:
    1: Failed to evaluate expression: 1/0
    2: Division by zero
  ```

  The output error message must be the exact match of the expected one to pass the test, except for the leading and trailing whitespaces. Users may use `--override` to let the runner update the test files with the actual output.

  Empty lines are allowed in the expected error message. As a result, the message must end with two consecutive empty lines.

  Breaking changes in the parser:
  - Add new variants to `ParseErrorKind`. Mark it as `#[non_exhaustive]`.
  - Change the type of `expected_error` from `Regex` to `ExpectedError`, which is either a inline `Regex` or multiline `String`.

## [0.17.2] - 2023-11-01

* fix(runner): fix parallel testing db name duplication. Now we use full file path instead of filename as the temporary db name in `run_parallel_async`.

## [0.17.1] - 2023-09-20

* bin: support envvars `SLT_HOST/PORT/DB/USER/PASSWORD`

## [0.17.0] - 2023-09-19

* Support environment variables substituion for SQL and system commands.
  For compatibility, this feature is by default disabled, and can be enabled by adding `control substitution on` to the test file.
  ```
  control substitution on

  query TTTT
  SELECT
      '$foo'                -- short
    , '${foo}'              -- long
    , '${bar:default}'      -- default value
    , '${bar:$foo-default}' -- recursive default value
  FROM baz;
  ----
  ...
  ```

  Besides, there's a special variable `$__TEST_DIR__` which is the path to a temporary directory specific to the current test case.
  This can be helpful if you need to manipulate some external resources during the test.
  ```
  control substitution on

  statement ok
  COPY (SELECT * FROM foo) TO '$__TEST_DIR__/foo.txt';

  system ok
  echo "foo" > "$__TEST_DIR__/foo.txt"
  ```

  Changes:
  - (parser) **Breaking change**: Add `Control::Substitution`. Mark `Control` as `#[non_exhaustive]`.
  - (runner) **Breaking change**: Remove `enable_testdir`. For migration, one should now enable general substitution by the `control` statement and use a dollar-prefixed `$__TEST_DIR__`.

## [0.16.0] - 2023-09-15

* Support running external system commands with the syntax below. This is useful for manipulating some external resources during the test.
  ```
  system ok
  echo "Hello, world!"
  ```
  The runner will check the exit code of the command, and the output will be ignored. Currently, only `ok` is supported.

  Changes:
  - (parser) **Breaking change**: Add `Record::System`, and corresponding `TestErrorKind` and `RecordOutput`. Mark `TestErrorKind` and `RecordOutput` as `#[non_exhaustive]`.
  - (runner) Add `run_command` to `AsyncDB` trait. The default implementation will run the command with `std::process::Command::status`. Implementors can override this method to utilize an asynchronous runtime such as `tokio`.

* fix(runner): fix database name duplication for parallel tests by using the **full path** of the test file (instead of the file name) as the database name.

## [0.15.3] - 2023-08-02

* fix(bin): fix error context display. To avoid stack backtrace being printed, unset `RUST_BACKTRACE` environment variable, or use pre-built binaries built with stable toolchain instead.

## [0.15.2] - 2023-07-31

* fix(bin): do not print stack backtrace on error

## [0.15.1] - 2023-07-24

* fix `statement error` unexpectedly passed when result is a successful `query`. Similarly for expected `query error` but successful `statement ok`.

## [0.15.0] - 2023-07-06

* Allow multiple connections to the database in a single test case, which is useful for testing the transaction behavior. This can be achieved by attaching a `connection foo` record before the query or statement.
  - (parser) Add `Record::Connection`.
  - (runner) **Breaking change**: Since the runner may establish multiple connections at runtime, `Runner::new` now takes a `impl MakeConnection`, which is usually a closure that returns a try-future of the `AsyncDB` instance.
  - (bin) The connection to the database is now established lazily on the first query or statement.

## [0.14.0] - 2023-06-08

* We enhanced how `skipif` and `onlyif` works. Previously it checks against `DB::engine_name()`, and `sqllogictest-bin` didn't implement it.
  - (parser) A minor **breaking change**: Change the field names of `Condition:: OnlyIf/SkipIf`.
  - (runner) Add `Runner::add_label`. Now multiple labels are supported ( `DB::engine_name()` is still included). The condition evaluates to true if *any* of the provided labels match the `skipif/onlyif <lable>`.
  - (bin) Add `--label` option to specify custom labels.

## [0.13.2] - 2023-03-24

* `Runner::update_test_file` properly escapes regex special characters.

## [0.13.1] - 2023-03-16

* Support postgres options.

## [0.13.0] - 2023-02-15

* `sqllogictest-bin` now uses the strict validator to update records (the runner still doesn't check schema).
* The query syntax now allows optional columns (`query\n` without any column arguments).

## [0.12.0] - 2023-02-09

* customizable column types and validators

## [0.11.2] - 2023-02-09

* support multiple files as input in cli
* remove unnecessary debug

## [0.11.1] - 2023-01-15

* fix parsing for trailing comments

## [0.11.0] - 2023-01-14

This release contains some minor fixes.

* fix: use `Vec<Vec<String>>` for external engine (JDBC)
* fix: Use `lines()` instead of `split('\n')` in `parse_inner`. So the behavior can be correct on Windows.
* fix: parse DML with `returning` as a query
* A minor **breaking change**: `update_test_file` takes `&mut Runner` instead of its ownnership.

## [0.10.0] - 2022-12-15

- Improve the ability to unparse and update the test files. Mainly add `update_record_with_output` and `update_test_file` to the library. 

  More details:
  * Add `impl Display` for `Record` (refactor `unparse`).
  * Add `Record::Whitespace` so the whitespace in the original files can be reconstructed during `unparse`.
  * Add tests for unparsing and updating records.
  * Refactor and fix the behavior about newlines and `halt` for CLI options `--override` and `--format`.
- Fix: `hash-threshold` should be compared with the number of values instead of the number of rows.
- **Breaking change**: The type of `Validator` is changed from `fn(&Vec<String>, &Vec<String>) -> bool` to `fn(&[Vec<String>], &[String]) -> bool`. Also added a `default_validator`.

Thanks to the contributions of @alamb and @xudong963 .

## [0.9.0] - 2022-12-07

- Improve the format and color handling for errors.
- Support `hash-threshold`.
- Fix `statement count <n>` for postgres engines.
- **Breaking change**: use `Vec<Vec<String>>` instead of `String` as the query results by `DB`. This allows the runner to verify the results more precisely.
  + For `rowsort`, runner will only sort actual results now, which means the result in the test cases should be sorted.
- **Breaking change**: `Hook` is removed.
- **Breaking change**: `Record` and parser's behavior are tweaked:
  + retain `Include` record when linking its content
  + keep parsing after `Halt`
  + move `Begin/EndInclude` to `Injected`
- Added CLI options `--override` and `--format`, which can override the test files with the actual output of the database, or reformat the test files.

## [0.8.0] - 2022-11-22

- Support checking error message using `statement error <regex>` and `query error <regex>` syntax.
  - **Breaking change**: `Record::Statement`,  `Record::Query` and `TestErrorKind` are changed accordingly.

## [0.7.1] - 2022-11-15

- Fix: `--external-engine-command-template` should not be required

## [0.7.0] - 2022-11-14

- Add support for external driver.
- Support more type in postgres-extended.
- Record file stack in location.

## [0.6.4] - 2022-08-25

- Use one session for each file in serial mode.

## [0.6.3] - 2022-08-24

- Support registering hook function after each query.

## [0.6.2] - 2022-08-22

- Support load balancing of multiple addr.
- Integrate with libtest-mimic. Add the macro `sqllogictest::harness!`.
- Improve error handling for parser.

## [0.6.1] - 2022-08-16

- Add parallel running to `Runner`.
- Drop database after parallel run.

## [0.6.0] - 2022-08-06

- Support postgres extended mode
- Separate sqllogictest runner to sqllogictest-bin

## [0.5.5] - 2022-07-26

- Add timestamp to junit. (#57)
- Add `sleep` function to `AsyncDB`. (#61)
- Fix panic without junit. (#58)

## [0.5.4] - 2022-07-02

- Remove unsupported characters from junit test name.

## [0.5.3] - 2022-06-26

### Added

- Add junit support. Use `--junit <filename>` to generate junit xml.

## [0.5.2] - 2022-06-16

### Fixed

- Fix expanded `include` wildcard file name. (#52)

## [0.5.1] - 2022-06-16

### Added

- Support wildcard in `include` statement. (#49)

### Changed

- Show diff instead of actual / expected data on failed. (#51)

## [0.5.0] - 2022-06-09

### Added

- Print empty strings as "(empty)"

## [0.4.0] - 2022-06-07

### Added

- Support parallel sqllogictest

## [0.3.4] - 2022-04-21

### Added

- Panic if no test file is found

## [0.3.3] - 2022-03-30

### Added

- New test UI for sqllogictest binary
- Support set console color on the test UI with `--color` parameter

## [0.3.0] - 2022-03-21

### Added

- Async interface `AsyncDB` for SQL runners.
- support evaluating `skipif` and `onlyif` conditions
- support file-level sort mode control syntax
- supports custom validator

## [0.2.0] - 2022-01-12

### Added

- A command-line tool to run scripts from file against postgres-compatible databases.
- Support `sleep` and `include` statement.

### Changed

- Add file location to the error message.
- Runner returns error type instead of panic.

## [0.1.0] - 2021-12-10

### Added

- Basic sqllogictest parser and runner.
