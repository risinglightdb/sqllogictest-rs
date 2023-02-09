# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), 
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
