use std::path::Path;

pub use glob::glob;
pub use libtest_mimic::{run, Arguments, Failed, Trial};

use crate::{MakeConnection, Runner, RunnerContext};

/// * `db_name`: The name of the database.
/// * `db_fn`: `fn() -> sqllogictest::AsyncDB`
/// * `pattern`: The glob used to match against and select each file to be tested. It is relative to
///   the root of the crate.
#[macro_export]
macro_rules! harness {
    ($db_name:literal, $db_fn:path, $pattern:expr) => {
        fn main() {
            let paths = $crate::harness::glob($pattern).expect("failed to find test files");
            let mut tests = vec![];

            for entry in paths {
                let path = entry.expect("failed to read glob entry");
                tests.push($crate::harness::Trial::test(
                    path.to_str().unwrap().to_string(),
                    move || $crate::harness::test(db_name, &path, || async { Ok($db_fn()) }),
                ));
            }

            if tests.is_empty() {
                panic!("no test found for sqllogictest under: {}", $pattern);
            }

            $crate::harness::run(&$crate::harness::Arguments::from_args(), tests).exit();
        }
    };
}

pub fn test(
    filename: impl AsRef<Path>,
    db_name: impl ToOwned<Owned = String>,
    make_conn: impl MakeConnection,
) -> Result<(), Failed> {
    let ctx = RunnerContext::new(db_name.to_owned());
    let mut tester = Runner::new(ctx, make_conn);
    tester.run_file(filename)?;
    tester.shutdown();
    Ok(())
}
