use std::sync::{Arc, OnceLock};

use subst::Env;
use tempfile::{tempdir, TempDir};

/// Substitute environment variables and special variables like `__TEST_DIR__` in SQL.
#[derive(Default, Clone)]
pub(crate) struct Substitution {
    /// The temporary directory for `__TEST_DIR__`.
    /// Lazily initialized and cleaned up when dropped.
    test_dir: Arc<OnceLock<TempDir>>,
}

impl<'a> subst::VariableMap<'a> for Substitution {
    type Value = String;

    fn get(&'a self, key: &str) -> Option<Self::Value> {
        match key {
            "__TEST_DIR__" => {
                let test_dir = self
                    .test_dir
                    .get_or_init(|| tempdir().expect("failed to create testdir"));
                test_dir.path().to_string_lossy().into_owned().into()
            }

            "__NOW__" => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("failed to get current time")
                .as_nanos()
                .to_string()
                .into(),

            key => Env.get(key),
        }
    }
}
