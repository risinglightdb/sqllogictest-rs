use std::sync::OnceLock;

use subst::Env;
use tempfile::{tempdir, TempDir};

#[derive(Default)]
pub(crate) struct Substitution {
    test_dir: OnceLock<TempDir>,
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

            key => Env.get(key),
        }
    }
}
