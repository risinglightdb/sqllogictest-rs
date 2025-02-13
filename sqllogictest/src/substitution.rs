use subst::Env;

use crate::RunnerContext;

/// Substitute environment variables and special variables like `__TEST_DIR__` in SQL.
#[derive(Default, Clone)]
pub(crate) struct Substitution {
    pub(crate) runner_ctx: &'a RunnerContext,
}

#[derive(thiserror::Error, Debug)]
#[error("substitution failed: {0}")]
pub(crate) struct SubstError(subst::Error);

impl Substitution {
    pub fn substitute(&self, input: &str, subst_env_vars: bool) -> Result<String, SubstError> {
        if !subst_env_vars {
            Ok(input
                .replace("$__TEST_DIR__", &self.test_dir())
                .replace("$__NOW__", &self.now())
                .replace("$__DATABASE__", self.runner_ctx.db_name()))
        } else {
            subst::substitute(input, self).map_err(SubstError)
        }
    }

    fn test_dir(&self) -> String {
        self.runner_ctx
            .test_dir()
            .path()
            .to_string_lossy()
            .into_owned()
    }

    fn now(&self) -> String {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("failed to get current time")
            .as_nanos()
            .to_string()
    }
}

impl<'a> subst::VariableMap<'a> for Substitution<'a> {
    type Value = String;

    fn get(&'a self, key: &str) -> Option<Self::Value> {
        match key {
            "__TEST_DIR__" => self.test_dir().into(),
            "__NOW__" => self.now().into(),
            "__DATABASE__" => self.runner_ctx.db_name().to_owned().into(),
            key => Env.get(key),
        }
    }
}
