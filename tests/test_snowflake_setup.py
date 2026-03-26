import importlib.util
import os
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path("/home/fillip/projec/cursorskills")
TEMPLATE_MODULE_PATH = REPO_ROOT / "scripts" / "snowflake_setup" / "snowflake_fivetran_template.py"
SKILL_RUNNER_PATH = (
    REPO_ROOT / ".cursor" / "skills" / "snowflake-setup" / "scripts" / "run_snowflake_setup.py"
)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


template_module = _load_module("snowflake_fivetran_template", TEMPLATE_MODULE_PATH)
skill_runner = _load_module("snowflake_setup_skill_runner", SKILL_RUNNER_PATH)


class SnowflakeSetupTests(unittest.TestCase):
    def setUp(self):
        self.original_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_env)

    def test_render_template_requires_all_placeholders(self):
        sql = "user={{SNOWFLAKE_FIVETRAN_USER}} db={{SNOWFLAKE_DRIP_DATABASE}}"
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
            handle.write(sql)
            path = Path(handle.name)
        try:
            with self.assertRaises(SystemExit) as ctx:
                template_module.render_template(path, require_password=False)
            self.assertIn("SNOWFLAKE_DRIP_DATABASE", str(ctx.exception))
            self.assertIn("SNOWFLAKE_FIVETRAN_USER", str(ctx.exception))
        finally:
            path.unlink(missing_ok=True)

    def test_render_template_uses_explicit_values_only(self):
        os.environ["SNOWFLAKE_FIVETRAN_USER"] = "svc_user"
        os.environ["SNOWFLAKE_DRIP_DATABASE"] = "raw_db"
        sql = "user={{SNOWFLAKE_FIVETRAN_USER}} db={{SNOWFLAKE_DRIP_DATABASE}}"
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
            handle.write(sql)
            path = Path(handle.name)
        try:
            rendered = template_module.render_template(path, require_password=False)
            self.assertEqual(rendered, "user=svc_user db=raw_db")
        finally:
            path.unlink(missing_ok=True)

    def test_skill_runner_detects_missing_required_env(self):
        missing = skill_runner._missing_required_env()
        self.assertIn("SNOWFLAKE_PAT", missing)
        self.assertIn("SNOWFLAKE_SQL_API_HOST or SNOWFLAKE_ACCOUNT", missing)
        self.assertIn("SNOWFLAKE_FIVETRAN_USER", missing)

    def test_skill_runner_resolves_host_from_sql_api_host(self):
        os.environ["SNOWFLAKE_SQL_API_HOST"] = "https://ACCT.snowflakecomputing.com/"
        self.assertEqual(skill_runner._resolved_host(), "ACCT.snowflakecomputing.com")


if __name__ == "__main__":
    unittest.main()
