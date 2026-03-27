import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path("/home/fillip/projec/cursorskills/scripts/run_source_analysis_with_description_checklist.py")
SPEC = importlib.util.spec_from_file_location("run_source_analysis_with_description_checklist", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class RunSourceAnalysisWithDescriptionChecklistTests(unittest.TestCase):
    def test_resolved_output_path_matches_analyzer_suffix_pattern(self):
        result = {
            "metadata": {"schema_filter": "dbo"},
            "connection": {"driver": "mssql"},
        }

        output = MODULE._resolved_output_path("schema.json", result)

        self.assertEqual(str(output), "schema_dbo_mssql.json")


if __name__ == "__main__":
    unittest.main()
