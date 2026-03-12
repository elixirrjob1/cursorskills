import importlib.util
import sys
import types
import unittest
from pathlib import Path


sqlalchemy = types.ModuleType("sqlalchemy")
sqlalchemy.MetaData = object
sqlalchemy.Table = object
sqlalchemy.func = types.SimpleNamespace(count=lambda *args, **kwargs: None)
sqlalchemy.inspect = lambda *args, **kwargs: None
sqlalchemy.select = lambda *args, **kwargs: None
sqlalchemy.text = lambda value: value
sqlalchemy.create_engine = lambda *args, **kwargs: None
sqlalchemy_engine = types.ModuleType("sqlalchemy.engine")
sqlalchemy_engine.Engine = object
sqlalchemy_exc = types.ModuleType("sqlalchemy.exc")
sqlalchemy_exc.SAWarning = Warning
databases = types.ModuleType("databases")
databases.get_adapter = lambda *args, **kwargs: None
databases.get_adapter_for_engine = lambda *args, **kwargs: None
sys.modules.setdefault("sqlalchemy", sqlalchemy)
sys.modules.setdefault("sqlalchemy.engine", sqlalchemy_engine)
sys.modules.setdefault("sqlalchemy.exc", sqlalchemy_exc)
sys.modules.setdefault("databases", databases)

from api import db


MODULE_PATH = Path("/home/fillip/projec/cursorskills/.cursor/skills/source-system-analyser/scripts/source_system_analyzer.py")
SPEC = importlib.util.spec_from_file_location("source_system_analyzer", MODULE_PATH)
analyzer = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(analyzer)


class SemanticTaxonomyTests(unittest.TestCase):
    def test_api_db_infers_customer_and_name_semantics(self):
        self.assertEqual(db._infer_semantic_class("customer_id"), "customer_identifier")
        self.assertEqual(db._infer_semantic_class("client_identifier"), "customer_identifier")
        self.assertEqual(db._infer_semantic_class("first_name"), "given_name")
        self.assertEqual(db._infer_semantic_class("surname"), "family_name")

    def test_analyzer_prefers_specific_semantic_patterns_over_generic_class_mapping(self):
        self.assertEqual(analyzer.classify_field("customer_id"), "identifier")
        self.assertEqual(analyzer._infer_semantic_class("customer_id", "identifier"), "customer_identifier")
        self.assertEqual(analyzer.classify_field("first_name"), "person_name")
        self.assertEqual(analyzer._infer_semantic_class("first_name", "person_name"), "given_name")
        self.assertEqual(analyzer._infer_semantic_class("last_name", "person_name"), "family_name")


if __name__ == "__main__":
    unittest.main()
