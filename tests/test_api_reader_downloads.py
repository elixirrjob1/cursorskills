import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path("/home/filip/Projects/skills/.cursor/skills/source-system-analyser/scripts/apis/api_reader.py")
SPEC = importlib.util.spec_from_file_location("api_reader", MODULE_PATH)
api_reader = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(api_reader)


class FakeResponse:
    def __init__(self, *, status_code=200, content_type="application/json", content=b"{}", payload=None):
        self.status_code = status_code
        self.headers = {"Content-Type": content_type, "Content-Length": str(len(content))}
        self._content = content
        self._payload = payload if payload is not None else {}

    @property
    def content(self):
        return self._content

    def json(self):
        return self._payload

    def iter_content(self, chunk_size=65536):
        yield self._content


class FakeSession:
    def __init__(self, response):
        self.response = response

    def get(self, url, timeout=30, stream=False):
        return self.response


class ApiReaderDownloadTests(unittest.TestCase):
    def test_filename_from_url_uses_last_path_segment(self):
        self.assertEqual(api_reader._filename_from_url("https://example.com/api/analyze?schema=dbo"), "analyze")

    def test_json_download_adds_json_suffix_and_pretty_prints(self):
        response = FakeResponse(
            content_type="application/json; charset=utf-8",
            content=b'{"ok":true}',
            payload={"ok": True},
        )
        session = FakeSession(response)
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = str(Path(tmpdir) / "analyze")
            result = api_reader._fetch_url(session, "https://example.com/api/analyze?schema=dbo", 30, destination=destination)
            written = Path(result["downloaded_to"])
            self.assertEqual(written.name, "analyze.json")
            self.assertTrue(written.exists())
            parsed = json.loads(written.read_text(encoding="utf-8"))
            self.assertEqual(parsed, {"ok": True})
            self.assertIn("\n", written.read_text(encoding="utf-8"))

    def test_non_json_download_keeps_original_destination(self):
        response = FakeResponse(content_type="text/plain", content=b"hello", payload=None)
        session = FakeSession(response)
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = str(Path(tmpdir) / "notes")
            result = api_reader._fetch_url(session, "https://example.com/files/notes", 30, destination=destination)
            written = Path(result["downloaded_to"])
            self.assertEqual(written.name, "notes")
            self.assertEqual(written.read_bytes(), b"hello")


if __name__ == "__main__":
    unittest.main()
