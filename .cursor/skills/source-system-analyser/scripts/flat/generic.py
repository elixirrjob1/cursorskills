"""Generic flat-file analyzer scaffold.

This module is intentionally lightweight and preserves backward compatibility by
not changing the existing database entrypoint.
"""

from typing import Any, Dict

from .base import BaseFlatAnalyzer


class GenericFlatAnalyzer(BaseFlatAnalyzer):
    """Provide a placeholder generic flat-file analyzer extension point."""

    def analyze(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "metadata": {
                "source_type": "flat",
                "analyzer": "generic",
                "note": "Generic flat analyzer scaffold is available for extension.",
            },
            "connection": {
                "provider": source_config.get("provider", "file"),
            },
            "source_system_context": {
                "contacts": [],
                "delete_management_instruction": "",
                "restrictions": "",
            },
            "data_quality_summary": {
                "critical": 0,
                "warning": 0,
                "info": 1,
            },
            "tables": [],
        }
