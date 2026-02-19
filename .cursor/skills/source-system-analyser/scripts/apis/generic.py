"""Generic API analyzer scaffold.

This module is intentionally lightweight and preserves backward compatibility by
not changing the existing database entrypoint.
"""

from typing import Any, Dict

from .base import BaseAPIAnalyzer


class GenericAPIAnalyzer(BaseAPIAnalyzer):
    """Provide a placeholder generic API analyzer extension point."""

    def analyze(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "metadata": {
                "source_type": "api",
                "analyzer": "generic",
                "note": "Generic API analyzer scaffold is available for extension.",
            },
            "connection": {
                "provider": source_config.get("provider", "generic"),
            },
            "data_quality_summary": {
                "critical": 0,
                "warning": 0,
                "info": 1,
            },
            "tables": [],
        }
