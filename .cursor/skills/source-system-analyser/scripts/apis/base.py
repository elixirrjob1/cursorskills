"""Base interfaces for API analyzers."""

from typing import Any, Dict


class BaseAPIAnalyzer:
    """Define a minimal API analyzer contract for modular extensions."""

    def analyze(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
