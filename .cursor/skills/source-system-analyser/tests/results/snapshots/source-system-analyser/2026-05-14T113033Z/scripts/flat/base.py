"""Base interfaces for flat-file analyzers."""

from typing import Any, Dict


class BaseFlatAnalyzer:
    """Define a minimal flat analyzer contract for modular extensions."""

    def analyze(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
