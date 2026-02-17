#!/usr/bin/env python3
"""
Run the API with env loaded from Azure Key Vault (or .env).
Usage: python scripts/run_api.py [uvicorn args...]

Example:
  python scripts/run_api.py --reload --host 0.0.0.0 --port 8000
"""
import subprocess
import sys
from pathlib import Path

_scripts = Path(__file__).resolve().parent
_root = _scripts.parent
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from keyvault_loader import load_env

load_env()
args = [sys.executable, "-m", "uvicorn", "api.main:app", *sys.argv[1:]]
sys.exit(subprocess.run(args, cwd=str(_root)).returncode)
