#!/usr/bin/env python3
"""
Run a command with env loaded from Azure Key Vault.
Usage: python run_with_keyvault.py <script> [args...]

Use @VAR to substitute env vars (e.g. @DATABASE_URL) after loading from Key Vault.

Example:
  python scripts/run_with_keyvault.py .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py @DATABASE_URL schema.json public
"""
import os
import subprocess
import sys
from pathlib import Path

_scripts = Path(__file__).resolve().parent
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))

from keyvault_loader import load_env

load_env()

# Substitute @VAR with os.environ.get("VAR")
args = []
for a in sys.argv[1:]:
    if a.startswith("@") and len(a) > 1:
        args.append(os.environ.get(a[1:], ""))
    else:
        args.append(a)

sys.exit(subprocess.run(args).returncode)
