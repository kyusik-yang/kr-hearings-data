"""Open API (open.assembly.go.kr) key lookup for the few keyed downloads in the pipeline.

The key is never stored in the repository. It is read at runtime from, in order:
  1. the environment variable ASSEMBLY_API_KEY, or
  2. the file named by the environment variable ASSEMBLY_API_KEY_FILE, which holds either the bare
     key or a line  ASSEMBLY_API_KEY = '...'.
Never print, log or write the key. Request logs strip the KEY parameter.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

_LINE_RE = re.compile(r"ASSEMBLY_API_KEY\s*=\s*['\"]([^'\"]+)['\"]")


def get_assembly_api_key() -> str:
    key = os.environ.get("ASSEMBLY_API_KEY", "").strip()
    if key:
        return key
    path = os.environ.get("ASSEMBLY_API_KEY_FILE", "").strip()
    if path:
        text = Path(path).expanduser().read_text(encoding="utf-8")
        m = _LINE_RE.search(text)
        key = (m.group(1) if m else text).strip()
        if key and "\n" not in key:
            return key
    raise RuntimeError("Open API key not found: set ASSEMBLY_API_KEY or ASSEMBLY_API_KEY_FILE")
