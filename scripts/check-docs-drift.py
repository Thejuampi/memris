#!/usr/bin/env python3
"""Fail fast when common docs drift patterns are detected."""

from __future__ import annotations


import fnmatch
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple


ROOT = Path(__file__).resolve().parents[1]

# Empty by default; set DOCS_DRIFT_ALLOWLIST to comma-separated globs when needed.
ALLOWLIST_GLOBS: List[str] = []
ALLOWLIST_GLOBS.extend(
    glob.strip()
    for glob in os.getenv("DOCS_DRIFT_ALLOWLIST", "").split(",")
    if glob.strip()
)

RULES = (
    (
        "stale-version",
        re.compile(r"\b0\.1\.10\b"),
        "stale version literal detected (expected docs baseline: 0.2.0)",
    ),
    (
        "invalid-memrisrepository-arity",
        re.compile(r"MemrisRepository<[^>\n]+,\s*[^>\n]+>"),
        "MemrisRepository must use one generic type parameter: MemrisRepository<T>",
    ),
    (
        "invalid-index-annotation-syntax",
        re.compile(r"@Index\(\s*IndexType\."),
        "invalid @Index syntax; use @Index(type = Index.IndexType.X)",
    ),
)


def iter_targets() -> List[Path]:
    targets = [ROOT / "README.md"]
    targets.extend(sorted((ROOT / "docs").rglob("*.md")))
    return targets


def is_allowlisted(path: Path) -> bool:
    relative = path.relative_to(ROOT).as_posix()
    return any(fnmatch.fnmatch(relative, glob) for glob in ALLOWLIST_GLOBS)


def main() -> int:
    findings: List[Tuple[str, int, str, str]] = []
    targets = iter_targets()

    for path in targets:
        if is_allowlisted(path):
            continue
        relative = path.relative_to(ROOT).as_posix()
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for rule_id, pattern, message in RULES:
                if pattern.search(line):
                    findings.append((relative, line_number, rule_id, message))

    if findings:
        print("Docs drift check failed:")
        for relative, line_number, rule_id, message in findings:
            print(f"{relative}:{line_number}: [{rule_id}] {message}")
        return 1

    checked = sum(1 for path in targets if not is_allowlisted(path))
    print(f"Docs drift check passed ({checked} files checked).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
