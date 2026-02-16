#!/usr/bin/env python3
"""Fail fast when common docs drift patterns are detected."""

from __future__ import annotations


import fnmatch
import os
import re
import sys
import xml.etree.ElementTree as ET
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

SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$")


def parse_semver(raw_version: str) -> tuple[str, tuple[int, int, int]] | None:
    """Parse a semver-like string and normalize to major.minor.patch."""
    match = SEMVER_RE.match(raw_version.strip())
    if not match:
        return None
    major, minor, patch = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return f"{major}.{minor}.{patch}", (major, minor, patch)


def baseline_version_from_pom() -> str | None:
    pom_path = ROOT / "pom.xml"
    if not pom_path.exists():
        return None
    try:
        root = ET.parse(pom_path).getroot()
    except ET.ParseError:
        return None

    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    version = root.findtext("m:version", namespaces=namespace)
    if version is None:
        return None
    parsed = parse_semver(version)
    return parsed[0] if parsed else None


def resolve_docs_baseline_version() -> str | None:
    env_value = os.getenv("DOCS_BASELINE_VERSION")
    if env_value:
        parsed = parse_semver(env_value)
        if not parsed:
            raise SystemExit(
                "Invalid DOCS_BASELINE_VERSION: expected semantic version (e.g. 0.2.0)"
            )
        return parsed[0]
    return baseline_version_from_pom()


def stale_version_rule() -> tuple[str, re.Pattern[str], str] | None:
    baseline = resolve_docs_baseline_version()
    stale_overrides = [
        item.strip()
        for item in os.getenv("DOCS_STALE_VERSIONS", "").split(",")
        if item.strip()
    ]

    pattern: str | None = None
    if stale_overrides:
        escaped = "|".join(re.escape(v) for v in stale_overrides)
        pattern = rf"\b(?:{escaped})\b"
    elif baseline:
        parsed = parse_semver(baseline)
        if parsed:
            _, (major, minor, _) = parsed
            if minor > 0:
                # For baseline X.Y.Z, flag stale literals from X.(Y-1).*
                pattern = rf"\b{major}\.{minor - 1}\.\d+\b"
            elif major > 0:
                # For baseline X.0.Z, flag stale literals from (X-1).*.*
                pattern = rf"\b{major - 1}\.\d+\.\d+\b"

    if not pattern:
        return None

    baseline_note = baseline or "configured baseline"
    return (
        "stale-version",
        re.compile(pattern),
        f"stale version literal detected (expected docs baseline: {baseline_note})",
    )


RULES = [
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
]

stale_rule = stale_version_rule()
if stale_rule is not None:
    RULES.insert(0, stale_rule)
RULES = tuple(RULES)


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
