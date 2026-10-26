#!/usr/bin/env python3
"""Version recording and artifact checks shared by local and CI releases."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ("memris", "memris-spring-data-boot2", "memris-spring-data-boot3",
             "memris-spring-boot-starter-2", "memris-spring-boot-starter-3")
CENTRAL = "https://repo.maven.apache.org/maven2/io/github/thejuampi"
POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def properties() -> dict[str, str]:
    return dict(re.findall(r"^(\w+)=(.*)$", (ROOT / "gradle.properties").read_text(), re.MULTILINE))


def versions() -> tuple[str, str]:
    current = properties()["version"]
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)-SNAPSHOT", current)
    if not match:
        raise SystemExit("Expected major.minor.patch-SNAPSHOT in gradle.properties")
    major, minor, patch = map(int, match.groups())
    return f"{major}.{minor}.{patch}", f"{major}.{minor}.{patch + 1}-SNAPSHOT"


def check_available(version: str) -> None:
    for artifact in ARTIFACTS:
        url = f"{CENTRAL}/{artifact}/{version}/{artifact}-{version}.pom"
        try:
            with urllib.request.urlopen(url, timeout=30):
                raise SystemExit(f"Already published: {artifact}:{version}; do not republish")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
    if not (ROOT / "docs" / "releases" / f"{version}.md").is_file():
        raise SystemExit(f"Missing release notes for {version}")
    print(f"All five {version} coordinates are available")


def artifact_path(directory: Path, artifact: str, version: str, extension: str, classifier: str = "") -> Path:
    file_version = version
    if version.endswith("-SNAPSHOT"):
        metadata = ET.parse(directory / "maven-metadata.xml").getroot()
        file_version = next((entry.findtext("value")
                             for entry in metadata.findall("versioning/snapshotVersions/snapshotVersion")
                             if entry.findtext("extension") == extension
                             and entry.findtext("classifier", "") == classifier), None)
        if not file_version:
            raise SystemExit(f"Missing {extension}/{classifier} snapshot metadata for {artifact}")
    suffix = f"-{classifier}" if classifier else ""
    return directory / f"{artifact}-{file_version}{suffix}.{extension}"


def verify_artifacts(repository: Path, version: str) -> None:
    for artifact in ARTIFACTS:
        directory = repository / "io/github/thejuampi" / artifact / version
        for classifier in ("", "sources", "javadoc"):
            path = artifact_path(directory, artifact, version, "jar", classifier)
            with zipfile.ZipFile(path) as archive:
                if archive.testzip() is not None:
                    raise SystemExit(f"Invalid archive: {path}")
        pom = ET.parse(artifact_path(directory, artifact, version, "pom")).getroot()
        if pom.findtext("m:version", namespaces=POM_NS) != version:
            raise SystemExit(f"Wrong POM version for {artifact}")
        dependencies = pom.findall("m:dependencies/m:dependency", POM_NS)
        for dependency in dependencies:
            if dependency.findtext("m:groupId", namespaces=POM_NS) == "io.github.thejuampi":
                if dependency.findtext("m:version", namespaces=POM_NS) != version:
                    raise SystemExit(f"Mixed Memris dependency versions in {artifact}")
        if artifact.startswith("memris-spring-data-"):
            expected = "javax.persistence" if artifact.endswith("2") else "jakarta.persistence"
            opposite = "jakarta.persistence" if artifact.endswith("2") else "javax.persistence"
            groups = {d.findtext("m:groupId", namespaces=POM_NS) for d in dependencies}
            if expected not in groups or opposite in groups:
                raise SystemExit(f"Wrong persistence API dependencies for {artifact}")
        print(f"Verified {artifact}:{version} JARs and POM")


def record_version(version: str, published: bool) -> None:
    path = ROOT / "gradle.properties"
    previous = properties()
    content = re.sub(r"^version=.*$", "version=" + version, path.read_text(), flags=re.MULTILINE)
    if published:
        content = re.sub(r"^releasedVersion=.*$", "releasedVersion=" + version, content, flags=re.MULTILINE)
    path.write_text(content, encoding="utf-8")
    targets = [ROOT / "README.md", *(ROOT / "docs").rglob("*.md")]
    for target in targets:
        if "releases" in target.relative_to(ROOT).parts:
            continue
        content = target.read_text(encoding="utf-8")
        updated = content.replace(f"| `{previous['version']}` | Current repository version",
                                  f"| `{version}` | Current repository version")
        updated = updated.replace(f"| Repository version | `{previous['version']}`",
                                  f"| Repository version | `{version}`")
        if published:
            updated = update_published_references(updated, previous["releasedVersion"], version)
        if updated != content:
            target.write_text(updated, encoding="utf-8")


def update_published_references(content: str, previous: str, version: str) -> str:
    content = re.sub(r"(io\.github\.thejuampi:[\w-]+:)[\w.+-]+", lambda m: m[1] + version, content)

    def update_dependency(match: re.Match[str]) -> str:
        block = match.group()
        if re.search(r"<groupId>\s*io\.github\.thejuampi\s*</groupId>", block):
            return re.sub(r"(<version>)\s*[^<]+\s*(</version>)", lambda m: m[1] + version + m[2], block)
        return block

    content = re.sub(r"<dependency>.*?</dependency>", update_dependency, content, flags=re.DOTALL)
    content = content.replace(f"| `{previous}` | Latest published", f"| `{version}` | Latest published")
    return content.replace(f"| Published artifacts | `{previous}`", f"| Published artifacts | `{version}`")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("versions", "check-available", "verify-artifacts",
                                             "record-release", "record-development"))
    parser.add_argument("--version")
    parser.add_argument("--repository", type=Path, default=ROOT / "build/repository")
    args = parser.parse_args()
    if args.command == "versions":
        release, development = versions()
        output = f"release_version={release}\ndevelopment_version={development}\n"
        print(output, end="")
        if os.getenv("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
                stream.write(output)
    else:
        if not args.version or not re.fullmatch(r"\d+\.\d+\.\d+(?:-SNAPSHOT)?", args.version):
            parser.error("--version must be a semantic release or snapshot version")
        if args.command == "check-available":
            check_available(args.version)
        elif args.command == "verify-artifacts":
            verify_artifacts(args.repository, args.version)
        else:
            record_version(args.version, args.command == "record-release")


if __name__ == "__main__":
    main()
