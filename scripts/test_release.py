"""Release records must preserve history and coordinate dependency versions."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("release", Path(__file__).with_name("release.py"))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseTest(unittest.TestCase):
    def test_snapshot_checks_follow_the_latest_metadata_instead_of_old_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "maven-metadata.xml").write_text("""<metadata><versioning><snapshotVersions>
<snapshotVersion><extension>jar</extension><value>0.3.0-20261025.123456-2</value></snapshotVersion>
<snapshotVersion><extension>jar</extension><classifier>sources</classifier>
<value>0.3.0-20261025.123456-2</value></snapshotVersion>
</snapshotVersions></versioning></metadata>""")
            self.assertEqual(root / "memris-0.3.0-20261025.123456-2.jar",
                             release.artifact_path(root, "memris", "0.3.0-SNAPSHOT", "jar"))
            self.assertEqual(root / "memris-0.3.0-20261025.123456-2-sources.jar",
                             release.artifact_path(root, "memris", "0.3.0-SNAPSHOT", "jar", "sources"))
            with self.assertRaisesRegex(SystemExit, "Missing pom"):
                release.artifact_path(root, "memris", "0.3.0-SNAPSHOT", "pom")

    def test_release_and_next_snapshot_records_keep_notes_and_history_intact(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(release, "ROOT", Path(directory)):
            root = Path(directory)
            (root / "docs/releases").mkdir(parents=True)
            (root / "gradle.properties").write_text("version=0.3.0-SNAPSHOT\nreleasedVersion=0.2.4\n")
            readme = root / "README.md"
            readme.write_text("| `0.2.4` | Latest published |\n| `0.3.0-SNAPSHOT` | Current repository version |\n"
                              "implementation('io.github.thejuampi:memris:0.2.4')\nHistory: 0.2.4\n")
            notes = root / "docs/releases/0.3.0.md"
            notes.write_text("Changes since 0.2.4\n")
            self.assertEqual(("0.3.0", "0.3.1-SNAPSHOT"), release.versions())
            release.record_version("0.3.0", True)
            self.assertIn("| `0.3.0` | Latest published", readme.read_text())
            self.assertIn("memris:0.3.0", readme.read_text())
            release.record_version("0.3.1-SNAPSHOT", False)
            self.assertEqual({"version": "0.3.1-SNAPSHOT", "releasedVersion": "0.3.0"}, release.properties())
            self.assertIn("History: 0.2.4", readme.read_text())
            self.assertIn("| `0.3.1-SNAPSHOT` | Current repository version", readme.read_text())
            self.assertIn("memris:0.3.0'", readme.read_text())
            self.assertNotIn("memris:0.3.1-SNAPSHOT", readme.read_text())
            self.assertEqual("Changes since 0.2.4\n", notes.read_text())

    def test_pom_updates_only_memris_dependencies(self):
        content = """<dependency><groupId>io.github.thejuampi</groupId><version>0.2.4</version></dependency>
<dependency><groupId>org.example</groupId><version>0.2.4</version></dependency>"""
        updated = release.update_published_references(content, "0.2.4", "0.3.0")
        self.assertIn("<groupId>io.github.thejuampi</groupId><version>0.3.0</version>", updated)
        self.assertIn("<groupId>org.example</groupId><version>0.2.4</version>", updated)


if __name__ == "__main__":
    unittest.main()
