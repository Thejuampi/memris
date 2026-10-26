"""Release version checks must distinguish dependency examples from history."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("docs_drift", Path(__file__).with_name("check-docs-drift.py"))
docs_drift = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docs_drift)


class DependencyVersionTest(unittest.TestCase):
    def test_published_version_remains_valid_while_next_minor_is_prepared(self):
        content = "Historical 0.1.0.\nimplementation('io.github.thejuampi:memris:0.2.4')"
        self.assertEqual([], docs_drift.dependency_version_findings(content, "0.2.4"))

    def test_release_detects_outdated_gradle_and_maven_dependencies(self):
        content = """Changes since 0.2.4.
implementation('io.github.thejuampi:memris:0.2.4')
<dependency>
  <groupId>io.github.thejuampi</groupId>
  <artifactId>memris-spring-boot-starter-3</artifactId>
  <version>0.2.4</version>
</dependency>
<dependency><groupId>org.example</groupId><version>0.2.4</version></dependency>
"""
        self.assertEqual([(2, "0.2.4"), (6, "0.2.4")],
                         docs_drift.dependency_version_findings(content, "0.3.0"))


if __name__ == "__main__":
    unittest.main()
