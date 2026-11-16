"""Keep tag pushes in forks from attempting canonical crate publication."""
from pathlib import Path
import unittest
import yaml


class PublishingTest(unittest.TestCase):
    def test_canonical_repository_admission(self):
        root = Path(__file__).resolve().parents[1]
        workflow = yaml.safe_load((root / '.github/workflows/publish-crate.yml').read_text())
        job = workflow['jobs']['publish']
        self.assertEqual(job['if'], "github.repository == 'datagutt/moblink-rust'")
        self.assertTrue(any(step.get('run') == 'cargo publish' for step in job['steps']))


if __name__ == '__main__':
    unittest.main()
