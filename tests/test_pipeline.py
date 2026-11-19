import unittest
from src.pipeline import transform, run

class PipelineTests(unittest.TestCase):
    def test_valid_row(self):
        import csv
        from pathlib import Path
        path = Path(__file__).resolve().parents[1] / 'data' / 'input.csv'
        with path.open(newline='') as f: row = next(csv.DictReader(f))
        self.assertIn('value', transform(row))
        self.assertIn('flag', transform(row))

    def test_pipeline(self):
        self.assertTrue(run())

if __name__ == '__main__': unittest.main()
