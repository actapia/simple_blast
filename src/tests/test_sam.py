from simple_blast.multiformat import MultiformatBlastnSearch
from simple_blast.sam import SAMBlastnSearch
from .simple_blast_test import (
    SimpleBlastTestCase,
)

class TestSAMBlastnSearch(SimpleBlastTestCase):
    def test_basic_search(self):
        for subject in self.data_dir.glob("seqs_*.fasta"):
            search = SAMBlastnSearch(self.data_dir / "queries.fasta", subject)
            self.assertGreater(len(list(iter(search.hits))), 0)
        search = SAMBlastnSearch(
            self.data_dir / "no_matches.fasta",
            self.data_dir / "queries.fasta"
        )
        self.assertEqual(len(list(iter(search.hits))), 0)

    def test_search_pyblast4(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        for subject in self.data_dir.glob("seqs_*.fasta"):
            multi_search = MultiformatBlastnSearch(
                self.data_dir / "queries.fasta",
                subject,
            )
            for al in multi_search.to_sam().hits:
                self.assertEqual(
                    al.target.id.removeprefix("from_"),
                    al.query.id
                )

