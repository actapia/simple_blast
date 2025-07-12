import io
import tempfile

from simple_blast.blasting import (
    TabularBlastnSearch,
    default_out_columns,
)
from simple_blast.blastdb_cache import BlastDBCache
from simple_blast.multiformat import MultiformatBlastnSearch
from simple_blast.sam import SAMBlastnSearch
from .simple_blast_test import (
    SimpleBlastTestCase,
)

import Bio.Align

class TestMultiformatBlastnSearch(SimpleBlastTestCase):
    def test_construction(self):
        subject_str = "subject.fasta"
        query_str = "query.fasta"
        res = MultiformatBlastnSearch(query_str, subject_str)
        self.assertEqual(res.out_format, 11)

    def test_basic_search(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        search = MultiformatBlastnSearch(
            self.data_dir / "queries.fasta",
            self.data_dir / "seqs_0.fasta",
        )
        # This just tests that we get a valid Blast4-Archive as output for now.
        pyblast4_archive.Blast4Archive.from_file(
            io.BytesIO(search.get_output()),
            "asn_text"
        )

    def test_to(self):
        search = MultiformatBlastnSearch(
            self.data_dir / "queries.fasta",
            self.data_dir / "seqs_0.fasta",
        )
        res = search.to(6)
        TabularBlastnSearch.parse_hits(io.BytesIO(res), default_out_columns)
        res = search.to(17)
        Bio.Align.parse(io.TextIOWrapper(io.BytesIO(res)), "sam")
        
    def test_to_search(self):
        subject = self.data_dir / "seqs_0.fasta",
        query = self.data_dir / "queries.fasta"
        search = MultiformatBlastnSearch(query, subject)
        res = search.to_search(6)
        self.assertIsInstance(res, TabularBlastnSearch)
        search2 = TabularBlastnSearch(query, subject)
        self.assertDataFramesEqual(res.hits, search2.hits)
        res = search.to_search(17)
        self.assertIsInstance(res, SAMBlastnSearch)
        search2 = SAMBlastnSearch(query, subject)
        self.assertSAMsEqual(res.hits, search2.hits)

    def test_to_sam(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        subject = self.data_dir / "seqs_0.fasta",
        query = self.data_dir / "queries.fasta"
        search = MultiformatBlastnSearch(query, subject)
        res = search.to_sam()
        for al in res.hits:
            self.assertTrue(al.query.id.startswith("seq"))
            self.assertTrue(al.target.id.startswith("from_seq"))
        res = search.to_sam(False)
        for al in res.hits:
            self.assertTrue(al.target.id.startswith("Query_"))
            self.assertTrue(al.query.id.startswith("Subject_"))

    def test_empty_to_sam(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        search = MultiformatBlastnSearch(
            self.data_dir / "queries.fasta",
            self.data_dir / "no_matches.fasta",
        )
        self.assertEqual(list(search.to_sam().hits), [])

    def test_sam_SR(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        for subject in self.data_dir.glob("seqs_*.fasta"):
            multi_search = MultiformatBlastnSearch(
                self.data_dir / "queries.fasta",
                subject,
            )
            for al in multi_search.to_sam(subject_as_reference=True).hits:
                self.assertEqual(
                    al.query.id.removeprefix("from_"),
                    al.target.id
                )

    def test_sam_from_db(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = BlastDBCache(temp_dir)
            for subject in self.data_dir.glob("seqs_*.fasta"):
                cache.makedb(subject)
                multi_search = MultiformatBlastnSearch(
                    self.data_dir / "queries.fasta",
                    subject,
                    db_cache=cache
                )
                for al in multi_search.to_sam().hits:
                    self.assertEqual(
                        al.target.id.removeprefix("from_"),
                        al.query.id
                    )
            
