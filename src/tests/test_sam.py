import random
from simple_blast.multiformat import MultiformatBlastnSearch
from simple_blast.sam import SAMBlastnSearch, merge_sam_bytes
from .simple_blast_test import (
    SimpleBlastTestCase,
)
from Bio import SeqIO
from Bio.Seq import Seq

large_size = 5001

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

    def test_large_search(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        random.seed(485)
        subjects = [
            SeqIO.SeqRecord(
                Seq("".join(random.choices("ATCG", k=large_size*2))),
                id=f"subject_{i}", name="", description=""
            )
            for i in range(2)
        ]
        queries = [
            SeqIO.SeqRecord(
                subjects[i].seq[a:a+large_size],
                id=f"query_{i}",
                name="",
                description=""
            )
            for (i, a) in enumerate(
                    [
                        random.randint(0, large_size - 1) for _ in range(2)
                    ]
            )
        ]
        with MultiformatBlastnSearch.from_sequences(
                queries,
                subjects
        ) as multi_search:
            b4s = pyblast4_archive.Blast4Archive.from_bytes(
                multi_search.output,
                "asn_text"
            )
            self.assertGreater(
                len(b4s),
                1
            )
            sam_search =  multi_search.to_sam()
            for al in sam_search.hits:
                self.assertEqual(
                    al.target.id.removeprefix("query_"),
                    al.query.id.removeprefix("subject_")
                )

    def test_empty_merge(self):
        try:
            import pyblast4_archive
        except ImportError:
            self.skipTest("pyblast4_archive not installed.")
        self.assertEqual(merge_sam_bytes(), b'')


    def test_search_SR(self):
        for subject in self.data_dir.glob("seqs_*.fasta"):
            search = SAMBlastnSearch(
                self.data_dir / "queries.fasta",
                subject,
            )
            SR_search = SAMBlastnSearch(
                self.data_dir / "queries.fasta",
                subject,
                subject_as_reference=True
            )
            targets = set()
            queries = set()
            for al in search.hits:
                targets.add(al.target.id)
                queries.add(al.query.id)
            SR_targets = set()
            SR_queries = set()
            for al in SR_search.hits:
                SR_targets.add(al.target.id)
                SR_queries.add(al.query.id)
            self.assertEqual(targets, SR_queries)
            self.assertEqual(queries, SR_targets)


