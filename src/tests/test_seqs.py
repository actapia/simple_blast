import tempfile
import pandas as pd
import Bio
import simple_blast.seqs
from unittest.mock import patch, ANY
from simple_blast.blasting import (
    BlastnSearch,
    blastn_from_sequences,
    TabularBlastnSearch
)
from simple_blast.seqs import (
    SeqsAsFile,
    _write_fasta_fallback,
    _write_fasta
)
from Bio.SeqRecord import SeqRecord, Seq
from Bio import SeqIO

from .simple_blast_test import SimpleBlastTestCase

class TestSeqs(SimpleBlastTestCase):
    def setUp(self):
        super().setUp()
        self.data = [
            ('GGTTGTCTTTGGAGACGCCGGAACTCTCCACGCAGCTAGGGCCATGCGCATTAATGGTAAGGCGG'
             'ATGATCACGCCTCCATTGTGAAGCACGAGAAACT'),
            ('TGGCGTTCTCTTAGAGATCCGGATGCCAATCGCCGAGACATCGGGAAATAGGGAACGTACATCGG'
             'GAAGGACTTGACAAG')
        ]
        self.seqio_data = [
            SeqRecord(Seq(d), id=f"SeqIO_seq_{i}", description="", name="")
            for (i, d) in enumerate(self.data)
        ]

    def assertSeqIOSequencesEqual(self, a, b):
        for x, y in zip(a, b):
            self.assertEqual(x.seq, y.seq)
            self.assertEqual(x.id, y.id)

    def verify_test_data(self, path, debug=False):
        with open(path, "r") as tf:
            lines = list(map(str.rstrip,tf))
        self.assertEqual(lines[0], ">seq_0")
        self.assertEqual("".join(lines[1:3]), self.data[0])
        self.assertEqual(lines[3], ">seq_1")
        self.assertEqual(lines[4], self.data[1])
        
    def test_write_fasta_fallback(self):
        with tempfile.NamedTemporaryFile(mode="wt") as temp_file:
            _write_fasta_fallback(self.data, temp_file)
            self.verify_test_data(temp_file.name)

    def test_write_fasta(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            # Test write with SeqIO.
            _write_fasta(self.seqio_data, temp_file.name)
            self.assertSeqIOSequencesEqual(
                SeqIO.parse(temp_file.name, "fasta"), self.seqio_data
            )
            # Test write with str sequences.
            _write_fasta(self.data, temp_file.name)
            self.verify_test_data(temp_file.name)
            # Test without SeqIO.
            with (
                    patch.object(simple_blast.seqs, "Bio", None) as _,
                    patch.object(Bio.SeqIO, "write") as seqio_write,
            ):
                with (
                        patch.object(
                            simple_blast.seqs,
                            "_write_fasta_fallback"
                        ) as fallback_write
                ):
                    _write_fasta(self.seqio_data, temp_file.name)
                    seqio_write.assert_not_called()
                    fallback_write.assert_called_with(self.seqio_data, ANY)
                _write_fasta(self.data, temp_file.name)
                self.verify_test_data(temp_file.name)
                

    def test_seqs_as_file(self):
        # Test using create and destroy.
        saf = SeqsAsFile(self.data)
        saf.create()
        self.verify_test_data(saf.name)
        saf.destroy()
        saf = SeqsAsFile(self.seqio_data)
        saf.create()
        self.assertSeqIOSequencesEqual(
            SeqIO.parse(saf.name, "fasta"), self.seqio_data
        )
        saf.destroy()
        # Test using context manager.
        with SeqsAsFile(self.data) as saf:
            #from IPython import embed
            #embed()
            self.verify_test_data(saf.name, debug=True)
        with SeqsAsFile(self.seqio_data) as saf:
            self.assertSeqIOSequencesEqual(
                SeqIO.parse(saf.name, "fasta"), self.seqio_data
            )

    def test_blast_with_seqs(self):
        seqio_subjects = [
            list(SeqIO.parse(self.data_dir / f"seqs_{i}.fasta", "fasta"))
            for i in range(3)
        ]
        seqio_query = list(
            SeqIO.parse(self.data_dir / "queries.fasta", "fasta")
        )
        seqio_no_matches = list(
            SeqIO.parse(self.data_dir / "no_matches.fasta", "fasta")
        )
        # With SeqIO.
        # Test with query and subject from seqs.
        for subject in seqio_subjects:
            with TabularBlastnSearch.from_sequences(
                    seqio_query,
                    subject,
            ) as search:
                self.assertGreater(search.hits.shape[0], 0)
                self.assertColumnsEqual(
                    search.hits.qseqid.str.removeprefix("from_"),
                    search.hits.sseqid
                )
            hits = blastn_from_sequences(seqio_query, subject)
            self.assertGreater(hits.shape[0], 0)
            self.assertColumnsEqual(
                hits.qseqid.str.removeprefix("from_"),
                hits.sseqid
            )
        with TabularBlastnSearch.from_sequences(
                seqio_no_matches,
                seqio_query
        ) as search:
            self.assertEqual(search.hits.shape[0], 0)
        # Test with query from seqs.
        for subject in self.data_dir.glob("seqs_*.fasta"):
            with TabularBlastnSearch.from_sequences(
                    subject=subject,
                    query_seqs=seqio_query
            ) as search:
                self.assertGreater(search.hits.shape[0], 0)
                self.assertColumnsEqual(
                    search.hits.qseqid.str.removeprefix("from_"),
                    search.hits.sseqid
                )
            hits = blastn_from_sequences(
                subject=subject,
                query_seqs=seqio_query
            )
            self.assertGreater(hits.shape[0], 0)
            self.assertColumnsEqual(
                hits.qseqid.str.removeprefix("from_"),
                hits.sseqid
            )
        with TabularBlastnSearch.from_sequences(
                subject=self.data_dir / "no_matches.fasta",
                query_seqs=seqio_query
        ) as search:
            self.assertEqual(search.hits.shape[0], 0)
        # Test with subject from seqs.
        for subject in seqio_subjects:
            with TabularBlastnSearch.from_sequences(
                    subject_seqs=subject,
                    query=self.data_dir / "queries.fasta",
            ) as search:
                self.assertGreater(search.hits.shape[0], 0)
                self.assertColumnsEqual(
                    search.hits.qseqid.str.removeprefix("from_"),
                    search.hits.sseqid
                )
            hits = blastn_from_sequences(
                subject_seqs=subject,
                query=self.data_dir / "queries.fasta"
            )
            self.assertGreater(hits.shape[0], 0)
            self.assertColumnsEqual(
                hits.qseqid.str.removeprefix("from_"),
                hits.sseqid
            )
        with TabularBlastnSearch.from_sequences(
                subject_seqs=seqio_no_matches,
                query=self.data_dir / "queries.fasta"
        ) as search:
            self.assertEqual(search.hits.shape[0], 0)
        # Without SeqIO.
        subjects = [[str(t.seq) for t in s] for s in seqio_subjects]
        query = [str(s.seq) for s in seqio_query]
        no_matches = [str(s.seq) for s in seqio_no_matches]
        for i, subject in enumerate(subjects):
            with TabularBlastnSearch.from_sequences(
                    subject_seqs=subject,
                    query_seqs=query
            ) as search:
                self.assertGreater(search.hits.shape[0], 0)
                self.assertColumnsEqual(
                    pd.to_numeric(
                        search.hits.qseqid.str.removeprefix("seq_")
                    )*4,
                    pd.to_numeric(
                        search.hits.sseqid.str.removeprefix("seq_")
                    ) + 3*i
                )
            hits = blastn_from_sequences(query, subject)
            self.assertGreater(hits.shape[0], 0)
            self.assertColumnsEqual(
                pd.to_numeric(
                    search.hits.qseqid.str.removeprefix("seq_")
                )*4,
                pd.to_numeric(
                    search.hits.sseqid.str.removeprefix("seq_")
                ) + 3*i                
            )
        with TabularBlastnSearch.from_sequences(
                subject_seqs=no_matches,
                query_seqs=query
        ) as search:
            self.assertEqual(search.hits.shape[0], 0)
        # Hybrid.
        for i, subject in enumerate(subjects):
            with TabularBlastnSearch.from_sequences(
                    subject_seqs=subject,
                    query_seqs=seqio_query
            ) as search:
                self.assertGreater(search.hits.shape[0], 0)
                self.assertColumnsEqual(
                    pd.to_numeric(
                        search.hits.qseqid.str.removeprefix("from_seq")
                    ),
                    pd.to_numeric(
                        search.hits.sseqid.str.removeprefix("seq_")
                    ) + 3*i
                )
            hits = blastn_from_sequences(seqio_query, subject)
            self.assertGreater(hits.shape[0], 0)
            self.assertColumnsEqual(
                pd.to_numeric(
                    search.hits.qseqid.str.removeprefix("from_seq")
                ),
                pd.to_numeric(
                    search.hits.sseqid.str.removeprefix("seq_")
                ) + 3*i                
            )
        with TabularBlastnSearch.from_sequences(
                seqio_query,
                no_matches,
        ) as search:
            self.assertEqual(search.hits.shape[0], 0)
        for i, subject in enumerate(seqio_subjects):
            with TabularBlastnSearch.from_sequences(
                    query,
                    subject,
            ) as search:
                self.assertGreater(search.hits.shape[0], 0)
                self.assertColumnsEqual(
                    pd.to_numeric(
                        search.hits.qseqid.str.removeprefix("seq_")
                    )*4,
                    pd.to_numeric(
                        search.hits.sseqid.str.removeprefix("seq")
                    )
                )
            hits = blastn_from_sequences(query, subject)
            self.assertGreater(hits.shape[0], 0)
            self.assertColumnsEqual(
                pd.to_numeric(
                    search.hits.qseqid.str.removeprefix("seq_")
                )*4,
                pd.to_numeric(
                    search.hits.sseqid.str.removeprefix("seq")
                )
            )
        with TabularBlastnSearch.from_sequences(
                query,
                seqio_no_matches,
        ) as search:
            self.assertEqual(search.hits.shape[0], 0)

