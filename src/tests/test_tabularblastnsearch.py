import numpy as np
from pathlib import Path

from simple_blast.blasting import (
    TabularBlastnSearch,
    default_out_columns,
    NotInDatabaseError,
    blastn_from_files
)
from .simple_blast_test import (
    SimpleBlastTestCase,
    parse_blast_command,
)

class TestTabularBlastnSearch(SimpleBlastTestCase):
    def test_construction(self):
        subject_str = "subject.fasta"
        query_str = "query.fasta"
        subject_path = Path(subject_str)
        query_path = Path(query_str)
        subject_set = {subject_path, query_path}
        res = TabularBlastnSearch(subject_set, query_path)
        self.assertEqual(list(res.out_columns), default_out_columns)
        new_out_columns = ["foo", "bar"]
        res = TabularBlastnSearch(
            subject_path,
            query_path,
            out_columns=new_out_columns
        )
        self.assertEqual(list(res.out_columns), new_out_columns)
        res = TabularBlastnSearch(
            subject_path,
            query_path,
            additional_columns=new_out_columns
        )
        self.assertEqual(
            list(res.out_columns),
            default_out_columns + new_out_columns
        )

    def test_build_blast_command_basic(self):
        subject_str = "subject.fasta"
        query_str = "query.fasta"
        subject_path = Path(subject_str)
        query_path = Path(query_str)
        new_out_columns = ["foo", "bar"]
        args, kwargs = parse_blast_command(
            list(
                TabularBlastnSearch(
                    subject_path,
                    query_path,
                )._build_blast_command().argument_iter()
            )[1:]
        )
        self.assertDictIsSubset(
            {
                "subject": subject_str,
                "query": query_str,
                "outfmt": " ".join(["6"] + default_out_columns)
            },
            kwargs
        )
        for x in ["evalue", "num_threads", "dust", "max_target_seqs"]:
            self.assertIn(x, kwargs)
        for x in ["task", "negative_seqidlist"]:
            self.assertNotIn(x, kwargs)

        # Test columns.
        args, kwargs = parse_blast_command(
            list(
                TabularBlastnSearch(
                    subject_path,
                    query_path,
                    out_columns=new_out_columns
                )._build_blast_command().argument_iter()
            )[1:]
        )
        self.assertDictIsSubset(
            {
                "outfmt": " ".join(["6"] + new_out_columns)
            },
            kwargs
        )
        args, kwargs = parse_blast_command(
            list(
                TabularBlastnSearch(
                    subject_path,
                    query_path,
                    additional_columns=new_out_columns
                )._build_blast_command().argument_iter()
            )[1:]
        )
        self.assertDictIsSubset(
            {
                "outfmt": " ".join(
                    ["6"] + default_out_columns + new_out_columns
                )
            },
            kwargs
        )

    def test_basic_search(self):
        for subject in self.data_dir.glob("seqs_*.fasta"):
            search = TabularBlastnSearch(
                subject,
                self.data_dir / "queries.fasta"
            )
            self.assertGreater(search.hits.shape[0], 0)
            #self.assertEqual(5,4)
            self.assertColumnsEqual(
                search.hits.qseqid.str.removeprefix("from_"),
                search.hits.sseqid
            )
            self.assertEqual(
                list(search.hits.columns),
                default_out_columns
            )
        search = TabularBlastnSearch(
            self.data_dir / "no_matches.fasta",
            self.data_dir / "queries.fasta"
        )
        self.assertEqual(search.hits.shape[0], 0)
        
    def test_out_columns(self):
        search = TabularBlastnSearch(
            self.data_dir / "seqs_0.fasta",
            self.data_dir / "queries.fasta"
        )
        self.assertEqual(
            list(search.hits.columns),
            default_out_columns
        )
        new_out_columns = ["slen", "nident"]
        search = TabularBlastnSearch(
            self.data_dir / "seqs_0.fasta",
            self.data_dir / "queries.fasta",
            out_columns=new_out_columns
        )
        self.assertEqual(
            list(search.hits.columns),
            new_out_columns
        )
        search = TabularBlastnSearch(
            self.data_dir / "seqs_0.fasta",
            self.data_dir / "queries.fasta",
            additional_columns=new_out_columns
        )
        self.assertEqual(
            list(search.hits.columns),
            default_out_columns + new_out_columns
        )

    def test_blastn_from_files(self):
        for subject in self.data_dir.glob("seqs_*.fasta"):
            hits = blastn_from_files(
                subject,
                self.data_dir / "queries.fasta"
            )
            self.assertGreater(hits.shape[0], 0)
            #self.assertEqual(5,4)
            self.assertColumnsEqual(
                hits.qseqid.str.removeprefix("from_"),
                hits.sseqid
            )
            self.assertEqual(
                list(hits.columns),
                default_out_columns
            )
        hits = blastn_from_files(
            self.data_dir / "no_matches.fasta",
            self.data_dir / "queries.fasta"
        )
        self.assertEqual(hits.shape[0], 0)

    def test_column_dtypes(self):
        search = TabularBlastnSearch(
            self.data_dir / "seqs_0.fasta",
            self.data_dir / "queries.fasta",
        )
        search.column_dtypes = search.column_dtypes | {"bitscore": np.float64}
        self.assertEqual(search.hits["bitscore"].dtype, np.float64)
        search = TabularBlastnSearch(
            self.data_dir / "seqs_0.fasta",
            self.data_dir / "queries.fasta",
        )
        self.assertNotEqual(search.hits["bitscore"].dtype, np.float64)

    def test_sstrand_categorical(self):
        self.assertHasAttr(
            TabularBlastnSearch(
                self.data_dir / "seqs_0.fasta",
                self.data_dir / "queries.fasta",
                additional_columns=["sstrand"]
            ).hits["sstrand"],
            "cat"
        )
