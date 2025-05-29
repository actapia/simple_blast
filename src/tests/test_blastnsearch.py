import os

from pathlib import Path
from contextlib import contextmanager

import numpy as np

from simple_blast.blasting import (
    BlastnSearch,
    default_out_columns,
    NotInDatabaseError,
    blastn_from_files
)
from simple_blast.blastdb_cache import BlastDBCache
from .simple_blast_test import (
    SimpleBlastTestCase,
    parse_blast_command,
)

@contextmanager
def temporary_os_environ(**kwargs):
    try:
        old_environ = dict(os.environ)
        os.environ |= kwargs
        yield
    finally:
        os.environ = old_environ

class TestBlastnSearch(SimpleBlastTestCase):
    def test_construction(self):
        subject_str = "subject.fasta"
        query_str = "query.fasta"
        subject_path = Path(subject_str)
        query_path = Path(query_str)
        subject_list = [subject_path]
        # Basic subject and query strings.
        res = BlastnSearch(subject_str, query_str, 11)
        self.assertEqual(list(res.seq1_path), subject_list)
        self.assertEqual(res.seq2_path, query_path)
        self.assertEqual(list(res.subject), subject_list)
        self.assertEqual(res.query, query_path)
        # From paths.
        res = BlastnSearch(subject_path, query_path,  11)
        self.assertEqual(list(res.seq1_path), subject_list)
        self.assertEqual(res.seq2_path, query_path)
        self.assertEqual(list(res.subject), subject_list)
        self.assertEqual(res.query, query_path)
        # From collection of paths.
        subject_set = {subject_path, query_path}
        res = BlastnSearch(subject_set, query_path, 11)
        self.assertEqual(set(res.seq1_path), subject_set)
        self.assertEqual(set(res.subject), subject_set)
        # # Check setting output columns.
        # self.assertEqual(list(res.out_columns), default_out_columns)
        # new_out_columns = ["foo", "bar"]
        # res = BlastnSearch(
        #     subject_path,
        #     query_path,
        #     out_columns=new_out_columns
        # )
        # self.assertEqual(list(res.out_columns), new_out_columns)
        # res = BlastnSearch(
        #     subject_path,
        #     query_path,
        #     additional_columns=new_out_columns
        # )
        # self.assertEqual(
        #     list(res.out_columns),
        #     default_out_columns + new_out_columns
        # )
        # Check setting remaining parameters.
        evalue = 1e-99
        cache = BlastDBCache("example_dir")
        threads = 12
        task = "foo"
        max_targets = 100
        negative_seqidlist = "apples"
        perc_ident = 90
        res = BlastnSearch(
            subject_path,
            query_path,
            11,
            evalue=evalue,
            db_cache=cache,
            threads=threads,
            dust=False,
            task=task,
            max_targets=max_targets,
            n_seqidlist=negative_seqidlist,
            perc_ident=90,
            debug=True            
        )
        self.assertEqual(res.evalue, evalue)
        self.assertEqual(res.db_cache, cache)
        self.assertEqual(res.threads, threads)
        self.assertFalse(res.dust)
        self.assertEqual(res.task, task)
        self.assertEqual(res.max_targets, max_targets)
        self.assertEqual(res.negative_seqidlist, negative_seqidlist)
        self.assertTrue(res.debug)
        self.assertEqual(res.perc_identity, perc_ident)

    def test_build_blast_command_basic(self):
        evalue = 1e-99
        threads = 12
        task = "foo"
        max_targets = 100
        negative_seqidlist = "apples"
        subject_str = "subject.fasta"
        query_str = "query.fasta"
        subject_path = Path(subject_str)
        query_path = Path(query_str)
        negative_seqidlist = "apples"
        new_out_columns = ["foo", "bar"]
        # self.assertDictIsSubset(
        #     {
        #         "outfmt": " ".join(
        #             ["6"] + new_out_columns
        #         )
        #     },
        #     kwargs
        # )
        # Test with other parameters.
        perc_ident = 90
        args, kwargs = parse_blast_command(
            list(
                BlastnSearch(
                    subject_path,
                    query_path,
                    11,
                    evalue=evalue,
                    threads=threads,
                    dust=False,
                    task=task,
                    max_targets=max_targets,
                    n_seqidlist=negative_seqidlist,
                    debug=True,
                    perc_ident=perc_ident
                )._build_blast_command().argument_iter()
            )[1:]
        )
        self.assertDictIsSubset(
            {
                "evalue": str(evalue),
                "num_threads": str(threads),
                "dust": "no",
                "task": str(task),
                "max_target_seqs": str(max_targets),
                "negative_seqidlist": negative_seqidlist,
                "perc_identity": str(perc_ident),
                "outfmt": str(11)
            },
            kwargs
        )

    def test_missing_executable(self):
        with temporary_os_environ(PATH="."):
            search = BlastnSearch(
                self.data_dir / "seqs_0.fasta",
                self.data_dir / "queries.fasta",
                11
            )
            with self.assertRaises(FileNotFoundError):
                search.get_output()

    def test_multiple_subjects(self):
        search = BlastnSearch(
            [self.data_dir / x for x in ["seqs_0.fasta", "seqs_1.fasta"]],
            self.data_dir / "queries.fasta",
            11
        )
        with self.assertRaises(NotInDatabaseError):
            search.get_output()
