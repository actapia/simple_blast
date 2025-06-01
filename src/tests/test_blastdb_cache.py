import tempfile
import shutil
import os
from simple_blast.blastdb_cache import BlastDBCache, get_existing
from simple_blast.blasting import BlastnSearch, TabularBlastnSearch
from .simple_blast_test import (
    SimpleBlastTestCase,
    parse_blast_command
)
from pathlib import Path

class TestBlastDBCache(SimpleBlastTestCase):
    def test_construction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = BlastDBCache(temp_dir)
            self.assertEqual(cache.location, temp_dir)
            # Test other options.
            cache = BlastDBCache(
                temp_dir,
                find_existing=False,
                parse_seqids=True,
                absolute=True
            )
            self.assertTrue(cache.parse_seqids)
            self.assertTrue(cache.absolute)

    def test_build_makeblastdb_command_basic(self):
        filenames = ["foo", "bar"]
        db_name = "my_db"
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = BlastDBCache(temp_dir)
            args, kwargs = parse_blast_command(
                cache._build_makeblastdb_command(
                    filenames[:1],
                    db_name
                )
            )
            self.assertDictIsSubset(
                {
                    "in": filenames[0],
                    "out": db_name,
                    "dbtype": "nucl",
                    "hash_index": None
                },
                kwargs
            )
            args, kwargs = parse_blast_command(
                cache._build_makeblastdb_command(
                    filenames,
                    db_name
                )
            )
            self.assertDictIsSubset(
                {
                    "in": " ".join(filenames)
                },
                kwargs
            )
            cache = BlastDBCache(temp_dir, parse_seqids=True)
            args, kwargs = parse_blast_command(
                cache._build_makeblastdb_command(
                    filenames[:1],
                    db_name
                )
            )
            self.assertDictIsSubset(
                {
                    "parse_seqids": None
                },
                kwargs
            )

    def test_makedb_basic(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            cache = BlastDBCache(temp_dir)
            cache.makedb(self.data_dir / "seqs_0.fasta")
            db_dirs = list(temp_dir_path.glob("seqs_0*"))
            self.assertGreater(len(db_dirs), 0)
            db_dir = db_dirs[0]
            self.assertIsDirectory(db_dir)
            existing = dict(get_existing(temp_dir))
            self.assertEqual(len(existing), 1)
            self.assertCountEqual(
                next(iter(existing)),
                [self.data_dir / "seqs_0.fasta"]
            )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            cache = BlastDBCache(temp_dir)
            files = [
                self.data_dir / ("seqs_{}.fasta".format(x)) for x in range(2)
            ]
            cache.makedb(files)
            db_dirs = list(temp_dir_path.glob("seqs_*"))
            # if len(db_dirs) <= 0:
            #     from IPython import embed
            #     embed()
            self.assertGreater(len(db_dirs), 0)
            db_dir = db_dirs[0]
            self.assertIsDirectory(db_dir)
            existing = dict(get_existing(temp_dir))
            self.assertEqual(len(existing), 1)
            self.assertCountEqual(
                next(iter(existing)),
                files
            )
    def test_use_blastdb_cache(self):
        temp_files = {}
        for f in self.data_dir.glob("seqs_*.fasta"):
            temp_files[f] =  tempfile.NamedTemporaryFile(
                suffix=".fasta",
                delete=False
            )
            temp_files[f].close()
            shutil.copy(f, temp_files[f].name)
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test with a single sequence.
            cache = BlastDBCache(temp_dir)
            cache.makedb(temp_files[self.data_dir / "seqs_0.fasta"].name)
            # Test with two sequences.
            files = [
                temp_files[k].name for k in [
                    self.data_dir / f"seqs_{x}.fasta" for x in range(1, 3)
                ]
            ]
            cache.makedb(files)
            # Delete files used to make DBs to ensure we don't use them.
            for f in temp_files.values():
                os.remove(f.name)
            search = TabularBlastnSearch(
                subject=temp_files[self.data_dir / "seqs_0.fasta"].name,
                query=self.data_dir / "queries.fasta",
                db_cache=cache,
                #debug=True
            )
            self.assertEqual(
                list(search.hits.sseqid),
                ["seq0"]
            )
            search = TabularBlastnSearch(
                subject=files,
                query=self.data_dir / "queries.fasta",
                db_cache=cache
            )
            self.assertCountEqual(
                list(search.hits.sseqid),
                ["seq4", "seq8"]
            )

    def test_absolute(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = BlastDBCache(temp_dir, absolute=False)
            self.assertFalse(cache.absolute)
            cache.makedb(self.data_dir / "seqs_0.fasta")
            self.assertIn(self.data_dir / "seqs_0.fasta", cache)
            self.assertNotIn((self.data_dir / "seqs_0.fasta").absolute(), cache)
            cache.makedb(self.data_dir / "seqs_1.fasta", absolute=True)
            self.assertTrue(
                cache.contains(self.data_dir / "seqs_1.fasta",
                               absolute=True)
            )
            self.assertNotIn(self.data_dir / "seqs_1.fasta", cache)
            self.assertIn((self.data_dir / "seqs_1.fasta").absolute(), cache)
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = BlastDBCache(temp_dir, absolute=True)
            self.assertTrue(cache.absolute)
            cache.makedb(self.data_dir / "seqs_0.fasta")
            self.assertIn(self.data_dir / "seqs_0.fasta", cache)
            self.assertIn((self.data_dir / "seqs_0.fasta").absolute(), cache)

    def test_find_existing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = BlastDBCache(temp_dir)
            cache.makedb(self.data_dir / "seqs_0.fasta")
            cache = BlastDBCache(temp_dir, find_existing=True)
            self.assertIn(self.data_dir / "seqs_0.fasta", cache)
            files = [
                self.data_dir / ("seqs_{}.fasta".format(x)) for x in range(2)
            ]
            cache.makedb(files)
            cache = BlastDBCache(temp_dir, find_existing=True)
            self.assertIn(files, cache)
