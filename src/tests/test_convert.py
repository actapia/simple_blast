import io
import pandas as pd
from simple_blast.blasting import (
    default_out_columns,
    TabularBlastnSearch
)
from simple_blast.convert import (
    blast_format_file,
    blast_format_bytes
)
from pathlib import Path
from .simple_blast_test import (
    SimpleBlastTestCase,
)

import Bio.Align

class TestConvert(SimpleBlastTestCase):
    def test_blast_format_file_to_file(self):
        archive_file = self.data_dir / "seqs0--queries.asn1"
        # To format 6
        out_file = self.data_dir / "seqs0--queries.blastn6"
        blast_format_file(6, archive_file, out_file)
        TabularBlastnSearch.parse_hits(out_file, default_out_columns)
        # To format 17
        out_file = self.data_dir / "seqs0--queries.sam"
        blast_format_file(17, archive_file, out_file)
        Bio.Align.parse(out_file, "sam")

    def test_blast_format_file_to_bytes(self):
        archive_file = self.data_dir / "seqs0--queries.asn1"
        # To format 6
        res = blast_format_file(6, archive_file)
        TabularBlastnSearch.parse_hits(io.BytesIO(res), default_out_columns)
        # To format 17
        res = blast_format_file(17, archive_file)
        Bio.Align.parse(io.TextIOWrapper(io.BytesIO(res)), "sam")

    def test_blast_format_bytes_to_file(self):
        archive_file = self.data_dir / "seqs0--queries.asn1"
        with open(archive_file, "rb") as af:
            archive = af.read()
        # To format 6
        out_file = self.data_dir / "seqs0--queries.blastn6"
        blast_format_bytes(6, archive, out_file)
        TabularBlastnSearch.parse_hits(out_file, default_out_columns)
        # To format 17
        out_file = self.data_dir / "seqs0--queries.sam"
        blast_format_bytes(17, archive, out_file)
        Bio.Align.parse(out_file, "sam")

    def test_blast_format_bytes_to_bytes(self):
        archive_file = self.data_dir / "seqs0--queries.asn1"
        with open(archive_file, "rb") as af:
            archive = af.read()
        # To format 6
        res = blast_format_bytes(6, archive)
        TabularBlastnSearch.parse_hits(io.BytesIO(res), default_out_columns)
        # To format 17
        res = blast_format_bytes(17, archive)
        Bio.Align.parse(io.TextIOWrapper(io.BytesIO(res)), "sam")
