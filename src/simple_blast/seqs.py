import io
import os
import tempfile
import textwrap
import functools
from pathlib import Path
from .fifo import WriterFIFO

from collections.abc import Iterable

SeqType = str

try:
    import Bio
    import Bio.SeqIO
    import Bio.SeqRecord
    SeqType = SeqType | Bio.SeqRecord.SeqRecord
except ImportError:
    Bio = None

def _write_fasta_fallback(seqs: Iterable[str], f: io.TextIOBase):
    for i, s in enumerate(seqs):
        f.write(
            ">seq_{}\n{}\n".format(i, textwrap.fill(s, width=80))
        )
    f.flush()

def _write_fasta(seqs: Iterable[SeqType], path: Path):
    with open(path, "w") as f:
        if Bio is not None:
            try:
                Bio.SeqIO.write(seqs, f, "fasta")
                return
            except AttributeError:
                pass
        _write_fasta_fallback(seqs, f)

class SeqsAsFile(WriterFIFO):
    """Used for creating temporary FIFOs for sequences."""
    def __init__(self, seqs: Iterable[SeqType]):
        """Construct object for making a FIFO for the sequences."""
        super().__init__(functools.partial(_write_fasta, seqs))
