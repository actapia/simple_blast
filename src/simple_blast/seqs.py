import io
import os
import tempfile
import textwrap
import threading
import functools
import time
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

def _write_fasta_fallback(f: io.TextIOBase, seqs: Iterable[str]):
    for i, s in enumerate(seqs):
        f.write(
            ">seq_{}\n{}\n".format(i, textwrap.fill(s, width=80))
        )
    f.flush()

def _write_fasta(open_, seqs: Iterable[SeqType]):
    with open_("w") as f:
        if Bio is not None:
            try:
                Bio.SeqIO.write(seqs, f, "fasta")
                return
            except AttributeError:
                pass
        _write_fasta_fallback(f, seqs)

class SeqsAsFile(WriterFIFO):
    """Used for creating temporary FIFOs for sequences."""
    def __init__(self, seqs: Iterable[SeqType]):
        """Construct object for making a FIFO for the sequences."""
        super().__init__(functools.partial(_write_fasta, seqs=seqs))
