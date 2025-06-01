import io
import os
import tempfile
import textwrap
from pathlib import Path
from contextlib import AbstractContextManager
from collections.abc import Iterable
from threading import Thread

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

def _write_thread(*args, **kwargs):
    _write_fasta(*args, **kwargs)

class SeqsAsFile(AbstractContextManager):
    """Used for creating temporary FIFOs for sequences."""
    def __init__(self, seqs: Iterable[SeqType]):
        """Construct object for making a FIFO for the sequences."""
        self._seqs = seqs
        self._name = None

    def create(self):
        """Create the FIFO and prepare to write."""
        self._name = tempfile.mktemp()
        os.mkfifo(self.name)
        self._write_thread = Thread(
            target=_write_thread,
            args=(self._seqs, self.name)
        )
        self._write_thread.start()

    def __enter__(self):
        self.create()
        return self

    @property
    def name(self):
        """Return the file path associated with the FIFO."""
        return self._name

    def destroy(self):
        """Destroy the FIFO."""
        if self._write_thread.is_alive():
            # Try to kill.
            #print("Trying to kill.")
            os.open(self._name, os.O_NONBLOCK | os.O_RDONLY)
            self._write_thread.join()
        os.remove(self._name)
        self._name = None

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy()
