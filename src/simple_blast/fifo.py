import io
import os
import tempfile
import functools
from threading import Thread
from contextlib import AbstractContextManager

class FIFO(AbstractContextManager):
    def __init__(self, suffix=""):
        self._suffix = suffix

    def create(self):
        """Create the FIFO."""
        self._name = tempfile.mktemp(suffix=self._suffix)
        os.mkfifo(self.name)

    @property
    def name(self):
        """Return the file path associated with the FIFO."""
        return self._name

    def destroy(self):
        """Destroy the FIFO."""
        os.remove(self._name)
        self._name = None

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy()

class WriterFIFO(FIFO):
    """Used for creating temporary FIFO file for writing."""
    def __init__(self, f, suffix=""):
        super().__init__(suffix=suffix)
        self._run = f

    def create(self):
        """Create the FIFO and prepare to write."""
        super().create()
        self._write_thread = Thread(
            target=self._run,
            args=(self.name,)
        )
        self._write_thread.start()

    def destroy(self):
        """Destroy the FIFO."""
        if self._write_thread.is_alive():
            os.open(self._name, os.O_NONBLOCK | os.O_RDONLY)
            self._write_thread.join()
        super().destroy()

def write_binary_data(data, path):
    with open(path, "wb") as f:
        f.write(data)

def write_text_data(data, path):
    with open(path, "w") as f:
        f.write(data)

class BinaryWriterFifo(WriterFIFO):
    def __init__(self, data, suffix=""):
        super().__init__(
            functools.partial(write_binary_data, data),
            suffix=suffix
        )

class TextWriterFifo(WriterFIFO):
    def __init__(self, data, suffix=""):
        super().__init__(
            functools.partial(write_text_data, data),
            suffix=suffix,
        )

def read_thread(io_, mode, path):
    with open(path, mode) as f:
        io_.write(f.read())

class ReaderFifo(FIFO):
    def __init__(self, io_=io.StringIO, mode="r", suffix=""):
        super().__init__(suffix=suffix)
        self._io = io_()
        self._mode = mode

    def get(self):
        return self._io.getvalue()
    
    def create(self):
        """Create the FIFO and prepare to write."""
        super().create()
        self._read_thread = Thread(
            target=read_thread,
            args=(self._io, self._mode, self.name)
        )
        self._read_thread.start()

    def destroy(self):
        """Destroy the FIFO."""
        if self._read_thread.is_alive():
            os.close(os.open(self._name, os.O_NONBLOCK | os.O_WRONLY))
            self._read_thread.join()
        super().destroy()
