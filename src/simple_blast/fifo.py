import io
import os
import tempfile
import functools
import time
import select
import errno
import fcntl
from threading import Thread, Event
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

def io_thread_wrap(f, error_event):
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except Exception as e:
            error_event.set()
            raise e
    return wrapped

class FIFOError(Exception):
    pass
        
class IOFIFO(FIFO):
    def _post_open(self, f):
        self._opened.set()
        
    def fifo_open(self, f, mode):
        def open_(*args, **kwargs):
            try:
                try:
                    res = open(os.open(f, mode), *args, **kwargs)
                    self._post_open(res)
                except Exception as e:
                    raise e
                return res
            finally:
                self._opened.set()
        return open_

    def __init__(self, f, mode, destroy_mode, suffix=""):
        super().__init__(suffix)
        self._run = f
        self._mode = mode
        self._destroy_mode = destroy_mode
        self._error_event = Event()

    def _get_args(self):
        return tuple()
        
    def create(self):
        """Create the FIFO and prepare to write."""
        super().create()
        self._opened = Event()
        self._thread = Thread(
            target=io_thread_wrap(self._run, self._error_event),
            args=(
                self.fifo_open(self.name, self._mode),
            ) + self._get_args(),
        )
        self._thread.start()

    def destroy(self):
        """Destroy the FIFO."""
        if self._thread.is_alive():
            self._clean_up_thread()
            self._thread.join()
            if self._error_event.is_set():
                raise FIFOError("FIFO thread failed.")
        super().destroy()

def ignored_sigpipe(f):
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except BrokenPipeError:
            pass
    return wrapped

class WriterFIFO(IOFIFO):
    """Used for creating temporary FIFO file for writing."""
    def __init__(self, f, suffix="", ignore_sigpipe=True):
        super().__init__(
            ignored_sigpipe(f) if ignore_sigpipe else f,
            os.O_WRONLY,
            os.O_RDONLY | os.O_NONBLOCK,
            suffix=suffix
        )

    def _clean_up_thread(self):
        fd = os.open(self._name, self._destroy_mode)
        self._opened.wait()
        os.close(fd)

def write_data(open_, data, mode):
    with open_(mode) as f:
        f.write(data)

class BinaryWriterFifo(WriterFIFO):
    def __init__(self, data, suffix=""):
        super().__init__(
            functools.partial(write_data, data=data, mode="wb"),
            suffix=suffix
        )

class TextWriterFifo(WriterFIFO):
    def __init__(self, data, suffix=""):
        super().__init__(
            functools.partial(write_data, data=data, mode="wt"),
            suffix=suffix,
        )

def read_thread(open_, io_, mode):
    with open_(mode) as f:
        res = io_.write(f.read())

class ReaderFifo(IOFIFO):
    def __init__(
            self,
            io_=io.StringIO,
            read_mode="r",
            suffix="",
            ignore_enxio=True,
    ):
        super().__init__(
            read_thread,
            os.O_RDONLY | os.O_NONBLOCK,
            os.O_WRONLY | os.O_NONBLOCK,
            suffix=suffix
        )
        self._io = io_()
        self._read_mode = read_mode
        self._ignore_enxio = ignore_enxio

    def _post_open(self, f):
        super()._post_open(f)
        poll = select.poll()
        poll.register(f.fileno(), select.POLLIN)
        poll.poll()
        fcntl.fcntl(f.fileno(), fcntl.F_SETFL, self._mode & ~os.O_NONBLOCK)

    def _clean_up_thread(self):
        self._opened.wait()
        try:
            fd = os.open(self._name, self._destroy_mode)
            os.close(fd)
        except OSError as e:
            if not self._ignore_enxio or e.args[0] != errno.ENXIO:
                raise e

    def get(self):
        return self._io.getvalue()

    def _get_args(self):
        return super()._get_args() + (self._io, self._read_mode)
