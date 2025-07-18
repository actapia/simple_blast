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

from typing import Callable, IO, Any

class FIFO(AbstractContextManager):
    """Provides an interface for working with a Unix FIFO (named pipe).

    This class automatically creates a temporary file associated with the FIFO
    using mktemp. The path to the temporary file can be accessed via the name
    property when inside the FIFO context.

    FIFO is a context manager. When the FIFO context is entered, the temporary
    file to be used as the FIFO is created, and the file is made a FIFO with
    os.mkfifo. When the FIFO context is exited, the temporary file is deleted.

    This class has no public attributes.
    """
    def __init__(self, suffix=""):
        """Construct a FIFO context manager.

        Parameters:
            suffix (str): The suffix for the temporary file.
        """
        self._suffix = suffix

    def create(self):
        """Create the FIFO."""
        self._name = tempfile.mktemp(suffix=self._suffix)
        os.mkfifo(self.name)

    @property
    def name(self) -> str:
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

def io_thread_wrap(f: Callable, error_event: Event) -> Callable:
    """Wraps a function to be used in an IOFIFO thread.

    The purpose of this function is to set the provided error_event Event
    whenever an error is encountered.

    Parameters:
        f:           The function to wrap.
        error_event: The threading Event to set when an error has ocurred.

    Returns:
        A wrapped version of f that sets the error_event when it has en error.
    """
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except Exception as e:
            error_event.set()
            raise e
    return wrapped

class FIFOError(Exception):
    """Exception raised in main thread when a FIFO thread has an Exception."""
    pass
        
class IOFIFO(FIFO):
    """Base class for classes for performing I/O operations with FIFOs.

    An IOFIFO will create a thread that runs a provided function when its
    context is entered (or when the create method is called). This function is
    intended to perform I/O on the FIFO.  The function must accept as its first
    argument an "open" function that behaves similarly to the built-in open
    function but does not allow a file path or descriptor to be
    specified. Subclasses of IOFIFO also pass additional arguments.
    """
    def _post_open(self, f: IO):
        """Runs after the FIFO is opened in the IOFIFO's _run function."""
        self._opened.set()
        
    def _fifo_open(self) -> Callable:
        """Create a wrapped open function to provide to the function to run.

        The wrapped open function opens the FIFO in the appropriate mode,
        performs any necessary post-open steps (e.g., for the purpose of
        synchronization), and sets the _opened Event (also possibly for
        snychronization).

        Returns:
            A function that can be used to open the FIFO.
        """
        def open_(*args, **kwargs):
            try:
                try:
                    res = open(os.open(self.name, self._mode), *args, **kwargs)
                    self._post_open(res)
                except Exception as e:
                    raise e
                return res
            finally:
                self._opened.set()
        return open_

    def __init__(
            self,
            f: Callable,
            mode: int,
            destroy_mode: int,
            suffix: str = ""
    ):
        """Construct an IOFIFO.

        Parameters:
            f:                  I/O performing function to run in thread.
            mode (int):         File mode to use for opening the FIFO in thread.
            destroy_mode (int): File mode to use when unblocking thread.
            suffix (str):       Suffix for FIFO temporary file.
        """
        super().__init__(suffix)
        self._run = f
        self._mode = mode
        self._destroy_mode = destroy_mode
        self._error_event = Event()

    def _get_args(self) -> tuple:
        """Get the arguments to pass to the function to be run, except open_."""
        return tuple()
        
    def create(self):
        """Create the FIFO and prepare to read or write."""
        super().create()
        self._opened = Event()
        self._thread = Thread(
            target=io_thread_wrap(self._run, self._error_event),
            args=(
                self._fifo_open(),
            ) + self._get_args(),
        )
        self._thread.start()

    def _clean_up_thread(self):
        """Run steps necessary to get thread to terminate."""
        raise NotImplementedError()

    def destroy(self):
        """Destroy the FIFO, waiting for the reader or writer thread."""
        if self._thread.is_alive():
            self._clean_up_thread()
            self._thread.join()
            if self._error_event.is_set():
                raise FIFOError("FIFO thread failed.")
        super().destroy()

def ignored_sigpipe(f: Callable) -> Callable:
    """Wraps a function to ignore BrokenPipeError (caused by SIGPIPE)."""
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except BrokenPipeError:
            pass
    return wrapped

class WriterFIFO(IOFIFO):
    """Used to write data to a FIFO.

    WriterFIFO is constructed with a function that will be run in a separate
    thread. The function is expected to write data to the FIFO using the
    file-like object returned by the "open" function provided as the function's
    first parameter.
    """
    def __init__(self, f, suffix="", ignore_sigpipe=True):
        """Construct a WriterFIFO.

        Parameters:
            f:                     Writer function to run.
            suffix (str):          Suffix to use for FIFO's temporary file.
            ignore_sigpipe (bool): Ignore BrokenPipeError in writer thread.
        """
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

def write_data(
        open_: Callable[*tuple[Any, ...], IO],
        data: str | bytes,
        mode: str
):
    """Write the given data to the file-like object returned by a function.

    If data is a str, mode must be a text mode. Otherwise, modee must be a
    binary mode.
    
    Parameters:
        open_:      Function with which to open the file-like object.
        data:       Data to write to the file-like object.
        mode (str): Mode in which to open the file-like object.
    """
    with open_(mode) as f:
        f.write(data)

class BinaryWriterFIFO(WriterFIFO):
    """Writes given binary data (bytes) to the FIFO."""
    def __init__(self, data: bytes, suffix=""):
        """Construct a BinaryWriterFIFO
        
        Parameters:
            data (bytes): The binary data to write to the FIFO.
            suffix (str): The suffix for the FIFO temporary file.
        """
        super().__init__(
            functools.partial(write_data, data=data, mode="wb"),
            suffix=suffix
        )

class TextWriterFIFO(WriterFIFO):
    """Writes given text data (str) to the FIFO."""
    def __init__(self, data, suffix=""):
        """Construct a TextWriterFIFO.

        Parameters:
            data (str):   The binary data to write to the FIFO.
            suffix (str): The suffix for the FIFO temporary file.
        """
        super().__init__(
            functools.partial(write_data, data=data, mode="wt"),
            suffix=suffix,
        )

def read_thread(open_: Callable[*tuple[Any, ...], IO], io_: IO, mode: str):
    """Write contents read from file-like created by function to a file-like.

    Parameters:
        open_:      Function with which to open the file-like object.
        io_:        File-like object to which to write.
        mode (str): Mode in which to open file-like object returned by open_.
    """
    with open_(mode) as f:
        io_.write(f.read())

class ReaderFIFO(IOFIFO):
    """Used to read data from a FIFO.

    ReaderFIFO reads data from a FIFO and writes it to another file-like object.
    By default, the file-like object created is an instance of io.StringIO so
    that the read data may be retrieved using the get method of the io property
    of te ReaderFIFO, or by using the get method of the ReaderFIFO.
    """
    def __init__(
            self,
            io_: Callable[[], IO] = io.StringIO,
            read_mode: str = "r",
            suffix: str = "",
            ignore_enxio: bool = True,
    ):
        """Construct a ReaderFIFO.

        Parameters:
            io_:                 Callable to make file-like object for writing.
            read_mode (str):     Mode in which to open FIFO for reading.
            suffix (str):        Suffix to use for FIFO temporary file.
            ignore_enxio (bool): Ignore ENXIO signals during cleanup.
        """
        super().__init__(
            read_thread,
            os.O_RDONLY | os.O_NONBLOCK,
            os.O_WRONLY | os.O_NONBLOCK,
            suffix=suffix
        )
        self._io = io_()
        self._read_mode = read_mode
        self._ignore_enxio = ignore_enxio

    @property
    def io(self) -> IO:
        """The file-like object to which the FIFO contents are to be written."""
        return self._io

    def _post_open(self, f: IO):
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

    def _get_args(self) -> tuple[IO, str]:
        return super()._get_args() + (self._io, self._read_mode)
