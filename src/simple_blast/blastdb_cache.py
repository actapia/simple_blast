import subprocess
import os
import tempfile
import itertools
from pathlib import Path
from collections.abc import Iterable
from typing import Iterator, Callable
from .blastdb import read_nin_metadata, UnsupportedDatabaseFormatException
import json

def read_js_title(js: str | os.PathLike) -> frozenset[Path]:
    """Get file paths used to construct BLAST DB from a JSON file."""
    with open(js, "r") as js_file:
        return frozenset(
            map(
                Path,
                json.loads(js_file.read())["description"].split()
            )
        )

def read_nin_title(nin) -> frozenset[Path]:
    """Get file paths used to construct BLAST DB from a nin file."""
    return frozenset(map(Path, read_nin_metadata(nin).title.split()))

title_parsers = {"*.njs": read_js_title, "*.nin": read_nin_title}

CacheIndex = str | Iterable[str | os.PathLike]

def get_existing(
        location: str | os.PathLike
) -> Iterator[tuple[frozenset[Path], str]]:
    """Obtain existing BLAST databases created in the specified location.

    This function yield pairs representing BLAST databases. The first element is
    a frozenset containing the names/paths of the FASTA files indexed in the
    database. The second is the path to the database as used in a blastn
    command.

    Parameters:
        location: The location in which to search for existing databases.
    """        
    path_stems = set(
        map(
            lambda x: x.parent / x.name.split(".")[0],
            itertools.chain(
                *map(
                    Path(location).glob,
                    [
                        "*/*.njs",
                        "*/*.nin"
                    ]
                )
            )
        )
    )
    for stem in path_stems:
        for ext, parser in title_parsers.items():
            try:
                yield parser(next(stem.parent.glob(stem.name + ext))), stem
                break
            except (StopIteration, UnsupportedDatabaseFormatException):
                pass


def to_path_iterable(
        ix: str | Iterable[str | Path] | Path,
        cls = frozenset
) -> Iterable[Path]:
    """Convert a string or an iterable of values to an iterable of Paths."""
    if isinstance(ix, str):
        ix = [Path(ix)]
    try:
        return cls(map(Path, ix))
    except TypeError:
        return cls({ix})

def convert_index(
        self_i: int = 0, paths_i: int = 1
) -> Callable[[Callable], Callable]:
    """Creates a function wrapper that converts strs or paths to path iterables.

    The wrapper will also convert paths to absolute paths depending on the
    provided parameters to the inner function and the attributes of the self
    object.

    Parameters:
        self_i (int):  Index of the self argument.
        paths_i (int): Index of the path/paths argument.

    Returns:
        A function that can be used to wrap functions with the specified params.
    """
    def wrapper(f: Callable) -> Callable:
        """Wraps a function to convert one argument to a path iterable."""
        def inner(*args, **kwargs):
            args = list(args)
            args[paths_i] = to_path_iterable(args[paths_i])
            if kwargs.get("absolute") or args[self_i].absolute:
                args[paths_i] = frozenset(p.absolute() for p in args[paths_i])
            try:
                del kwargs["absolute"]
            except KeyError:
                pass
            return f(*args, **kwargs)
        return inner
    return wrapper

class BlastDBCache:
    """Keeps track of BLAST databases that index certain FASTA files.

    This can be used in conjunction with BlastnSearch objects to automatically
    use a created BLAST DB for searching against a set of subjects when such a
    DB is available.

    Attributes:
        location: The location in which the databases are found or created.
    """
    def __init__(
            self,
            location,
            find_existing=True,
            parse_seqids=False,
            absolute=False
    ):
        """Create a BlastDBCache in the provided location.

        Parameters:
            location:             Where the databases are found or created.
            find_existing (bool): Find existing databases in location.
            parse_seqids (bool):  Whether to parse IDs in FASTA headers.
            absolute (bool):      Whether to use absolute paths.
        """
        self.location = location
        self._cache = {}
        if find_existing:
            self._cache = dict(get_existing(location))
        self._parse_seqids = parse_seqids
        self._absolute = absolute

    @property
    def absolute(self) -> bool:
        """Whether to use absolute paths."""
        return self._absolute

    @property
    def parse_seqids(self) -> bool:
        """Whether to parse IDs in FASTA headers."""
        return self._parse_seqids

    def _build_makeblastdb_command(
            self,
            seq_file_paths: Iterable[str | os.PathLike],
            db_name: str
    ) -> list[str]:
        command = [
                "makeblastdb",
                "-in",
                " ".join(map(str, seq_file_paths)),
                "-out",
                db_name,
                "-dbtype",
                "nucl",
                "-hash_index" # Do I need this?
        ]
        if self._parse_seqids:
            command.append("-parse_seqids")
        return command

    @convert_index()
    def makedb(self, seq_file_paths: str | Iterable[str | os.PathLike]):
        """Create a DB from the given FASTA files and store it in the cache."""
        if seq_file_paths in self._cache:
            return
        # noinspection PyUnresolvedReferences
        prefix = next(iter(seq_file_paths)).stem
        if len(seq_file_paths) > 1:
            prefix = prefix + "+"
        tempdir = Path(
            tempfile.mkdtemp(
                prefix=prefix,
                dir=self.location
            )
        )
        db_name = str(tempdir / "db")
        proc = subprocess.Popen(
            self._build_makeblastdb_command(seq_file_paths, db_name),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        proc.communicate()
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, proc.args)
        # noinspection PyTypeChecker
        self._cache[seq_file_paths] = db_name

    @convert_index()
    def get(self, k: CacheIndex) -> str:
        """Get the BLAST database that indexes the given FASTA file(s)."""
        # noinspection PyTypeChecker
        return self._cache[k]

    @convert_index()
    def delete(self, k: CacheIndex):
        """Remove the database indexing the given file(s) from the cache."""
        # noinspection PyTypeChecker
        del self._cache[k]

    @convert_index()
    def contains(self, k: CacheIndex) -> bool:
        """Check if the given file(s) are indexed in a database in the cache."""
        return k in self._cache

    def __getitem__(self, k: CacheIndex) -> str:
        return self.get(k)

    def __delitem__(self, k: CacheIndex):
        self.delete(k)

    def __contains__(self, k: CacheIndex) -> bool:
        return self.contains(k)
        
        
