import subprocess
import tempfile
import itertools
from pathlib import Path
from collections.abc import Iterable
from collections import namedtuple
from .blastdb import read_nin_metadata, UnsupportedDatabaseFormatException
import json

# def read_nal_title(nal):
#     with open(nal, "r") as nal_file:
#         for l in nal_file:
#             if not l.startswith("#"):
#                 split = l.rstrip().split(" ")
#                 if split[0] == "TITLE":
#                     return frozenset(
#                         map(
#                             Path,
#                             split[1:]
#                         )
#                     )

def read_js_title(js):
    with open(js, "r") as js_file:
        return frozenset(
            map(
                Path,
                json.loads(js_file.read())["description"].split()
            )
        )

def read_nin_title(nin):
    return frozenset(map(Path, read_nin_metadata(nin).title.split()))

title_parsers = {"*.njs": read_js_title, "*.nin": read_nin_title}

def get_existing(location):
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


def to_path_iterable(ix, cls=frozenset) -> Iterable[Path]:
    if isinstance(ix, str):
        ix = [Path(ix)]
    try:
        return cls(map(Path, ix))
    except TypeError:
        return cls({ix})

def convert_index(f, self_i=0, paths_i=1):
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

class BlastDBCache:
    def __init__(
            self,
            location,
            find_existing=True,
            parse_seqids=False,
            absolute=False
    ):
        self.location = location
        self._cache = {}
        if find_existing:
            self._cache = dict(get_existing(location))
        self._parse_seqids = parse_seqids
        self._absolute = absolute

    @property
    def absolute(self):
        return self._absolute

    @property
    def parse_seqids(self):
        return self._parse_seqids

    def _build_makeblastdb_command(self, seq_file_paths, db_name):
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

    @convert_index
    def makedb(self, seq_file_paths):
        if seq_file_paths in self._cache:
            return
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
        self._cache[seq_file_paths] = db_name

    @convert_index
    def get(self, k):
        return self._cache[k]

    @convert_index
    def delete(self, k):
        del self._cache[k]

    @convert_index
    def contains(self, k):
        return k in self._cache

    def __getitem__(self, k):
        return self.get(k)

    def __delitem__(self, k):
        self.delete(k)

    def __contains__(self, k):
        return self.contains(k)
        
        
