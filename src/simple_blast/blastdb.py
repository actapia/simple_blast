import os
from dataclasses import dataclass
import dateparser
import datetime
from typing import Optional

# This code is based on the CSeqDBIdxFile::CSeqDBIdxFile constructor from the
# NCBI C++ Toolkit; the original code is credited to Kevin Bealer. In the source
# distribution of NCBI BLAST+, the code can be found at
# c++/src/objtools/blast/seqdb_reader/seqdbfile.cpp.


@dataclass(frozen=True)
class BlastDBMetadata:
    """Metadata about a BLAST database."""
    format_version: int
    db_seqtype: str # protein ("p") or nucleic acid ("n")
    title: str
    date: datetime.datetime
    num_oids: int
    vol_len: int
    max_len: int
    volume: Optional[int] = None
    lmdb_file: Optional[str] = None

class UnsupportedDatabaseFormatException(Exception):
    pass

def read_nin_metadata(nin: str | os.PathLike) -> BlastDBMetadata:
    """Read the metadata from a BLAST database .nin file."""
    metadata = {}
    with open(nin, "rb") as nin_file:
        metadata["format_version"] = int.from_bytes(nin_file.read(4))
        if metadata["format_version"] < 4:
            raise UnsupportedDatabaseFormatException(
                "Cannot read database in format {}".format(
                    metadata["format_version"]
                )
            )
        metadata["db_seqtype"] = "np"[int.from_bytes(nin_file.read(4))]
        if metadata["format_version"] >= 5:
            metadata["volume"] = int.from_bytes(nin_file.read(4))
        title_length = int.from_bytes(nin_file.read(4))
        metadata["title"] = nin_file.read(title_length).decode("ascii")
        if metadata["format_version"] >= 5:
            lmdb_file_length = int.from_bytes(nin_file.read(4))
            metadata["lmdb_file"] = nin_file.read(
                lmdb_file_length
            ).decode("ascii")
        date_length = int.from_bytes(nin_file.read(4))
        metadata["date"] = dateparser.parse(
            nin_file.read(date_length).decode("ascii")
        )
        metadata["num_oids"] = int.from_bytes(nin_file.read(4))
        metadata["vol_len"] = int.from_bytes(nin_file.read(8), "little")
        metadata["max_len"] = int.from_bytes(nin_file.read(4))
    return BlastDBMetadata(**metadata)
