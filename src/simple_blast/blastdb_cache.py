import subprocess
import tempfile
from pathlib import Path

class BlastDBCache:
    def __init__(self, location):
        self.location = location
        self._cache = {}

    def makedb(self, seq_file_path):
        seq_file_path = Path(seq_file_path)
        seq_name = seq_file_path.stem
        tempdir = Path(
            tempfile.mkdtemp(
                prefix=seq_name,
                dir=self.location
            )
        )
        db_name = str(tempdir / "db")
        proc = subprocess.Popen(
            [
                "makeblastdb",
                "-in",
                str(seq_file_path),
                "-out",
                db_name,
                "-dbtype",
                "nucl",
                "-hash_index" # Do I need this?
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        proc.communicate()
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, proc.args)
        self._cache[seq_file_path] = db_name

    def __getitem__(self, k):
        return self._cache[Path(k)]

    def __delitem__(self, k):
        del self._cache[Path(k)]

    def __contains__(self, k):
        return Path(k) in self._cache
        
        
