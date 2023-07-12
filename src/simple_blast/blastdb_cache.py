import subprocess
import tempfile
from pathlib import Path

def read_nal_title(nal):
    with open(nal, "r") as nal_file:
        for l in nal_file:
            if not l.startswith("#"):
                split = l.rstrip().split(" ")
                if split[0] == "TITLE":
                    return " ".join(split[1:])
            

class BlastDBCache:
    def __init__(self, location, find_existing=True):
        self.location = location
        self._cache = {}
        if find_existing:
            for path in Path(location).glob("*/*.nal"):
                self._cache[read_nal_title(path)] = path.parent                

    def makedb(self, seq_file_path):
        if seq_file_pth in self._cache:
            return
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
        
        
