import unittest
import tempfile
import os
import shutil
import importlib.resources

from pathlib import Path

package_file_root = importlib.resources.files(__package__)
data_dir = package_file_root / "data"

class SimpleBlastTestCase(unittest.TestCase):#(DictSubsetTestCase, PandasTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        os.chdir(self.temp_dir.name)
        self.data_dir = Path("data")
        self.data_dir.mkdir()
        for f in data_dir.glob("*.fasta"):
            shutil.copy(f, self.data_dir.name)

    def tearDown(self):
        os.chdir("/")
        self.temp_dir.cleanup()
        
        
    def assertColumnsEqual(self, a, b):
        return self.assertEqual(list(a.items()), list(b.items()))

    def assertDictIsSubset(self, a, b):
        for k, v in a.items():
            self.assertIn(k, b)
            self.assertEqual(v, b[k])

    def assertFileExists(self, p):
        p = Path(p)
        if not p.exists():
            raise AssertionError("File {} does not exist.".format(repr(str(p))))

    def assertIsDirectory(self, p):
        p = Path(p)
        if not p.is_dir():
            raise AssertionError("{} is not a directory.".format(repr(str(p))))

    def assertHasAttr(self, x, attr):
        if not hasattr(x, attr):
            raise AssertionError(
                "{} does not have attribute {}.".format(repr(x), attr)
            )

def parse_blast_command(args):
    positional = []
    named = {}
    key = None
    for x in args:
        flag = x.startswith("-")
        if flag:
            if key is not None:
                named[key] = None
            key = x[1:]
        elif key is not None:
            named[key] = x
            key = None
        else:
            positional.append(x)
    if key is not None:
        named[key] = None
    return (positional, named)
