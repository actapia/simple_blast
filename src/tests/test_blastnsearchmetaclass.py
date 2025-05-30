from simple_blast.blasting import (
    TabularBlastnSearch,
    BlastnSearchMetaclass
)
from simple_blast.multiformat import MultiformatBlastnSearch
from simple_blast.sam import SAMBlastnSearch
from .simple_blast_test import (
    SimpleBlastTestCase,
)

class TestBlastnSearchMetaclass(SimpleBlastTestCase):
    def test_registry(self):
        self.assertDictIsSubset(
            {
                6: TabularBlastnSearch,
                7: TabularBlastnSearch,
                11: MultiformatBlastnSearch,
                17: SAMBlastnSearch,
            },
            BlastnSearchMetaclass.registry
        )
