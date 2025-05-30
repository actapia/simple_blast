from simple_blast.blasting import (
    formatted_blastn_search,
    TabularBlastnSearch,
    BlastnSearch
)
from simple_blast.multiformat import MultiformatBlastnSearch
from simple_blast.sam import SAMBlastnSearch
from .simple_blast_test import (
    SimpleBlastTestCase,
)

class TestAutoFormat(SimpleBlastTestCase):
    def test_formatted_blastn_search(self):
        self.assertEqual(formatted_blastn_search(6), TabularBlastnSearch)
        self.assertEqual(formatted_blastn_search(7), TabularBlastnSearch)
        self.assertEqual(formatted_blastn_search(11), MultiformatBlastnSearch)
        self.assertEqual(formatted_blastn_search(1), BlastnSearch)
        self.assertEqual(formatted_blastn_search(17), SAMBlastnSearch)
