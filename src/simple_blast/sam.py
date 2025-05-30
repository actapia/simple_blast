import io
from .blasting import SpecializedBlastnSearch, ParsedSearch

class SAMBlastnSearch(ParsedSearch, SpecializedBlastnSearch):
    out_formats = [17]

    def __init__(self, *args, decode_query=None, decode_target=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._decode_query = decode_query
        self._decode_target = decode_target

    @classmethod
    def parse_hits(cls, hits, decode_query=None, decode_target=None):        
        if decode_query is None:
            decode_query = {}
        if decode_target is None:
            decode_target = {}
        return RenamedSamAlignmentIterator(
            io.TextIOWrapper(hits),
            decode_query,
            decode_target
        )

    def _parse_hits(self, hits):
        return self.parse_hits(hits, self._decode_query, self._decode_target)

try:
    import Bio.Align.sam
    class RenamedSamAlignmentIterator(Bio.Align.sam.AlignmentIterator):
        def __init__(self, source, rename_query, rename_target):
            super().__init__(source)
            self._rename_query = rename_query
            self._rename_target = rename_target
            for t in self.targets:
                t.id = self._rename_target.get(t.id, t.id)

        def __next__(self):
            al = super().__next__()
            al.target.id = self._rename_target.get(al.target.id, al.target.id)
            al.query.id = self._rename_query.get(al.query.id, al.query.id)
            return al    
except ImportError:
    pass
