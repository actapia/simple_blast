import io
from typing import Optional
from .blasting import SpecializedBlastnSearch, ParsedSearch

try:
    import Bio.Align
    import Bio.Align.sam
    class RenamedSamAlignmentIterator(Bio.Align.sam.AlignmentIterator):
        """SAM AlignmentIterator that renames subjects and queries on-the-fly.

        This class functions much like the Bio.Align.sam.AlignmentIterator
        objects that are produced when using Bio.Align.parse(file, "sam"), but
        this class will also rename query and/or subject sequences as lines are
        read from the SAM file. This is useful, for example, for correcting
        query or sequence IDs in the SAM output produced by ncbi-blast.
        """
        def __init__(
                self,
                source: str | io.TextIOBase,
                rename_query: dict[str, str],
                rename_target: dict[str, str]
        ):
            """Construct a RenamedSamAlignmentIterator with given rename dicts.

            If a query or target name is present in the SAM file but does not
            exist in the corresponding rename dict, the original name will be
            retained.

            Parameters:
                source:               The SAM file-like object to parse.
                rename_query (dict):  New names to assign queries.
                rename_target (dict): New names to assign targets.
            """
            super().__init__(source)
            self._rename_query = rename_query
            self._rename_target = rename_target
            for t in self.targets:
                t.id = self._rename_target.get(t.id, t.id)

        def __next__(self) -> Bio.Align.Alignment:
            al = super().__next__()
            al.target.id = self._rename_target.get(al.target.id, al.target.id)
            al.query.id = self._rename_query.get(al.query.id, al.query.id)
            return al    
except ImportError:
    pass

class SAMBlastnSearch(ParsedSearch, SpecializedBlastnSearch):
    """A BlastnSearch with Sequence Alignment Map (SAM) output.

    If Biopython is installed, the hits property can be used to get parsed SAM
    output.

    Note that, under some circumstances, ncbi-blast+ uses internal IDs for
    subject and query sequences instead of their original IDs. To ensure the
    original IDs are preserved, use MultiformatBlastnSearch's to_sam method
    instead of directly constructing this class.
    """
    out_formats = [17]

    def __init__(self,
                 *args,
                 decode_query: Optional[dict[str, str]] = None,
                 decode_target: Optional[dict[str, str]] = None,
                 **kwargs
    ):
        """Construct a SAMBlastnSearch with the given parameters.

        The parameters decode_query and decode_target can be used to
        automatically rename query and target sequences in the parsed SAM
        output. By default, no sequences are renamed. If a dict is provided to
        decode sequence names, and a sequence name is not present in the dict,
        the original sequence name will be retained.

        Parameters:
            decode_query (dict):  Renames queries in the parsed output.
            decode_target (dict): Renames targets in the parsed output.
        """
        super().__init__(*args, **kwargs)
        self._decode_query = decode_query
        self._decode_target = decode_target

    @classmethod
    def parse_hits(
            cls,
            hits: io.BufferedIOBase,
            decode_query: Optional[dict[str, str]] = None,
            decode_target: Optional[dict[str, str]] = None,
    ):
        """Parse the given BLAST output as a SAM file.

        Optionally, decode_query or decode_target may be provided to rename
        query or target sequences in the parsed output. By default, no sequences
        are renamed. If a dict is provided to decode sequence names, and a
        sequence name is not present in the dict, the original sequence name
        will be retained.

        Parameters:
            hits:                 File-like object representing BLAST output.
            decode_query (dict):  Renames queries in the parsed output.
            decode_target (dict): Renames targets in the parsed output.

        Returns:
            The BLAST output, parsed as SAM.
        """
        if decode_query is None:
            decode_query = {}
        if decode_target is None:
            decode_target = {}
        # noinspection PyTypeChecker
        return RenamedSamAlignmentIterator(
            io.TextIOWrapper(hits),
            decode_query,
            decode_target
        )

    def _parse_hits(self, hits):
        return self.parse_hits(hits, self._decode_query, self._decode_target)
