from functools import cached_property

from .blasting import SpecializedBlastnSearch, formatted_blastn_search
from .convert import blast_format_bytes
from .sam import SAMBlastnSearch

class MultiformatBlastnSearch(SpecializedBlastnSearch):
    out_formats = [11]

    @cached_property
    def output(self):
        return self.get_output()

    def to(self, out_format):
        return blast_format_bytes(out_format, self.output)

    def to_search(self, out_format):
        return formatted_blastn_search(out_format)._load_results(
            self.to(out_format),
            subject = self.subject,
            query = self.query,
            out_format = out_format,
            evalue = self.evalue,
            db_cache = self.db_cache,
            threads = self.threads,
            dust = self.dust,
            task = self.task,
            max_targets = self.max_targets,
            n_seqidlist = self.negative_seqidlist,
            perc_ident = self.perc_identity,
            debug = self.debug
        )

    def to_sam(self, decode=True):
        decode_query = {}
        decode_subject = {}
        if decode:
            import pyblast4_archive
            b4 = pyblast4_archive.Blast4Archive.from_bytes(
                self.output,
                "asn_text"
            )
            decode_query = pyblast4_archive.decode_query_ids(b4)
            decode_subject = pyblast4_archive.decode_subject_ids(b4)            
        return SAMBlastnSearch._load_results(
            self.to(17),
            subject = self.subject,
            query = self.query,
            evalue = self.evalue,
            db_cache = self.db_cache,
            threads = self.threads,
            dust = self.dust,
            task = self.task,
            max_targets = self.max_targets,
            n_seqidlist = self.negative_seqidlist,
            perc_ident = self.perc_identity,
            debug = self.debug,
            decode_target = decode_query,
            decode_query = decode_subject
        )

    @classmethod
    def _load_results(cls, res, **kwargs):
        try:
            assert int(kwargs["out_format"]) in cls.out_formats
            del kwargs["out_format"]
        except KeyError:
            pass
        search = cls(**kwargs)
        return search
        

