from functools import cached_property

from .blasting import BlastnSearch, formatted_blastn_search
from .convert import blast_format_bytes

class MultiformatBlastnSearch(BlastnSearch):
    out_formats = [11]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, out_format=type(self).out_formats[0], **kwargs)

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

    @classmethod
    def _load_results(cls, res, **kwargs):
        try:
            assert int(kwargs["out_format"]) in cls.out_formats
            del kwargs["out_format"]
        except KeyError:
            pass
        search = cls(**kwargs)
        return search
        

