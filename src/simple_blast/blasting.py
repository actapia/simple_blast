import subprocess
import pandas as pd
from collections.abc import Iterable
from typing import List, Optional
from pathlib import Path
from contextlib import contextmanager, ExitStack

from .blastdb_cache import BlastDBCache, to_path_iterable
from .seqs import SeqsAsFile, SeqType

default_out_columns = ['qseqid',
 'sseqid',
 'pident',
 'length',
 'mismatch',
 'gapopen',
 'qstart',
 'qend',
 'sstart',
 'send',
 'evalue',
 'bitscore']

yes_no = ["no", "yes"]

class NotInDatabaseError(Exception):
    pass

def try_element_to_SeqType_list(x):
    if isinstance(x, SeqType):
        return [x]
    else:
        return x

class BlastnSearch:
    """A search (alignment) to be made with blastn.

    This class provides a programmer-friendly way to define the parameters of a
    simple blastn search, carry out the search, and parse the results.

    The most useful property of a BlastnSearch instance is hits. hits runs the
    defined blastn search (if it hasn't been run already), parses the results,
    stores them in a pandas dataframe, and returns the result.

    Values passed to the constructor may be retrieved through the class's
    properties.

    Attributes:
        debug (bool): Whether to enable debug features for this instance.
    """
    def __init__(
            self,
            subject: str | Path | Iterable[str] | Iterable[Path],
            query: str | Path,
            evalue: float = 1e-20,
            out_columns: List[str] = default_out_columns,
            additional_columns: List[str] = [],
            db_cache: Optional[BlastDBCache] = None,
            threads: int = 1,
            dust: bool = True,
            task: Optional[str] = None,
            max_targets: int = 500,
            n_seqidlist: Optional[str] = None,
            perc_ident: int = 0,
            debug: bool = False
    ):
        """Construct a BlastnSearch with the specified settings.

        This constructor requires paths to FASTA files containing the query and
        subject sequences to use in the search.

        Optionally, the caller may provide an expect value cutoff to use for the
        search. If no value is provided, a default evalue of 1e-20 will be used.

        The caller may specify what columns should be included in the output.
        By default, the included columns are

            sseqid
            pident
            length
            mismatcch
            gapopen
            qstart
            qend
            sstart
            send
            evalue
            bitscore

        Explanations of these columns may be found at
        https://www.metagenomics.wiki/tools/blast/blastn-output-format-6

        If the caller desires to include additional columns, it may provide
        them to the additional_columns parameter.

        Parameters:
            subject:            Path(s) to subject sequence FASTA file(s).
            query:              Path to query sequence FASTA file.
            evalue (float):     Expect value cutoff to use in BLAST search.
            out_columns:        Output columns to include in results.
            additional_columns: Additional output columns to include in results.
            db_cache:           BlastDBCache that tells where to find BLAST DBs.
            threads (int):      Number of threads to use for BLAST search.
            dust (bool):        Filter low-complexity regions from search.
            task (str):         Parameter preset to use.
            max_targets (int):  Maximum number of target seqs to include.
            n_seqidlist (str):  Specifies seqids to ignore.
            perc_ident (int):   Percent identity cutoff.
            debug (bool):       Whether to enable debug features.
        """
        subject = to_path_iterable(subject, tuple)
        query = Path(query)
        self._seq1_path = subject
        self._seq2_path = query
        self._evalue = evalue
        self._hits = None
        self._out_columns = tuple(out_columns + additional_columns)
        self._db_cache = db_cache
        self._threads =  threads
        self._dust = dust
        self._task = task
        self._max_targets = max_targets
        self._negative_seqidlist = n_seqidlist
        self._perc_identity = perc_ident
        # If you really need to add extra arguments, you can do it by setting
        # the _extra_args attribute.
        self._extra_args = []
        self.debug = debug

    @property
    def query(self) -> Path:
        """Return the query sequence path."""
        return self._seq2_path

    @property
    def subject(self) -> Iterable[Path]:
        """Return the subject sequence paths."""
        return self._seq1_path

    @property
    def seq1_path(self) -> str:
        """Return the subject sequence paths."""
        return self._seq1_path

    @property
    def seq2_path(self) -> str:
        """Return the query sequence path."""
        return self._seq2_path

    @property
    def evalue(self) -> float:
        """Return the expect value used as a cutoff in the blastn search."""
        return self._evalue

    @property
    def db_cache(self) -> Optional[BlastDBCache]:
        """Return a cache of BLAST DBs to be used in the search."""
        return self._db_cache

    @property
    def hits(self) -> pd.DataFrame:
        """Return a dataframe containing this search's BLAST results."""
        if self._hits is None:
            self._get_hits()
        return self._hits

    @property
    def threads(self) -> int:
        """Return the number of threads to use for the search."""
        return self._threads

    @property
    def dust(self) -> bool:
        """Return whether to filter low-complexity regions."""
        return self._dust

    @property
    def task(self) -> Optional[str]:
        """Return the name of the parameter preset to use."""
        return self._task

    @property
    def max_targets(self) -> int:
        """Return the maximum number of target sequences."""
        return self._max_targets

    @property
    def negative_seqidlist(self) -> Optional[str]:
        """Return a path to a list of sequence IDs to ignore."""
        return self._negative_seqidlist

    @property
    def out_columns(self) -> tuple[str]:
        """Return the list of columns to include in the output."""
        return self._out_columns

    @property
    def perc_identity(self) -> int:
        """Return the percent identity cutoff to use."""
        return self._perc_identity

    def _build_blast_command(self):
        command = ["blastn"]
        if self._db_cache and self.seq1_path in self._db_cache:
            command = command + ["-db", str(self._db_cache[self.seq1_path])]
        elif len(self.seq1_path) > 1:
            raise NotInDatabaseError("Must use DB cache for multiple subjects.")
        else:
            command = command + ["-subject", str(self.seq1_path[0])]
        if self._task is not None:
            command = command + ["-task", self._task]
        if self._negative_seqidlist is not None:
            command = command + [
                "-negative_seqidlist",
                self._negative_seqidlist
            ]
        command = command + [
            "-query",
            str(self.seq2_path),
            "-evalue",
            str(self.evalue),
            "-outfmt",
            " ".join(["6"] + list(self._out_columns)),
            "-num_threads",
            str(self._threads),
            "-dust",
            yes_no[self._dust],
            "-max_target_seqs",
            str(self._max_targets),
            "-perc_identity",
            str(self._perc_identity)
        ] + self._extra_args
        return command
            

    def _get_hits(self):
        proc = subprocess.Popen(
            self._build_blast_command(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        self._hits = pd.read_csv(
            proc.stdout,
            names=self._out_columns,
            sep=r"\s+"
        )
        proc.communicate()
        if proc.returncode:
            if self.debug:
                from IPython import embed
                embed()
            raise subprocess.CalledProcessError(proc.returncode, proc.args)

    @classmethod
    @contextmanager
    def from_sequences(
            cls,
            subject_seqs: Optional[Iterable[SeqType] | SeqType] = None,
            query_seqs: Optional[Iterable[SeqType] | SeqType] = None,
            **kwargs
    ):
        """Return a context for a BlastnSearch for the given sequences.

        Since temporary files must be created to hold the sequences, this
        function returns a context manager that automatically creates and later
        deletes the temporary files.

        Parameters:
            subject_seqs: Sequences to use as subjects in the search.
            query_seqs:   Sequences to use as queries in the search.

        Returns:
            A context manager that gives a BlastnSearch for the sequences.
        """
        seqs = {"subject": subject_seqs, "query": query_seqs}
        seq_keys = list(seqs)
        for k in seq_keys:            
            seqs[k] = try_element_to_SeqType_list(seqs[k])
        try:
            with ExitStack() as stack:
                for k, seq in seqs.items():
                    if seq is not None:
                        seq_file = SeqsAsFile(seq)
                        stack.enter_context(seq_file)
                        kwargs[k] = seq_file.name
                yield cls(**kwargs)
        finally:
            pass

def blastn_from_files(*args, **kwargs) -> pd.DataFrame:
    """Return the blastn results for the provided sequence files."""
    return BlastnSearch(*args, **kwargs).hits

def blastn_from_sequences(*args, **kwargs) -> pd.DataFrame:
    """Return the blastn results for the provided sequences."""
    with BlastnSearch.from_sequences(*args, **kwargs) as search:
        return search.hits
