import subprocess
import pandas as pd
from typing import List, Optional

from .blastdb_cache import BlastDBCache

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

class BlastnSearch:
    """A search (alignment) to be made with blastn.

    This class provides a programmer-friendly way to define the parameters of a
    simple blastn search, carry out the search, and parse the results.

    The most useful property of a BlastnSearch instance is hits. hits runs the
    defined blastn search (if it hasn't been run already), parses the results,
    stores them in a pandas dataframe, and returns the result.

    This class has no public attributes; the settings defined in the
    constructor should be consided immutable. Nevertheless, the values passed to
    the constructor may be retrieved through the class's properties.
    """
    def __init__(
            self,
            seq1_path: str,
            seq2_path: str,
            evalue: float = 1e-20,
            out_columns: List[str] = default_out_columns,
            additional_columns: List[str] = [],
            db_cache: Optional[BlastDBCache] = None,
            threads: int = 1,
            dust: bool = True,
            task: Optional[str] = None,
            max_targets: int = 500,
            n_seqidlist: Optional[str] = None,
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
            seq1_path (str):    Path to query sequence FASTA file.
            seq2_path (str):    Path to subject sequence FASTA file.
            evalue (float):     Expect value cutoff to use in BLAST search.
            out_columns:        Output columns to include in results.
            additional_columns: Additional output columns to include in results.
            db_cache:           BlastDBCache that tells where to find BLAST DBs.
            threads (int):      Number of threads to use for BLAST search.
            dust (bool):        Filter low-complexity regions from search.
            task (str):         Parameter preset to use.
            max_targets (int):  Maximum number of target seqs to include.
            n_seqidlist (str):  Specifies seqids to ignore.
        """
        self._seq1_path = seq1_path
        self._seq2_path = seq2_path
        self._evalue = evalue
        self._hits = None
        self._out_columns = list(out_columns + additional_columns)
        self._db_cache = db_cache
        self._threads =  threads
        self._dust = dust
        self._task = task
        self._max_targets = max_targets
        self._negative_seqidlist = n_seqidlist
        # If you really need to add extra arguments, you can do it by setting
        # the _extra_args attribute.
        self._extra_args = []
        self.debug = debug

    @property
    def seq1_path(self) -> str:
        """Return the query sequence path."""
        return self._seq1_path

    @property
    def seq2_path(self) -> str:
        """Return the subject sequence path."""
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


    # def __len__(self) -> int:
    #     return len(self.hits)

    # def __iter__(self):
    #     yield from self.hits

    def _build_blast_command(self):
        command = ["blastn"]
        if self._db_cache and self.seq1_path in self._db_cache:
            command = command + ["-db", str(self._db_cache[self.seq1_path])]
        else:
            command = command + ["-subject", self.seq1_path]
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
            " ".join(["6"] + self._out_columns),
            "-num_threads",
            str(self._threads),
            "-dust",
            yes_no[self._dust],
            "-max_target_seqs",
            str(self._max_targets)
        ] + self._extra_args
        #print(" ".join(command), file=sys.stderr)
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
            delim_whitespace=True
        )
        # from IPython import embed
        # embed()
        proc.communicate()
        if proc.returncode:
            if self.debug:
                from IPython import embed
                embed()
            raise subprocess.CalledProcessError(proc.returncode, proc.args)
