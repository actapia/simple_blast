# simple_blast

This is a library that provides a (decreasingly) basic wrapper around
ncbi-blast+.  Currently, the library supports searches with `blastn` only, but I
may expand the library to include wrappers for other BLAST executables if I need
them.

## Requirements

This library depends on Pandas for parsing BLAST output. The library has been
tested with Pandas 1.5.3, but it likely works with other versions.

Of course, this library assumes that ncbi-blast+ is installed. The library has
been tested with ncbi-blast 2.12.0+, and it likely works with newer versions of
the software as well.

## Basic usage

You can define a `blastn` search to be carried out using the `BlastnSearch`
class. `BlastnSearch`objects are constructed with two required
arguments&mdash;the subject sequence and the query sequence files, in that
order. For example, to set up a `balstn` search for sequences in `seqs1.fasta`
against those in `seqs2.fasta` using output format 11 (BLAST Archive ASN.1), you
could construct a `BlastnSearch` object like this:

```python
from simple_blast import BlastnSearch

search = BlastnSearch("seqs2.fasta", "seqs1.fasta", 11)
```

The BLAST search is not carried out until you ask for the results by running the
`get_output()` function.

```python
results = search.get_output()
```

`blastn` can output binary data, so the `get_output()` function appropriately
returns `bytes`.

Often, it's convenient to use output format 6, a tabular representation of the
HSPs. For that purpose, you can use `TabularBlastnSearch`.

```python
from simple_blast import TabularBlastnSearch

search = TabularBlastnSearch("seqs2.fasta", "seqs1.fasta", 11)
```

The `hits` property of the search returns a Pandas dataframe containing the HSPs
identified in the BLAST search.

```python
results = search.hits
```

The columns in the output may be configured by passing either the `out_columns`
or `additional_columns` arguments when constructing the
`TabularBlastnSearch`. The former argument overrides the set of output columns;
the latter argument is added to the list of default output columns.

### Sequences from memory

`simple_blast` can handle BLAST searches with sequences stored in memory (i.e.,
not in a file). It works with sequences stored as strings or in
[BioPython](https://biopython.org/) `SeqRecord` objects.

```python
from Bio.SeqRecord import SeqRecord, Seq
from simple_blast import TabularBlastnSearch

# Define some data.
subjects = [
    SeqRecord(
	    Seq(
		"AAGGCGTACGGGCCTTTCGCTTCCGAAAACTTCCTCTTAGGTCGCTGTTACTGGATGTCGAGTCAGCACA"
		"TGGGAAACTCCACGCATCGGCGGGATTTCACAACGCCTAGAACACCGGTAATGCGAGTATCCGTATCGGT"
		"AACAAATATCTTTGGGATACTACAGGAATATCCGTAGGAGTTCGCCGCGATTAGGTGCCTCGATGATATG"
		"CAGCTGTCACTGGAGATAACACACTATGCAGCAGTAATGGATGTTATTGCTACTAAGGTTCCCTGTCACC"
	    ),
		id="My Sequence 1"
	),
    SeqRecord(
	    Seq(
		"TTCATTGGTGGGCTTTCTGGTTCACGCCCATCTCAATGTACATTTTCCGTGACGTGATGATAATCATAAC"
		"TCGTTGGTAGTAATAGGGTAAGGGAATTTGGCAGGTAGTCGGGGCAAGACTGCCGTTACAAGCTAATCAT"
		"CTGCCAACTAACTTTAGCCGTAATTGGCACTAACAGTTAACCTTCGCGCGTTTCTCAGTGTAGAGTGAGA"
		"CTATGTGATTACTTTCAGCGCCCAGCGGTGGTAGGTAGTAAAAAGTGGCCACCGAACCGAATGCT"
	    ),
		id="My Sequence 2"
	)
]
queries = [
    SeqRecord(
        Seq("TGGGAAACTCCACGCATCGGCGGGATTTCACAACGCCTAGAACACCGGTAATGCGAGTATCCGT"),
        id="Query 1"
    )
]

with TabularBlastnSearch.from_sequences(subjects, queries) as search:
    results = search.hits
```

or, using a list of strings:

```python
from simple_blast import TabularBlastnSearch

# Define some data.
subjects = [
    (
        "AAGGCGTACGGGCCTTTCGCTTCCGAAAACTTCCTCTTAGGTCGCTGTTACTGGATGTCGAGTCAGCACA"
        "TGGGAAACTCCACGCATCGGCGGGATTTCACAACGCCTAGAACACCGGTAATGCGAGTATCCGTATCGGT"
        "AACAAATATCTTTGGGATACTACAGGAATATCCGTAGGAGTTCGCCGCGATTAGGTGCCTCGATGATATG"
        "CAGCTGTCACTGGAGATAACACACTATGCAGCAGTAATGGATGTTATTGCTACTAAGGTTCCCTGTCACC"
    ),
    (
        "TTCATTGGTGGGCTTTCTGGTTCACGCCCATCTCAATGTACATTTTCCGTGACGTGATGATAATCATAAC"
        "TCGTTGGTAGTAATAGGGTAAGGGAATTTGGCAGGTAGTCGGGGCAAGACTGCCGTTACAAGCTAATCAT"
        "CTGCCAACTAACTTTAGCCGTAATTGGCACTAACAGTTAACCTTCGCGCGTTTCTCAGTGTAGAGTGAGA"
        "CTATGTGATTACTTTCAGCGCCCAGCGGTGGTAGGTAGTAAAAAGTGGCCACCGAACCGAATGCT"
    )
]
queries = ["TGGGAAACTCCACGCATCGGCGGGATTTCACAACGCCTAGAACACCGGTAATGCGAGTATCCGT"]

with TabularBlastnSearch.from_sequences(subjects, queries) as search:
    results = search.hits
```

When using a list of strings, sequences are automatically named `seq_i`, where
i is the position of the sequence in the list.

You can use `SeqRecords` together with lists of strings, and you can also use
in-memory sequences together with files by providing the `subject` or `query`
keyword arguments to `from_sequences`.

```python
BlastnSearch.from_sequences(["CATGAACTA"], query="seqs1.fasta")
```

Since using a context manager is slightly cumbersome, you can also use the
`blastn_from_sequences` convenience function to get the hits for a search.

```python
from simple_blast import blastn_from_sequences

# Define some data.
subjects = [
    (
        "AAGGCGTACGGGCCTTTCGCTTCCGAAAACTTCCTCTTAGGTCGCTGTTACTGGATGTCGAGTCAGCACA"
        "TGGGAAACTCCACGCATCGGCGGGATTTCACAACGCCTAGAACACCGGTAATGCGAGTATCCGTATCGGT"
        "AACAAATATCTTTGGGATACTACAGGAATATCCGTAGGAGTTCGCCGCGATTAGGTGCCTCGATGATATG"
        "CAGCTGTCACTGGAGATAACACACTATGCAGCAGTAATGGATGTTATTGCTACTAAGGTTCCCTGTCACC"
    ),
    (
        "TTCATTGGTGGGCTTTCTGGTTCACGCCCATCTCAATGTACATTTTCCGTGACGTGATGATAATCATAAC"
        "TCGTTGGTAGTAATAGGGTAAGGGAATTTGGCAGGTAGTCGGGGCAAGACTGCCGTTACAAGCTAATCAT"
        "CTGCCAACTAACTTTAGCCGTAATTGGCACTAACAGTTAACCTTCGCGCGTTTCTCAGTGTAGAGTGAGA"
        "CTATGTGATTACTTTCAGCGCCCAGCGGTGGTAGGTAGTAAAAAGTGGCCACCGAACCGAATGCT"
    )
]
queries = ["TGGGAAACTCCACGCATCGGCGGGATTTCACAACGCCTAGAACACCGGTAATGCGAGTATCCGT"]

results = blastn_from_sequences(subjects, queries)
```

**Note:** Searching from in-memory sequences is implemented using Unix FIFOs, so
this feature currently will not work on Windows.

## DB caches

When the same sequence file is used as a subject in multiple searches, it can be
efficient to build a BLAST database up front. The `BlastDBCache` class can be
used to handle this mostly automatically. To make a `BlastDBCache`, you need
to specify the location of the on the file system.

```python
from simple_blast import BlastDBCache

cache = BlastDBCache("cache_dir")
```

To add a file to the cache, use the `makedb` method.

```python
cache.makedb("seqs2.fasta")
```

When constructing a `BlastnSearch` object, give it the `BlastDBCache` as the
`db_cache` parameter to make the `BlastnSearch` object use the cache for
searches.

```python
search = BlastnSearch("seqs2.fasta", "seqs1.fasta", db_cache=cache)
```

Now `search` will use the database we created for `seqs2.fasta`.

## Format conversions

It's sometimes useful to convert between different BLAST output
formats. ncbi-blast+ comes with a utility, `blast_formatter`, that can convert
output in the "Blast4 Archive" format (ASN.1, output format 11) to any other
BLAST format.

### Using `blast_formatter` with `simple_blast.convert`

You can use `blast_formatter` directly with the `simple_blast.convert`
module. For example,

```python
from simple_blast.convert import blast_format_file

# Convert to output format 11.
blast_format_file(12, "my_blast_results.asn1", "my_blast_results.json")
```

If you don't specify the output file, you can get the output as bytes.

```python
seqalign_bytes = blast_format_file(12, "my_blast_results.asn1")
```

You can also use the similar `blast_format_bytes` to provide bytes as input.

### Using `MultiformatBlastnSearch`

You can create a search with output format 11 using the
`MultiformatBlastnSearch` class.

```python
from simple_blast.multiformat import MultiformatBlastnSearch

search = MultiformatBlastnSearch("seqs2.fasta", "seqs1.fasta")
```

You can convert the output to another format using the `to` method.

```python
seqalign_bytes = search.to(12)
```

For output formats with an associated subclass of `BlastnSearch`, you can also
convert directly to that subclass with `to_search`..

```python
tabular_search = search.to_search(6)
results = tabular_search.hits # A Pandas DataFrame
```
