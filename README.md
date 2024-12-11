# simple_blast

This is a library that provides a very basic wrapper around ncbi-blast+.
Currently, the library supports searches with `blastn` only, but I may expand
the library to include wrappers for other BLAST executables if I need them.

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
against those in `seqs2.fasta`, you could construct a `BlastnSearch` object like
this:

```python
from simple_blast import BlastnSearch

search = BlastnSearch("seqs2.fasta", "seqs1.fasta")
```

The BLAST search is not carried out until you ask for the results by accessing
the `hits` property of the search. This property returns a Pandas dataframe
containing the HSPs identified in the BLAST search.

```python
results = search.hits
```

The columns in the output may be configured by passing either the `out_columns`
or `additional_columns` arguments when constructing the `BlastnSearch`. The
former argument overrides the set of output columns; the latter argument is
added to the list of default output columns.

You can also specify an e-value cutoff through the `evalue` argument.

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

<!-- This is a comment. -->
