# Awkward Winter Jacket

dict-like LRU for ``DataFrames`` backed by on-disk feather format
files


    >>> from awj import AWJ
	>>> awj = AWJ('/tmp/df_cache', max_size=500)
	>>> awj['a'] = dfA
	>>> awj['b'] = dfB
	>>> list(awj)
	['a', 'b']
	>>> del awj['a']
	>>> 'b' in awj
	True
	>>> 'a' in awj
	False

If the on-disk size of the cache gets above ``max_size``MB the least
recently used files are deleted until the cache is under size.  The
first insert before pruning will save to disk before the pruning is
done, the ``max_size`` is more of a suggestion than a guarantee.

The full `dict` interface has is not implemented (yet).

There are feather poking out everywhere.
