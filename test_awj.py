import tempfile
import shutil
import pandas as pd
from pandas.util.testing import assert_frame_equal
from contextlib import contextmanager
from glob import glob
import os.path

from awj import AWJ


@contextmanager
def awj_context(max_size=None):
    work_dir = tempfile.mkdtemp()
    awj = AWJ(work_dir, max_size=max_size)
    try:
        yield awj
    finally:
        shutil.rmtree(work_dir)


def test_basic():
    with awj_context() as awj:
        df = pd.DataFrame({'a': range(15)})
        awj['a'] = df
        df2 = awj['a']
        assert_frame_equal(df2, df)


def test_contains():
    with awj_context() as awj:
        df = pd.DataFrame({'a': range(15)})
        awj['a'] = df
        assert 'a' in awj
        assert 'b' not in awj
        assert list(awj) == ['a']

        dfb = pd.DataFrame({'b': range(15)})
        awj['b'] = dfb
        assert 'a' in awj
        assert 'b' in awj
        assert set(awj) == {'a', 'b'}

        assert_frame_equal(df, awj['a'])
        assert_frame_equal(dfb, awj['b'])


def test_del():
    with awj_context() as awj:
        df = pd.DataFrame({'a': range(15)})
        awj['a'] = df
        dfb = pd.DataFrame({'b': range(15)})
        awj['b'] = dfb

        assert 'a' in awj
        assert 'b' in awj

        del awj['a']
        assert 'a' not in awj
        assert 'b' in awj

        assert_frame_equal(dfb, awj['b'])

        del awj['b']
        assert 'b' not in awj
        assert list(awj) == []


def test_find_files():
    with awj_context() as awj:
        df = pd.DataFrame({'a': range(15)})
        awj['a'] = df
        dfb = pd.DataFrame({'b': range(15)})
        awj['b'] = dfb
        cache_path = awj.cache_path
        del awj

        awj2 = AWJ(cache_path)
        assert 'a' in awj2
        assert 'b' in awj2
        assert_frame_equal(df, awj2['a'])
        assert_frame_equal(dfb, awj2['b'])


def test_overwrite():
    with awj_context() as awj:
        df = pd.DataFrame({'a': range(15)})
        awj['a'] = df
        dfb = pd.DataFrame({'b': range(15)})
        awj['a'] = dfb
        assert_frame_equal(dfb, awj['a'])


def test_pruning():
    max_size = .5
    max_size_bytes = max_size * 1048576
    with awj_context(.5) as awj:
        max_len = len(awj)
        for j in range(100):
            k = 'a{}'.format(j)
            df = pd.DataFrame({'k': range(2000)})
            awj[k] = df
            files = glob(os.path.join(awj.cache_path, '*feather'))
            total_size = sum(os.stat(fn).st_size
                             for fn in files)
            assert total_size < max_size_bytes
            a_len = len(awj)
            assert a_len >= max_len
            if a_len > max_len:
                max_len = a_len
