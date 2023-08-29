# tests/test_basic.py

from smokedduck import connect

def test_connect():
    sd = connect()
    assert sd is not None

def test_enable_lineage():
    sd = connect()
    sd.execute('pragma enable_lineage')
    sd.execute('pragma disable_lineage')
    assert sd is not None

def test_duckdb_queries_list():
    sd = connect()
    empty = sd.execute('select * from duckdb_queries_list()').df()
    assert len(empty) == 0
