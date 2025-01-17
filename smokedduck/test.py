import smokedduck
import pandas as pd

p1 = pd.DataFrame({'a': [42, 43, 44, 45], 'b': ['a', 'b', 'a', 'b']})
p2 = pd.DataFrame({'b': ['a', 'a', 'c', 'b'], 'c': [4, 5, 6, 7]})
con = smokedduck.connect(':default:')
con.execute('create table t1 as (select * from p1)')
con.execute('create table t2 as (select * from p2)')
df = con.execute('SELECT t1.b, sum(a + c) FROM t1 join (select b, avg(c) as c from t2 group by b) as t2 on t1.b = t2.b group by t1.b', capture_lineage='lineage').df()
print(df)
print(con.lineage().df())
print(con.why().df())
print(con.polynomial().df())
print(con.backward([0, 2]).df())
print(con.backward([1, 3], 'polynomial').df())
print(con.forward('t1', [0, 1]).df())
print(con.forward('t2', [2, 3]).df())

all_rows = con.execute('SELECT t1.b, sum(a + c) FROM t1 join t2 on t1.b = t2.b group by t1.b', capture_lineage='lineage').fetchall()
print(all_rows)
print(con.lineage().df())
print(con.why().df())
print(con.polynomial().df())
print(con.backward([0, 2]).df())
print(con.backward([1, 3], 'polynomial').df())
print(con.forward('t1', [0, 1]).df())
print(con.forward('t2', [2, 3]).df())

np_rows = con.execute('SELECT t1.a, t1.b, t2.c FROM t1 join t2 on t1.b = t2.b', capture_lineage='lineage').fetchnumpy()
print(np_rows)
print(con.lineage().df())
print(con.why().df())
print(con.polynomial().df())
print(con.backward([0, 2]).df())
print(con.backward([1, 3], 'polynomial').df())
print(con.forward('t1', [0, 1]).df())
print(con.forward('t2', [2, 3]).df())

con.duckdb_conn.close()
