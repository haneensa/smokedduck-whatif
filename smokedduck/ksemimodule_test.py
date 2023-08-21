import pandas as pd
import smokedduck
import sql_statements

p1 = pd.DataFrame({'a': [42, 43, 44, 45], 'b': ['a', 'b', 'a', 'b']})
p2 = pd.DataFrame({'b': ['a', 'a', 'c', 'b'], 'c': [4, 5, 6, 7]})
con = smokedduck.connect(':default:')
con.execute('create table t1 as (select * from p1)')
con.execute('create table t2 as (select * from p2)')

df = con.execute('select t1.b, sum(a + c) from t1 join t2 on t1.b = t2.b group by t1.b', capture_lineage='ksemimodule')
print(df)
print(con.ksemimodule().df())

print(con.backward([0], 'ksemimodule').df())
print(con.forward('t1', [0], 'ksemimodule').df())
