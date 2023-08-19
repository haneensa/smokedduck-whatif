import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')

# Loading example data
p1 = pd.DataFrame({'a': [42, 43, 44, 45], 'b': ['a', 'b', 'a', 'b']})
p2 = pd.DataFrame({'b': ['a', 'a', 'c', 'b'], 'c': [4, 5, 6, 7]})
con.execute('create table t1 as (select * from p1)')
con.execute('create table t2 as (select * from p2)')

# Executing base query
con.execute('SELECT t1.b, sum(a + c) FROM t1 join (select b, avg(c) as c from t2 group by b) as t2 on t1.b = t2.b group by t1.b', capture_lineage='lineage').df()

# Printing lineage that was captured from base query
print(con.lineage().df())