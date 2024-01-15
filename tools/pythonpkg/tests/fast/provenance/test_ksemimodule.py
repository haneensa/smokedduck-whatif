import pandas as pd
import smokedduck

class TestKSemimodule(object):
    def test_ksemimodule(self):
        p1 = pd.DataFrame({'a': [42, 43, 44, 45], 'b': ['a', 'b', 'a', 'b']})
        p2 = pd.DataFrame({'b': ['a', 'a', 'c', 'b'], 'c': [4, 5, 6, 7]})
        con = smokedduck.prov_connect(':default:')
        con.execute('create table ksemi1 as (select * from p1)')
        con.execute('create table ksemi2 as (select * from p2)')

        print(con.execute('select ksemi1.b, sum(ksemi1.a + ksemi2.c) from ksemi1 join ksemi2 on ksemi1.b = ksemi2.b group by ksemi1.b', capture_lineage='ksemimodule'))
        print(con.ksemimodule().df())

        print(con.backward([0], 'ksemimodule').df())
        print(con.forward('ksemi1', [0], 'ksemimodule').df())
