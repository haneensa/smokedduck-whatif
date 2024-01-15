import smokedduck
import pandas as pd

class TestProvenance(object):
    def test_provenance(self):
        p1 = pd.DataFrame({'a': [42, 43, 44, 45], 'b': ['a', 'b', 'a', 'b']})
        p2 = pd.DataFrame({'b': ['a', 'a', 'c', 'b'], 'c': [4, 5, 6, 7]})
        con = smokedduck.prov_connect(':default:')
        con.execute('create table prov1 as (select * from p1)')
        con.execute('create table prov2 as (select * from p2)')
        df = con.execute('SELECT prov1.b, sum(a + c) FROM prov1 join (select b, avg(c) as c from prov2 group by b) as t2 on prov1.b = t2.b group by prov1.b', capture_lineage='lineage').df()
        print(df)
        print(con.lineage().df())
        print(con.why().df())
        print(con.polynomial().df())
        print(con.backward([0, 2]).df())
        print(con.backward([1, 3], 'polynomial').df())
        print(con.forward('prov1', [0, 1]).df())
        print(con.forward('prov2', [2, 3]).df())

        all_rows = con.execute('SELECT prov1.b, sum(a + c) FROM prov1 join prov2 on prov1.b = prov2.b group by prov1.b', capture_lineage='lineage').fetchall()
        print(all_rows)
        print(con.lineage().df())
        print(con.why().df())
        print(con.polynomial().df())
        print(con.backward([0, 2]).df())
        print(con.backward([1, 3], 'polynomial').df())
        print(con.forward('prov1', [0, 1]).df())
        print(con.forward('prov2', [2, 3]).df())

        np_rows = con.execute('SELECT prov1.a, prov1.b, prov2.c FROM prov1 join prov2 on prov1.b = prov2.b', capture_lineage='lineage').fetchnumpy()
        print(np_rows)
        print(con.lineage().df())
        print(con.why().df())
        print(con.polynomial().df())
        print(con.backward([0, 2]).df())
        print(con.backward([1, 3], 'polynomial').df())
        print(con.forward('prov1', [0, 1]).df())
        print(con.forward('prov2', [2, 3]).df())
