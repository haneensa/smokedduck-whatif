import smokedduck
import pandas as pd

class TestMinimalProvenance(object):
    def test_minimal_provenance(self):
        # Creating connection
        con = smokedduck.prov_connect(':default:')

        # Loading example data
        p1 = pd.DataFrame({'a': [42, 43, 44, 45], 'b': ['a', 'b', 'a', 'b']})
        p2 = pd.DataFrame({'b': ['a', 'a', 'c', 'b'], 'c': [4, 5, 6, 7]})
        con.execute('create table min_prov1 as (select * from p1)')
        con.execute('create table min_prov2 as (select * from p2)')

        # Executing base query
        con.execute('SELECT min_prov1.b, sum(min_prov1.a + t2.c) FROM min_prov1 join (select b, avg(c) as c from min_prov2 group by b) as t2 on min_prov1.b = t2.b group by min_prov1.b', capture_lineage='lineage').df()

        # Printing lineage that was captured from base query
        print(con.lineage().df())