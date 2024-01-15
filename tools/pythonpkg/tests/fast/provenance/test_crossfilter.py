import smokedduck
import pandas as pd

class TestCrossFilter(object):
    def test_cross_filter(self):
        p1 = pd.DataFrame({
            'a': ['1', '2', '1', '1', '2', '2', '1', '1', '2'],
            'b': ['a', 'b', 'a', 'b', 'c', 'a', 'a', 'a', 'b'],
            'c': ['x', 'x', 'x', 'y', 'y', 'z', 'z', 'z', 'z']
        })
        con = smokedduck.prov_connect(':default:')
        acon = con.cursor()
        bcon = con.cursor()
        ccon = con.cursor()

        acon.execute('create table crossfilter as (select * from p1)')

        print(acon.execute('select a, count(*) from crossfilter group by a', capture_lineage='ksemimodule'))
        print(bcon.execute('select b, count(*) from crossfilter group by b', capture_lineage='ksemimodule'))
        print(ccon.execute('select c, count(*) from crossfilter group by c', capture_lineage='ksemimodule'))

        # Select column from A
        selected_a = acon.backward([1], model='lineage').df()['crossfilter']
        print('Re-calculated b results')
        print(bcon.forward('crossfilter', selected_a, model='ksemimodule').df())
        print('Re-calculated c results')
        print(ccon.forward('crossfilter', selected_a, model='ksemimodule').df())
        print()

        # Select column from B
        selected_b = bcon.backward([0], model='lineage').df()['crossfilter']
        print('Re-calculated a results')
        print(acon.forward('crossfilter', selected_b, model='ksemimodule').df())
        print('Re-calculated c results')
        print(ccon.forward('crossfilter', selected_b, model='ksemimodule').df())
        print()

        # Select two columns from C
        selected_c = ccon.backward([0, 2], model='lineage').df()['crossfilter']
        print('Re-calculated a results')
        print(acon.forward('crossfilter', selected_c, model='ksemimodule').df())
        print('Re-calculated c results')
        print(bcon.forward('crossfilter', selected_c, model='ksemimodule').df())
        print()

        # Select a third column from B (in addition to the 2 from C)
        selected_b = bcon.backward([0], model='lineage').df()['crossfilter']
        print('Re-calculated a results')
        print(acon.forward('crossfilter', set(selected_b).intersection(selected_c), model='ksemimodule').df())
        print()

