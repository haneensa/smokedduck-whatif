import smokedduck
import pandas as pd

p1 = pd.DataFrame({
    'a': ['1', '2', '1', '1', '2', '2', '1', '1', '2'],
    'b': ['a', 'b', 'a', 'b', 'c', 'a', 'a', 'a', 'b'],
    'c': ['x', 'x', 'x', 'y', 'y', 'z', 'z', 'z', 'z']
})
con = smokedduck.connect(':default:')
acon = con.cursor()
bcon = con.cursor()
ccon = con.cursor()

acon.execute('create table t1 as (select * from p1)')

print(acon.execute('select a, count(*) from t1 group by a', capture_lineage='ksemimodule'))
print(bcon.execute('select b, count(*) from t1 group by b', capture_lineage='ksemimodule'))
print(ccon.execute('select c, count(*) from t1 group by c', capture_lineage='ksemimodule'))

# Select column from A
selected_a = acon.backward([1], model='lineage').df()['t1']
print('Re-calculated b results')
print(bcon.forward('t1', selected_a, model='ksemimodule').df())
print('Re-calculated c results')
print(ccon.forward('t1', selected_a, model='ksemimodule').df())
print()

# Select column from B
selected_b = bcon.backward([0], model='lineage').df()['t1']
print('Re-calculated a results')
print(acon.forward('t1', selected_b, model='ksemimodule').df())
print('Re-calculated c results')
print(ccon.forward('t1', selected_b, model='ksemimodule').df())
print()

# Select two columns from C
selected_c = ccon.backward([0, 2], model='lineage').df()['t1']
print('Re-calculated a results')
print(acon.forward('t1', selected_c, model='ksemimodule').df())
print('Re-calculated c results')
print(bcon.forward('t1', selected_c, model='ksemimodule').df())
print()

# Select a third column from B (in addition to the 2 from C)
selected_b = bcon.backward([0], model='lineage').df()['t1']
print('Re-calculated a results')
print(acon.forward('t1', set(selected_b).intersection(selected_c), model='ksemimodule').df())
print()

con.execute('drop table t1')
