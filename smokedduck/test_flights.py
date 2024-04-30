import time
import pandas as pd
import smokedduck

con = smokedduck.connect('flights.db')
if 0:
    con.execute('pragma threads=16')
    csvfile = '/home/haneenmo/airline.csv.shuffle'
    try:
        data = pd.read_csv(csvfile, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            data = pd.read_csv(csvfile, encoding='latin1')
        except UnicodeDecodeError:
            data = pd.read_csv(csvfile, encoding='ISO-8859-1')

    con.execute(f"create table flights as (select * from data)")
print(con.execute("select year, count() from flights group by year").df())
print(con.execute("select count() from flights").df())

q1 = f"""select year as x, sum(ActualElapsedTime-AirTime) as sy,
count() as cy
from flights WHERE YEAR >= 2007 and YEAR <= 2011 group by year"""
q = f"""with r as ({q1})
select ( (count(r.x)*sum(r.x*(r.sy/r.cy))) - (sum(r.x)*sum(r.sy/r.cy)) ) / ((count(r.x)*(sum(r.x*r.x)) - (sum(r.x))*(sum(r.x))))
from r"""

con.execute('pragma threads=1')
start = time.time()
out = con.execute(q1).df()
end = time.time()
no_lineage = end - start
print(out)

start = time.time()
out = con.execute(q).df()
end = time.time()
no_lineage = end - start
print(out)

start = time.time()
out = con.execute(q, capture_lineage='ksemimodule').df()
end = time.time()
ksemimodule_timing = end - start
print("Lineage Capture Timing: ", ksemimodule_timing, no_lineage, ksemimodule_timing - no_lineage)

prune = 'true'
query_id = con.query_id
pp_timings = con.execute(f"pragma PrepareLineage({query_id}, {prune}, false, false)").df()
print(pp_timings)

use_duckdb = 'false'
num_threads = '8'
is_scalar = 'false'
batch = '1'
debug = 'false'
prob = '0.1'
itype = 'DENSE_DELETE'
is_incremental = 'false'
spec = 'flights.i'
distinct = '896'

q = f"pragma WhatIf({query_id}, '{itype}', '{spec}', {distinct}, {batch}, {is_scalar}, {use_duckdb}, {num_threads}, {debug}, {prune}, {is_incremental}, {prob}, false);"
timings = con.execute(q).fetchdf()
print(timings)

def clear(c):
    tables = c.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            print("drop", row["name"])
            c.execute("DROP TABLE "+row["name"])
    c.execute("PRAGMA clear_lineage")

clear(con)
