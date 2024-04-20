import time
import pandas as pd
import smokedduck

con = smokedduck.connect('nested2.db')
con.execute(f'CALL dbgen(sf=20);')
con.execute('pragma threads=1')
tables = con.execute(f'pragma show_tables').df()
print(tables)

q1 = f"""select l_quantity as x, sum(2.0*l_extendedprice - l_extendedprice) as sy,
count(l_extendedprice - l_extendedprice) as cy
from lineitem group by l_quantity"""
q2 = f"""select ( (count(r.x)*sum(r.x*(r.sy/r.cy))) - (sum(r.x)*sum(r.sy/cy)) ) / ((count(r.x)*(sum(r.x*r.x)) - (sum(r.x))*(sum(r.x))))
from q1_out as r"""
q = f"""with r as ({q1})
select ( (count(r.x)*sum(r.x*(r.sy/r.cy))) - (sum(r.x)*sum(r.sy/r.cy)) ) / ((count(r.x)*(sum(r.x*r.x)) - (sum(r.x))*(sum(r.x))))
from r"""

start = time.time()
out = con.execute(q).df()
end = time.time()
no_lineage = end - start

start = time.time()
out = con.execute(q, capture_lineage='ksemimodule').df()
end = time.time()
print(out)
ksemimodule_timing = end - start
print("Lineage Capture Timing: ", ksemimodule_timing, no_lineage, ksemimodule_timing - no_lineage)

prune='true'
query_id = con.query_id
pp_timings = con.execute(f"pragma PrepareLineage({query_id}, {prune}, false)").df()
print(pp_timings)

use_duckdb = 'true'
num_threads = '8'
is_scalar = 'false'
batch = '1'
debug = 'false'
prune = 'true'
prob = '0.1'
itype = 'DENSE_DELETE'
is_incremental = 'false'
spec = 'lineitem.i'
distinct = '64'

q = f"pragma WhatIf({query_id}, '{itype}', '{spec}', {distinct}, {batch}, {is_scalar}, {use_duckdb}, {num_threads}, {debug}, {prune}, {is_incremental}, {prob});"
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
