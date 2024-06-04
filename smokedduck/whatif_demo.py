# TODO: 
# 1. map intervention ids to actual predicates
# 3. optimize retrieving the final results (check)
# 4. pass an index to the aggregates so that we only operator on a single aggregate at a time

import numpy as np
import time
import csv
import argparse
import smokedduck
import pandas as pd

def clear(c):
    tables = c.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            print("drop", row["name"])
            c.execute("DROP TABLE "+row["name"])
    c.execute("PRAGMA clear_lineage")

con = smokedduck.connect('intel.db')
tables = con.execute("PRAGMA show_tables").fetchdf()

for t in tables["name"]:
    print(t)
    print(con.execute(f"pragma table_info('{t}')"))

#specs = "lineitem.l_linestatus"
specs = "intel.moteid"
specs_tokens = specs.split('|')
#
cols = []
for token in specs_tokens:
    table_col = token.split('.')
    print(table_col)
    table = table_col[0]
    col = table_col[1]
    cols.append(col)

# TODO: figure out the best way to pass data

#i = 1
#qid = str(i).zfill(2)
#con.execute('pragma threads=1')
#query_file = f"queries/tpch/tpch_{qid}.sql"
#with open(query_file, "r") as f:
#    sql = " ".join(f.read().split())

sql = "select hr, count() from intel group by hr"
print(sql)

start = time.time()
out = con.execute(sql, capture_lineage='lineageAll').df()
end = time.time()
print(out)
query_timing = end - start
print(query_timing)
query_id = con.query_id

prune = 'false'
pp_timings = con.execute(f"pragma PrepareLineage({query_id}, {prune}, false, false)").df()
print(pp_timings)

q = f"pragma WhatIfSparse({query_id}, 10, '{specs}', false);"
timings = con.execute(q).fetchdf()
print(timings)

clear(con)

cols_str = ",".join(cols)
sql += ", " + cols_str
sql += " order by " + cols_str

sql = sql.lower().replace("select", "select " + cols_str + ",")
print(sql)
print(cols_str)

out = con.execute(sql).fetchdf()
print(out)
