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

# SEARCH: conjunctive predicate search using sparse encoding
#           is_incremental: 'true' then subtract deleted tuples, else recompute

# Creating connection
con = smokedduck.connect(':default:')
con.execute('CALL dbgen(sf=1);')
con.execute('pragma threads=1')
i = 1
qid = str(i).zfill(2)
print(f"############# Testing SA on {qid} ###########")
query_file = f"queries/tpch/tpch_{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())
print(sql)
# Printing lineage that was captured from base query
# 2. run the query with lineage capture
start = time.time()
out = con.execute(sql, capture_lineage='lineageAll').df()
end = time.time()
print(out)
ksemimodule_timing = end - start
query_id = con.query_id
k = 3
q = f"pragma SA({query_id}, {k}, 'lineitem.l_tax', false);"
res = con.execute(q).fetchdf()
print(res)
clear(con)
