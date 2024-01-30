import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')
con.execute('CALL dbgen(sf=1);')
con.execute('pragma threads=1')

qid = "01"
print(f"############# Testing Whatif on {qid} ###########")
query_file = f"queries/tpch/tpch_{qid}.sql"
logical_file = f"queries/perm/q{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())
print(sql)

# Printing lineage that was captured from base query
print(con.execute(sql, capture_lineage='lineage').df())

q = "pragma WhatIf(1, 'd', 'lineitem:0.3', 1024);"
con.execute(q)
