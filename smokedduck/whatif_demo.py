# TODO: 
# 1. map intervention ids to actual predicates
# 3. optimize retrieving the final results (check)
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

parser = argparse.ArgumentParser()
parser.add_argument("--specs", help="|table.col", type=str, default="intel.moteid")
parser.add_argument("--sql", help="sql", type=str, default="select hr, count() as count from intel group by hr")
parser.add_argument("--aggid", help="agg_id", type=str, default=0)
parser.add_argument("--groupid", help="group_id", type=str, default=-1)
args = parser.parse_args()

specs_tokens = args.specs.split('|')

cols = []
for token in specs_tokens:
    table_col = token.split('.')
    table = table_col[0]
    col = table_col[1]
    cols.append(col)

print(args.sql)

start = time.time()
out = con.execute(args.sql, capture_lineage='lineageAll').df()
end = time.time()
print(out)
query_timing = end - start
print(query_timing)
query_id = con.query_id

pp_timings = con.execute(f"pragma PrepareLineage({query_id}, false, false, false)").df()

q = f"pragma WhatIfSparse({query_id}, {args.aggid}, {args.groupid}, '{args.specs}', false);"
res = con.execute(q).fetchdf()
print(res)

q = f"select * from duckdb_fade() where g0 > 0;"
res = con.execute(q).fetchdf()
print(res)

q = f"pragma GetPredicate(0);"
res = con.execute(q).fetchdf()
print(res)

clear(con)

cols_str = ",".join(cols)
args.sql += ", " + cols_str
args.sql += " order by " + cols_str

sql = args.sql.lower().replace("select", "select " + cols_str + ",")
print(sql)
print(cols_str)

out = con.execute(sql).fetchdf()
print(out)
