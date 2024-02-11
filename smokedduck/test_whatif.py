import time
import csv
import argparse
import smokedduck
import pandas as pd

def clear():
    tables = con.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            print("drop", row["name"])
            con.execute("DROP TABLE "+row["name"])
    con.execute("PRAGMA clear_lineage")

parser = argparse.ArgumentParser()
parser.add_argument("--i", help="qid", type=int, default=1)
parser.add_argument("--interventions", help="interventions", type=int, default=1024)
parser.add_argument("--sf", help="sf", type=float, default=1)
parser.add_argument("--use-duckdb", help="use duckdb", type=str, default="true")
parser.add_argument("--t", help="thread num", type=int, default=1)
parser.add_argument("--is-scalar", help="is scalar", type=str, default="true")
parser.add_argument("--csv", help="csv", type=str, default="out.csv")
parser.add_argument("--debug", help="debug", type=str, default="true")
parser.add_argument("--prune", help="prune", type=str, default="true")
args = parser.parse_args()
# Creating connection
con = smokedduck.connect('db.out')
# con.execute('CALL dbgen(sf=1);')
con.execute('pragma threads=1')
# 1, 3, 5, 6, 7, 8, 9, 10, 12
# true, false
i = args.i
use_duckdb = args.use_duckdb
num_threads = args.t
is_scalar = args.is_scalar
batch = 4
debug = args.debug
prune = args.prune
itype = "'DELETE_SPEC'"

qid = str(i).zfill(2)
distinct = args.interventions

print(f"############# Testing Whatif on {qid} ###########")
query_file = f"queries/tpch/tpch_{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())
# Printing lineage that was captured from base query

# 1. run the query without lineage capture
start = time.time()
out = con.execute(sql).df()
end = time.time()
query_timing = end - start

# 2. run the query with lineage capture
start = time.time()
out = con.execute(sql, capture_lineage='ksemimodule').df()
end = time.time()
lineage_capture_timing = end - start

query_id = con.query_id
print("=================", query_id, qid, use_duckdb, is_scalar, num_threads)
# use_duckdb = false, is_scalr = true/false, batch = 4
q = f"pragma WhatIf({query_id}, {itype}, 'lineitem.i', {distinct}, {batch}, {is_scalar}, {use_duckdb}, {num_threads}, {debug}, {prune});"
timings = con.execute(q).fetchdf()
print(timings)

res = [args.sf, i, use_duckdb, is_scalar, prune, num_threads, distinct, batch,
        timings["post_processing_time"][0], timings["intervention_gen_time"][0],
        timings["prep_time"][0], timings["compile_time"][0], timings["eval_time"][0],
        timings["prune_time"][0], timings["lineage_time"][0], lineage_capture_timing,
        query_timing]
print(res)

clear()
filename=args.csv
print(filename)
with open(filename, 'a') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(res)
