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

parser = argparse.ArgumentParser()
parser.add_argument("--group", help="g", type=int, default=10)
parser.add_argument("--card", help="n", type=int, default=10000)
parser.add_argument("--alpha", help="a", type=float, default=1)
parser.add_argument("--naggs", help="naggs", type=int, default=1)
parser.add_argument("--interventions", help="interventions", type=int, default=1024)
parser.add_argument("--t", help="thread num", type=int, default=1)
parser.add_argument("--is-scalar", help="is scalar", type=str, default="true")
parser.add_argument("--csv", help="csv", type=str, default="out.csv")
parser.add_argument("--debug", help="debug", type=str, default="true")
parser.add_argument("--prune", help="prune", type=str, default="true")
parser.add_argument("--itype", help="Intervention Type", type=str, default="DENSE_DELETE")
parser.add_argument("--prob", help="Deletion Probability", type=float, default="0.1")
parser.add_argument("--batch", help="Agg functions batch", type=int, default="4")
parser.add_argument("--use-gb-bw-lineage", help="use gb backward lineage", type=str, default="false")

args = parser.parse_args()
# Creating connection
con = smokedduck.connect(f'db_{args.group}_{args.card}_{args.alpha}_{args.naggs}.out')
# con.execute('CALL dbgen(sf=1);')
con.execute('pragma threads=1')
# 1, 3, 5, 6, 7, 8, 9, 10, 12
# true, false
num_threads = args.t
is_scalar = args.is_scalar
batch = args.batch
debug = args.debug
prune = args.prune
prob = args.prob
itype = args.itype
use_gb_bw_lineage = args.use_gb_bw_lineage
if use_gb_bw_lineage == "true":
    use_duckdb = "false"

distinct = args.interventions

print(f"############# Testing {args.group} {args.card} {args.alpha} ###########")
sql = """select z, sum(v) from micro_table group by z"""
# Printing lineage that was captured from base query
# 2. run the query with lineage capture
start = time.time()
out = con.execute(sql, capture_lineage='lineageAll').df()
end = time.time()
print(out)
ksemimodule_timing = end - start
#print(no_lineage, ksemimodule_timing, ksemimodule_timing-no_lineage)

query_id = con.query_id
print("=================", query_id, is_scalar, num_threads)
forward_lineage = "false"
pp_timings = con.execute(f"pragma PrepareLineage({query_id}, {prune}, {forward_lineage}, {use_gb_bw_lineage})").df()
print(pp_timings)
#clear(con)
# use_duckdb = false, is_scalr = true/false, batch = 4
use_duckdb="false"
is_incremental="false"
spec=""
q = f"pragma WhatIf({query_id}, '{itype}', '{spec}', {distinct}, {batch}, {is_scalar}, {use_duckdb}, {num_threads}, {debug}, {prune}, {is_incremental}, {prob}, {use_gb_bw_lineage});"
timings = con.execute(q).fetchdf()
print(timings)

clear(con)
if spec == '""':
    spec=''
res = [args.group, args.card, args.alpha, args.naggs, itype, prob, is_incremental, use_duckdb, is_scalar, prune, num_threads, distinct, batch,
        pp_timings["post_processing_time"][0], timings["intervention_gen_time"][0],
        timings["prep_time"][0], timings["compile_time"][0], timings["eval_time"][0],
        pp_timings["prune_time"][0], pp_timings["lineage_time"][0], ksemimodule_timing, spec,
        pp_timings["lineage_count"][0], pp_timings["lineage_count_prune"][0],
        pp_timings["lineage_size_mb"][0], pp_timings["lineage_size_mb_prune"][0],
        use_gb_bw_lineage
        ]
print(res)

filename=args.csv
print(filename)
with open(filename, 'a') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(res)
