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
parser.add_argument("--i", help="qid", type=int, default=1)
parser.add_argument("--interventions", help="interventions", type=int, default=1024)
parser.add_argument("--sf", help="sf", type=float, default=1)
parser.add_argument("--t", help="thread num", type=int, default=1)
parser.add_argument("--csv", help="csv", type=str, default="out.csv")

args = parser.parse_args()
i = args.i
num_threads = args.t
distinct = args.interventions
# Creating connection
con = smokedduck.connect('db.out')
# con.execute('CALL dbgen(sf=1);')
con.execute(f'pragma threads={num_threads}')
# 1, 3, 5, 6, 7, 8, 9, 10, 12
# true, false

qid = str(i).zfill(2)

print(f"############# Testing Whatif on {qid} ###########")
query_file = f"queries/tpch/tpch_{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())

if distinct > 0:
    sql += ", fade_col_" + str(distinct)
    sql = sql.replace("SELECT", "SELECT fade_col_"+str(distinct)+",")
# Printing lineage that was captured from base query
# 2. run the query with lineage capture
start = time.time()
out = con.execute(sql).df()
end = time.time()
print(out)
timing = end - start
res = [args.sf, i,  num_threads, distinct, timing]
print(res)

filename=args.csv
print(filename)
with open(filename, 'a') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(res)
