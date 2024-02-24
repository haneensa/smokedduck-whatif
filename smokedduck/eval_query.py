import time
import csv
import argparse
import smokedduck
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--sf", help="sf", type=float, default=1)
parser.add_argument("--csv", help="csv", type=str, default="out.csv")
args = parser.parse_args()

def clear(c):
    tables = c.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            print("drop", row["name"])
            c.execute("DROP TABLE "+row["name"])
    c.execute("PRAGMA clear_lineage")

res = []
sf = args.sf
con = smokedduck.connect(':default:')
con.execute(f'CALL dbgen(sf={sf});')
for i in [1, 3, 5, 7, 9, 10, 12]:
    qid = str(i).zfill(2)
    query_file = f"queries/tpch/tpch_{qid}.sql"
    with open(query_file, "r") as f:
        sql = " ".join(f.read().split())
    # 1. run the query without lineage capture
    start = time.time()
    out = con.execute(sql).df()
    end = time.time()
    query_timing = end - start
    clear(con)

    start = time.time()
    out = con.execute(sql, capture_lineage='lineage').df()
    end = time.time()
    lineage_timing = end - start
    clear(con)
    res.append([i, sf, query_timing, lineage_timing])

filename=args.csv
with open(filename, 'a') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(res)
