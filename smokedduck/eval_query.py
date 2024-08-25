import time
import csv
import argparse
import smokedduck
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--sf", help="sf", type=float, default=1)
parser.add_argument("--csv", help="csv", type=str, default="out.csv")
parser.add_argument("--model", help="model", type=str, default="lineage")
parser.add_argument("--workload", help="workload", type=str, default="tpch")
args = parser.parse_args()

print(args.model)
def clear(c):
    tables = c.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            print("drop", row["name"])
            c.execute("DROP TABLE "+row["name"])
    c.execute("PRAGMA clear_lineage")

res = []

def evalq(sql, res, workload, i, sf):
    # 1. run the query without lineage capture
    query_timing=0
    for n in range(3):
        start = time.time()
        out = con.execute(sql).df()
        end = time.time()
        query_timing += end - start
        clear(con)
    query_timing /= 3


    start = time.time()
    out = con.execute(sql, capture_lineage=args.model).df()
    end = time.time()
    lineage_timing = end - start
    clear(con)
    res.append([i, sf, args.model, workload, query_timing, lineage_timing])

sf = args.sf
if args.workload == "tpch":
    con = smokedduck.connect(':default:')
    con.execute(f'CALL dbgen(sf={sf});')
    # capture provenance overhead for flights and nsf workload
    for i in [1, 3, 5, 6, 7, 8, 9, 10, 12, 14, 19]:
        qid = str(i).zfill(2)
        query_file = f"queries/tpch/tpch_{qid}.sql"
        with open(query_file, "r") as f:
            sql = " ".join(f.read().split())
            evalq(sql, res, args.workload, i, sf)
elif args.workload == "nsf":
    con = smokedduck.connect('build/db/nsf_data_keyed.db')
    tables = con.execute("PRAGMA show_tables").fetchdf()
    i = 1
    qid = str(i).zfill(2)
    query_file = f"queries/nsf/nsf_{qid}.sql"
    with open(query_file, "r") as f:
        sql = " ".join(f.read().split())
        evalq(sql, res, args.workload, i, 1)
elif args.workload == "flights":
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

    q1 = f"""select year as x, sum(ActualElapsedTime-AirTime) as sy,
    count() as cy
    from flights WHERE YEAR >= 2007 and YEAR <= 2011 group by year"""
    sql= f"""with r as ({q1})
    select ( (count(r.x)*sum(r.x*(r.sy/r.cy))) - (sum(r.x)*sum(r.sy/r.cy)) ) / ((count(r.x)*(sum(r.x*r.x)) - (sum(r.x))*(sum(r.x))))
    from r"""
    evalq(sql, res, args.workload, 1, 1)

filename=args.csv
with open(filename, 'a') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(res)
