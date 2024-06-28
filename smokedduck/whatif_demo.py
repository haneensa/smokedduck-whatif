# TODO: (2) implement batching (compute 2K interventions at a time) and return the results
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

parser = argparse.ArgumentParser()
parser.add_argument("--specs", help="|table.col", type=str, default="intel.moteid")
parser.add_argument("--db", help="database name", type=str, default="tpch.db")
parser.add_argument("--sql", help="sql", type=str, default="select hr, count() as count from intel group by hr")
parser.add_argument("--agg_alias", help="agg_alias", type=str, default="c")
parser.add_argument("--num_workers", help="num_workers", type=int, default=1)
parser.add_argument('--groups', type=int, nargs='+', help='a list of groups', default=[])
args = parser.parse_args()

specs_tokens = args.specs.split('|')

qid = args.sql.zfill(2)
query_file = f"queries/tpch/tpch_{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())

with smokedduck.connect(args.db) as con:
    clear(con)
    tables = con.execute("PRAGMA show_tables").fetchdf()

    for t in tables["name"]:
        print(t)
        print(con.execute(f"pragma table_info('{t}')"))


    cols = []
    for token in specs_tokens:
        table_col = token.split('.')
        table = table_col[0]
        col = table_col[1]
        cols.append(col)

    start = time.time()
    out = con.execute(sql, capture_lineage='lineageAll').df()
    end = time.time()
    print(out)
    query_timing = end - start
    print(query_timing)
    query_id = con.query_id

    pp_timings = con.execute(f"pragma PrepareLineage({query_id}, false, false, false)").df()
    print(args.groups)

    #q = f"pragma WhatIfSparse({query_id}, {args.agg_alias}, {args.groups}, '{args.specs}', false);"
    q = f"pragma WhatIfSparseCompile({query_id}, {args.agg_alias}, '{args.specs}', {args.num_workers}, true, true, false);"
    res = con.execute(q).fetchdf()
    print(res)

    if len(args.groups) == 0:
        q = f"select * from duckdb_fade() where g0 > 0;"
        qp = f"pragma getpredicate(0);"
    else:
        q = f"select * from duckdb_fade() where g{args.groups[0]} > 0;"
        qp = f"pragma GetPredicate({args.groups[0]});"
    res = con.execute(q).fetchdf()
    print(res)

    res = con.execute(qp).fetchdf()
    print(res)

    clear(con)

    #cols_str = ",".join(cols)
    #sql += ", " + cols_str
    #sql += " order by " + cols_str
    #
    #sql = sql.lower().replace("select", "select " + cols_str + ",")
    #print(sql)
    #print(cols_str)
    #
    #out = con.execute(sql).fetchdf()
    #print(out)
