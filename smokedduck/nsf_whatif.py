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
parser.add_argument("--interventions", help="interventions", type=int, default=1024)
parser.add_argument("--sf", help="sf", type=float, default=1)
parser.add_argument("--use-duckdb", help="use duckdb", type=str, default="true")
parser.add_argument("--t", help="thread num", type=int, default=1)
parser.add_argument("--mat", help="dense vs search", type=int, default=1)
parser.add_argument("--is-scalar", help="is scalar", type=str, default="true")
parser.add_argument("--csv", help="csv", type=str, default="out.csv")
parser.add_argument("--debug", help="debug", type=str, default="true")
parser.add_argument("--prune", help="prune", type=str, default="true")
parser.add_argument("--batch", help="Agg functions batch", type=int, default="4")

args = parser.parse_args()
# Creating connection
con = smokedduck.connect('build/db/nsf_data_keyed.db')
tables = con.execute("PRAGMA show_tables").fetchdf()
print(tables)
for t in tables["name"]:
    print(con.execute(f"pragma table_info('{t}')"))
# true, false
i = 1
use_duckdb = args.use_duckdb
num_threads = args.t
is_scalar = args.is_scalar
batch = args.batch
debug = args.debug
prune = args.prune

qid = str(i).zfill(2)

def exp():
    exp_dict = {"Investigator": [], "Award": []}
    n=0
    for K in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        res = f"""(PIName IN (SELECT PIName 
        from (SELECT i.PIName, avg(a.amount) as amt FROM Investigator i, Award a  where i.aid=a.aid GROUP BY i.PIName ORDER BY amt LIMIT {K}))) as e{n}"""
        exp_dict["Investigator"].append(res)
        n += 1
    
    for K in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        res = f"""(PIName IN (SELECT PIName from 
        (SELECT i.PIName, sum(a.amount) as amt
        FROM Investigator i, Award a  where i.aid=a.aid GROUP BY i.PIName ORDER BY amt LIMIT {K}))) as e{n}"""
        exp_dict["Investigator"].append(res)
        n += 1
    
    for v in [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (10, 100000000000000000000000)]:
        l = v[0]
        r = v[1]
        res = f"""(PIName IN (select PIName from (SELECT PIName, num
                FROM (SELECT PIName, count(1) as num FROM Investigator GROUP BY PIName)
                WHERE num >= {l} and num <= {r}))) as e{n}"""
        exp_dict["Investigator"].append(res)
        n += 1
    
    for v in [(0, 1), (1, 5), (5, 10), (10, 50), (50, 100), (100, 100000000000000000000000)]:
        l = v[0]
        r = v[1]
        res = f"""(PIName IN (SELECT PIName FROM (SELECT i.PIName, sum(a.amount/1000000) as amt FROM Investigator i, Award a where i.aid=a.aid GROUP BY PIName
                    HAVING amt >= {l} and amt < {r}))) as e{n}"""
        exp_dict["Investigator"].append(res)
        n += 1
    
    for v in [(0,  1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (7, 8), (8, 9), (9, 10), (10, 11), (11, 100000000000000000000000)]:
        l = v[0]
        r = v[1]
        res = f"""(aid IN (SELECT aid FROM (SELECT aid, date_sub('year', strptime(startdate, '%m/%d/%Y'), strptime(enddate, '%m/%d/%Y')) as num_years 
                 FROM Award WHERE num_years >= {l} and num_years < {r}))) as e{n}"""

        exp_dict["Award"].append(res)
        n += 1

    for v in [(1,  2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 100000000000000000000000)]:
        l = v[0]
        r = v[1]
        res = f"""(aid IN (select aid FROM (SELECT aid, num 
        FROM (SELECT aid, count(1) as num FROM Investigator GROUP BY aid) WHERE num >= {l} and num < {r}))) as e{n}"""
        exp_dict["Award"].append(res)
        n += 1

    return exp_dict, n

if args.mat:
    exp_per_table = {}
    proj_list, distinct_count = exp()
    exp_per_table.update(proj_list)
    interventions_shape_per_table = {}
    print(exp_per_table)
    con.execute('pragma threads=16')
    spec = ""
    distinct = distinct_count+16-(distinct_count % 16)
    print("-->", distinct)
    count_so_far = 0
    debug_local = True
    start_all = time.time()
    q_eval_time = 0
    for table, plist in exp_per_table.items():
        slist = ",\n".join(plist)
        pad_count = distinct-len(plist)-count_so_far
        query = f"select {slist} from {table}"
        start = time.time()
        I = con.execute(query).df()
        end = time.time()
        q_eval_time += (end - start)
        exec_time = end - start
        prefix_extra_columns = np.zeros((I.shape[0], count_so_far)).astype(np.uint8)
        extra_columns = np.zeros((I.shape[0], pad_count)).astype(np.uint8)
        M_pad = np.concatenate((prefix_extra_columns, I.to_numpy().astype(np.uint8), extra_columns), axis=1)
        packed_array = np.packbits(M_pad, axis=1)
        filename = f"{table}.npy"
        packed_array.tofile(filename)
        interventions_shape_per_table[table] = f"{packed_array.shape[0]}, {packed_array.shape[1]}, {packed_array.dtype}"
        if len(spec) > 0:
            spec += "|"
        spec += f"{table}.npy_{packed_array.shape[0]}_{packed_array.shape[1]}"
        count_so_far += len(plist)
        if debug_local:
            M = I.to_numpy().astype(np.uint8)
            print(len(plist), distinct, pad_count)
            print(query)
            print("Target Relation: ")
            print(len(I), end - start)
            print(I)
            print(np.count_nonzero(I, axis=0))
            print("Target Matrix: ")
            print(np.count_nonzero(M_pad, axis=0))
            print(packed_array.shape, packed_array.dtype)
            # write it to desk: table.
    end_all = time.time()
    all_time = end_all - start_all
    print("intervention generation time: ", all_time, q_eval_time)
    print(interventions_shape_per_table)
    print(spec)
else:
    table = "Investigator"
    col = "PIName"
    # read PIName column, get unique values, write column to disk
    query = f"select {col} from {table}"
    start = time.time()
    I = con.execute(query).df()
    end = time.time()  
    code_df, unique_vals = pd.factorize(I[col])
    print(I)
    print(code_df, len(code_df), end-start)
    filename = f"{table}.npy"
    code_df.astype(np.int32).tofile(filename)
    spec = f"{table}.npy_{len(code_df)}_{len(unique_vals)}"

print(f"############# Testing Whatif on {qid} ###########")
con.execute('pragma threads=1')
query_file = f"queries/nsf/nsf_{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())
print(sql)

start = time.time()
out = con.execute(sql, capture_lineage='lineageAll').df()
end = time.time()
print(out)

ksemimodule_timing = end - start
#print(query_timing, lineage_timing, ksemimodule_timing)

query_id = con.query_id
print("=================", query_id, qid, use_duckdb, is_scalar, num_threads)
forward_lineage = "false"
pp_timings = con.execute(f"pragma PrepareLineage({query_id}, {prune}, {forward_lineage}, false)").df()
print(pp_timings)


if args.mat:
    itype="DENSE_DELETE"
    is_incremental = "false"
    q = f"pragma WhatIf({query_id}, '{itype}', '{spec}', {distinct}, {batch}, {is_scalar}, {use_duckdb}, {num_threads}, {debug}, {prune}, {is_incremental}, 0, false);"
    timings = con.execute(q).fetchdf()
    print(timings)
else:
    itype="SEARCH"
    is_incremental = "true"
    distinct = 170619
    #spec = "Investigator.nyp_PIName"
    q = f"pragma WhatIf({query_id}, '{itype}', '{spec}', {distinct}, {batch}, {is_scalar}, {use_duckdb}, {num_threads}, {debug}, {prune}, {is_incremental}, 0, false);"
    timings = con.execute(q).fetchdf()
    print(timings)

clear(con)

res = [args.sf, i, itype, 0, is_incremental, use_duckdb, is_scalar, prune, num_threads, distinct, batch,
        pp_timings["post_processing_time"][0], timings["intervention_gen_time"][0],
        timings["prep_time"][0], timings["compile_time"][0], timings["eval_time"][0],
        pp_timings["prune_time"][0], pp_timings["lineage_time"][0], ksemimodule_timing, spec,
        pp_timings["lineage_count"][0], pp_timings["lineage_count_prune"][0],
        pp_timings["lineage_size_mb"][0], pp_timings["lineage_size_mb_prune"][0],
        "false",
        timings["code_gen_time"][0], timings["data_time"][0]
        ]
print(res)

filename=args.csv
print(filename)
with open(filename, 'a') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(res)
