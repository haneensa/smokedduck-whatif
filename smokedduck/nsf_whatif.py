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
parser.add_argument("--i", help="qid", type=int, default=1)
parser.add_argument("--interventions", help="interventions", type=int, default=1024)
parser.add_argument("--sf", help="sf", type=float, default=1)
parser.add_argument("--use-duckdb", help="use duckdb", type=str, default="true")
parser.add_argument("--t", help="thread num", type=int, default=1)
parser.add_argument("--is-scalar", help="is scalar", type=str, default="true")
parser.add_argument("--csv", help="csv", type=str, default="out.csv")
parser.add_argument("--debug", help="debug", type=str, default="true")
parser.add_argument("--prune", help="prune", type=str, default="true")
parser.add_argument("--itype", help="Intervention Type", type=str, default="DENSE_DELETE")
parser.add_argument("--incremental", help="true if the agg functions are incremental", type=str, default="true")
parser.add_argument("--prob", help="Deletion Probability", type=float, default="0.1")
parser.add_argument("--batch", help="Agg functions batch", type=int, default="4")
# DENSE_DELETE_ALL: dense matrix encoding and evaluation on all tables using prob specified by --prob
# DENSE_DELETE_SPEC: same as DENSE_ALL except on only tables specified by --spec
# SEARCH: conjunctive predicate search using sparse encoding
#           is_incremental: 'true' then subtract deleted tuples, else recompute

args = parser.parse_args()
# Creating connection
con = smokedduck.connect('build/db/nsf_data_keyed.db')
con.execute('pragma threads=1')
# true, false
i = args.i
use_duckdb = args.use_duckdb
num_threads = args.t
is_scalar = args.is_scalar
batch = args.batch
debug = args.debug
prune = args.prune
prob = args.prob
itype = args.itype
is_incremental = args.incremental

qid = str(i).zfill(2)
distinct = args.interventions

def exp2():
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
#    return exp_dict
    
#def exp3():
#    n=0
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

    return exp_dict

exp_per_table = {}
exp_per_table.update(exp2())
print(exp_per_table)
# for each table run select * from table_name
for table, plist in exp_per_table.items():
    slist = ",\n".join(plist)
    query = f"select {slist} from {table}"
    print(query)
    start = time.time()
    I = con.execute(query).df()
    end = time.time()
    print(len(I), end - start)
    print(I)
    packed_array = np.packbits(I.to_numpy().astype(np.uint8), axis=1)
    print(packed_array)
    print(packed_array.shape)
    print(np.count_nonzero(packed_array, axis=0))


print(f"############# Testing Whatif on {qid} ###########")
query_file = f"queries/nsf/nsf_{qid}.sql"
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
#print(query_timing, lineage_timing, ksemimodule_timing)

query_id = con.query_id
print("=================", query_id, qid, use_duckdb, is_scalar, num_threads)
# use_duckdb = false, is_scalr = true/false, batch = 4
# 1. whatif institution.instname=?
# 2. whatif  award.duration=?
# 3. whatif award.pis=?
# 4. whatif pi.piname IN () --> need cascade delete to construct interventions on award relation
forward_lineage = "false"
if distinct == 1 and is_incremental == "true":
    forward_lineage = "true"

pp_timings = con.execute(f"pragma PrepareLineage({query_id}, {prune}, {forward_lineage})").df()
print(pp_timings)

# prepare interventions
# HACK: construct df, write the df to disk, read data in duckdb
#con.execute(f"pragma PrepareInterventions({viees_list})")

# TODO: add interface to construct templates and lineage for cascade delete and save them
## TODO: use the template to generate interventions for table A using table B with PK:FK relations
q = f"pragma WhatIf({query_id}, '{itype}', 'Investigator.PIName|Award.aid', {distinct}, {batch}, {is_scalar}, {use_duckdb}, {num_threads}, {debug}, {prune}, {is_incremental}, {prob});"
timings = con.execute(q).fetchdf()
print(timings)

clear(con)

res = [args.sf, i, itype, prob, is_incremental, use_duckdb, is_scalar, prune, num_threads, distinct, batch,
        pp_timings["post_processing_time"][0], timings["intervention_gen_time"][0],
        timings["prep_time"][0], timings["compile_time"][0], timings["eval_time"][0],
        pp_timings["prune_time"][0], pp_timings["lineage_time"][0], ksemimodule_timing]
print(res)

filename=args.csv
print(filename)
with open(filename, 'a') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(res)
