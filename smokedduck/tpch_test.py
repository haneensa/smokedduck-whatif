import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')
con.execute('CALL dbgen(sf=0.1);')

subset = [2, 4, 11, 17, 21, 22]
fix_list = [11, 16, 20]
qid = "02"
print(f"############# Testing {qid} ###########")
query_file = f"queries/tpch/tpch_{qid}.sql"
logical_file = f"queries/perm/q{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())
print(sql)

# nested queries: 16 (empty, distinct)
# check: 2, 4,  11, 15, 17, 18, 20, 21, 22

# Printing lineage that was captured from base query
print(con.execute(sql, capture_lineage='lineage').df())
lineage = con.lineage().df()

with open(logical_file, "r") as f:
    logical_sql = " ".join(f.read().split())

print(logical_sql)

logical_lineage = con.execute(logical_sql).df()

logical_lineage = logical_lineage.reindex(sorted(logical_lineage.columns), axis=1)
lineage= lineage.reindex(sorted(lineage.columns), axis=1)

logical_lineage = logical_lineage.sort_values(by=list(logical_lineage.columns)).reset_index(drop=True)
lineage = lineage.sort_values(by=list(lineage.columns)).reset_index(drop=True)
lineage = lineage[list(logical_lineage.columns)]
print(lineage)
print(logical_lineage)
logical_lineage= logical_lineage.astype(lineage.dtypes)
print(lineage.isin(logical_lineage).all().all())
df_all = logical_lineage.merge(lineage, on=list(logical_lineage.columns), how='left', indicator=True)
right_only = df_all[ df_all['_merge'] == "right_only"]
print(right_only)
left_only = df_all[ df_all['_merge'] == "left_only"]
print(left_only)
both = df_all[ df_all['_merge'] == "both"]
print(both)
assert (len(both) == len(lineage) and len(right_only) == 0) or lineage.equals(logical_lineage), f"DataFrames do not have equal content, {qid}"
