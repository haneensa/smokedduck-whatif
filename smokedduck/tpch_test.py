import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')
con.execute('CALL dbgen(sf=1);')
con.execute('pragma threads=1')
# 4: right semi join
# 17, 0.1 single?
# 20 is totally weird it has single and semi join and right semi
# 21 has anti and right_semi

# scale: 4, 22, 20
fix_list = [4, 17, 20, 21]
qid = "17"
print(f"############# Testing {qid} ###########")
query_file = f"queries/tpch/tpch_{qid}.sql"
logical_file = f"queries/perm/q{qid}.sql"
with open(query_file, "r") as f:
    sql = " ".join(f.read().split())
print(sql)

# Printing lineage that was captured from base query
print(con.execute(sql, capture_lineage='lineage').df().to_string())
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
print(logical_lineage.isin(lineage).all().all())
df_all = lineage.merge(logical_lineage, on=list(logical_lineage.columns), how='left', indicator=True)
right_only = df_all[ df_all['_merge'] == "right_only"]
print(right_only)
left_only = df_all[ df_all['_merge'] == "left_only"]
print(left_only)
both = df_all[ df_all['_merge'] == "both"]
print(both)
assert (len(both) == len(lineage) and len(right_only) == 0) or lineage.equals(logical_lineage), f"DataFrames do not have equal content, {qid}"
