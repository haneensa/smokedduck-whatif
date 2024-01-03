import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')
con.execute('CALL dbgen(sf=1);')

fix_list = [2, 16, 18, 20, 21, 22]
for i in range(1, 23):
    if i in fix_list:
        print(f"############# {i} SKIP ###########")
        continue
    qid = str(i).zfill(2)
    query_file = f"queries/tpch/tpch_{qid}.sql"
    logical_file = f"queries/perm/q{qid}.sql"
    try:
        with open(query_file, "r") as f:
            sql = " ".join(f.read().split())
        # Printing lineage that was captured from base query
        con.execute(sql, capture_lineage='lineage')
        lineage = con.lineage().df()
        with open(logical_file, "r") as f:
            logical_sql = " ".join(f.read().split())

        logical_lineage = con.execute(logical_sql).df()

        logical_lineage = logical_lineage.reindex(sorted(logical_lineage.columns), axis=1)
        lineage= lineage.reindex(sorted(lineage.columns), axis=1)

        logical_lineage = logical_lineage.sort_values(by=list(logical_lineage.columns)).reset_index(drop=True)
        lineage = lineage.sort_values(by=list(lineage.columns)).reset_index(drop=True)

        logical_lineage= logical_lineage.astype(lineage.dtypes)
        assert lineage.equals(logical_lineage), f"DataFrames do not have equal content, {qid}"
        print(f"############# {qid} PASSED ###########")
    except:
         print(f"############# {qid} FAILED ###########")
       
