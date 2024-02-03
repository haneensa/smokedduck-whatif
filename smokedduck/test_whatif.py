import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')
con.execute('CALL dbgen(sf=1);')
con.execute('pragma threads=1')

for i in [1]: #1, 3, 5, 6, 7, 8, 9, 10, 12]:
    qid = str(i).zfill(2)
    print(f"############# Testing Whatif on {qid} ###########")
    query_file = f"queries/tpch/tpch_{qid}.sql"
    with open(query_file, "r") as f:
        sql = " ".join(f.read().split())
    # Printing lineage that was captured from base query
    con.execute(sql, capture_lineage='lineage').df()
    query_id = con.query_id
    for use_duckdb in ['false']: #, 'false']:
        for is_scalar in ['true', 'false']:
            print("=================", query_id, qid, use_duckdb, is_scalar)
            # use_duckdb = false, is_scalr = true/false, batch = 4
            batch = 8
            q = f"pragma WhatIf({query_id}, 'd', 'lineitem:0.3', 1024, {batch}, {is_scalar}, {use_duckdb});"
            con.execute(q)
    tables = con.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            con.execute("DROP TABLE "+row["name"])
    con.execute("PRAGMA clear_lineage")
