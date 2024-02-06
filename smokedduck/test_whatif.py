import smokedduck
import pandas as pd

# Creating connection
con = smokedduck.connect(':default:')
con.execute('CALL dbgen(sf=1);')
con.execute('pragma threads=1')

for i in [1]: #, 3, 5, 6, 7, 8, 9, 10, 12]:
    for use_duckdb in ['true']:#, 'false']:
        for is_scalar in ['false']:#, 'false']:
            qid = str(i).zfill(2)
            print(f"############# Testing Whatif on {qid} ###########")
            query_file = f"queries/tpch/tpch_{qid}.sql"
            with open(query_file, "r") as f:
                sql = " ".join(f.read().split())
            # Printing lineage that was captured from base query
            print(con.execute(sql, capture_lineage='lineage').df())
            query_id = con.query_id
            print("=================", query_id, qid, use_duckdb, is_scalar)
            # use_duckdb = false, is_scalr = true/false, batch = 4
            batch = 4
            num_threads = 4
            q = f"pragma WhatIf({query_id}, 'd', 'lineitem:0.3', 1024, {batch}, {is_scalar}, {use_duckdb}, {num_threads});"
            con.execute(q)
            tables = con.execute("PRAGMA show_tables").fetchdf()
            for index, row in tables.iterrows():
                if row["name"][:7] == "LINEAGE":
                    con.execute("DROP TABLE "+row["name"])
            con.execute("PRAGMA clear_lineage")
