import numpy as np
import time
import random
import argparse
import smokedduck
import pandas as pd

def augment_intervention_attr(db, table: str, n_distinct: int) -> int:
    # read table as df
    base_table = db.execute(f"select * from {table}").df()
    
    # generate n number of distinct value 
    random_values = random.choices(range(n_distinct), k=len(base_table)) 

    # Create a DataFrame with a column containing the random values
    distinct_col = pd.DataFrame({f"fade_col_{n_distinct}": random_values})
    merged_df = pd.concat([base_table, distinct_col], axis=1)

    db.execute(f"drop table {table}")
    db.execute(f"create table {table}  as select * from merged_df")
    return len(base_table)


parser = argparse.ArgumentParser()
parser.add_argument("--sf", help="sf", type=float, default=1)
parser.add_argument("--gen_distinct", help="", type=bool, default=False)
parser.add_argument("--use_attrs", help="table_name.column_name | ..", type=str, default="")
args = parser.parse_args()

con = smokedduck.connect('db.out')
con.execute(f'CALL dbgen(sf={args.sf});')

if (args.gen_distinct):
    # TODO: FOR ALL TABLES
    table='lineitem'
    for distinct in [2048]: # , 1024, 2048, 2560]:
        print("gen ", distinct)
        augment_intervention_attr(con, table, distinct)

def parse_string(input_str):
    result = {}
    segments = input_str.split('|')
    for segment in segments:
        if segment.strip():  # Skip empty segments
            key, value = segment.split('.')
            result[key.strip()] = value.strip()
    return result


# hack: ideally this is done inside duckdb
if len(args.use_attrs) > 0:
    tspecs = parse_string(args.use_attrs)
    for table, col in tspecs.items():
        start = time.time()
        #vals = con.execute(f"select {col} from {table}").df()
        #codes, uvals = pd.factorize(vals[col])
        codes_table = con.execute(f"""select ROW_NUMBER() OVER (ORDER BY [{col}]) as code,  {col} as  codename
            from (select distinct {col} from {table}) as u""").df()
        q = f"""select d.code-1 as code from {table} as t JOIN codes_table as d ON (d.codename=t.{col}) ORDER BY t.rowid"""
        codes = con.execute(q).df()
        end = time.time()
        print(f"took {table} {col} {end-start} s")
        # write it to desk using table_name.column_name as filename; at runtime, we expect all columns that a user can intervene on has pre-prepared code file for them
        print(codes_table)
        filename = f"{table}_{col}.npy"
        codes.to_numpy().astype(np.uint32).tofile(filename)
        filename_vals = f"{table}_{col}_vals.csv"
        codes_table.to_csv(filename_vals, index=False)

        with open(f"{table}_{col}.rows", 'w') as file:
            file.write(str(len(codes)) + " " + str((len(codes_table))))

