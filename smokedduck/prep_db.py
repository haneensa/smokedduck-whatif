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
args = parser.parse_args()

con = smokedduck.connect('db.out')
con.execute(f'CALL dbgen(sf={args.sf});')

if (args.gen_distinct):
    # TODO: FOR ALL TABLES
    table='lineitem'
    for distinct in [2048]: # , 1024, 2048, 2560]:
        print("gen ", distinct)
        augment_intervention_attr(con, table, distinct)

