import numpy as np
import time
import csv
import smokedduck
import pandas as pd

con = smokedduck.connect('intel.db')
create_sql = """CREATE TABLE intel (
    date date,
    tstamp time without time zone,
    epoch integer,
    moteid integer,
    temp double precision,
    humidity double precision,
    light double precision,
    voltage double precision,
    id integer NOT NULL,
    hr timestamp without time zone
);"""

con.execute(create_sql)

con.execute("COPY intel FROM 'intel.csv' WITH (HEADER true, DELIMITER '\t', nullstr '\\N');")
print(con.execute("select * from intel").df())

specs = "intel.moteid|intel.temp|intel.voltage"
specs_tokens = specs.split('|')
cols = []
for token in specs_tokens:
    table_col = token.split('.')
    print(table_col)
    table = table_col[0]
    col = table_col[1]
    cols.append(col)
    #vals = con.execute(f"select {col} from {table}").df()
    #codes, uvals = pd.factorize(vals[col])
    start = time.time()
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


