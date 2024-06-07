import argparse
import numpy as np
import time
import csv
import smokedduck
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--specs", help="|table.col", type=str, default="readings.moteid|readings.temp|readings.voltage")
args = parser.parse_args()
specs = args.specs

with smokedduck.connect("intel.db") as con:
    con.execute("drop table if exists readings")
    #create_sql = """CREATE TABLE readings as SELECT * FROM 'intel.csv'"""
    create_sql = """CREATE TABLE readings (
        date date,
        tstamp time without time zone,
        epoch integer,
        moteid integer,
        temp float,
        humidity float,
        light float,
        voltage float,
        id integer NOT NULL,
        hr timestamp without time zone
    );"""

    con.execute(create_sql)

    con.execute("COPY readings FROM 'intel.csv' WITH (HEADER true, DELIMITER '\t', nullstr '\\N');")
    print(con.execute("select * from readings").df())


    # hack since we don't have guards for null values
    con.execute("""UPDATE readings SET temp = COALESCE(temp, 0);""")
    con.execute("""UPDATE readings SET light = COALESCE(light, 0);""")
    con.execute("""UPDATE readings SET voltage = COALESCE(voltage, 0);""")
    con.execute("""UPDATE readings SET humidity = COALESCE(humidity, 0);""")

    con.execute("""UPDATE readings SET moteid = COALESCE(moteid, -1);""")
    print(con.execute("select * from readings").df())

    specs_tokens = specs.split('|')
    cols = []
    for token in specs_tokens:
        table_col = token.split('.')
        print(table_col)
        table = table_col[0]
        col = table_col[1]
        cols.append(col)
        start = time.time()
        vals = con.execute(f"select {col} from {table}").df()
        codes, uvals = pd.factorize(vals[col])
        print(codes)
        print(uvals)
        #codes_table = con.execute(f"""select ROW_NUMBER() OVER (ORDER BY [{col}]) as code,  {col} as  codename
        #    from (select distinct {col} from {table}) as u""").df()
        #print(codes_table)
        #q = f"""select d.code-1 as code from {table} as t OUTER JOIN codes_table as d ON (d.codename=t.{col}) ORDER BY t.rowid"""
        #codes = con.execute(q).df()
        end = time.time()
        print(f"took {table} {col} {end-start} s")
        # write it to desk using table_name.column_name as filename; at runtime, we expect all columns that a user can intervene on has pre-prepared code file for them
        filename = f"{table}_{col}.npy"
        #codes.to_numpy().astype(np.uint32).tofile(filename)
        codes.astype(np.uint32).tofile(filename)
        filename_vals = f"{table}_{col}_vals.csv"
        unique_values_df = pd.DataFrame(uvals)
        unique_values_df.to_csv(filename_vals, header=False, index=False)

        with open(f"{table}_{col}.rows", 'w') as file:
            file.write(str(len(codes)) + " " + str((len(uvals))))


