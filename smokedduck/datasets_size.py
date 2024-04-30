import pandas as pd
import smokedduck

con = smokedduck.connect('flights.db')
if 1:
    con.execute('pragma threads=16')
    csvfile = '/home/haneenmo/airline.csv.shuffle'
    try:
        data = pd.read_csv(csvfile, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            data = pd.read_csv(csvfile, encoding='latin1')
        except UnicodeDecodeError:
            data = pd.read_csv(csvfile, encoding='ISO-8859-1')

    con.execute(f"create table flights as (select * from data)")
