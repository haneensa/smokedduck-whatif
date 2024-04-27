import sys
import duckdb
import pandas as pd
from pygg import *
from utils import get_data
from utils import legend, legend_bottom, legend_side

pd.set_option('display.max_rows', None)

con = duckdb.connect(':default:')

lineage_size = get_data(f"lineage_size.csv", 1000)
print(lineage_size)
print(con.execute("""select
avg(lineage_count) as avg_count, avg(lineage_count_prune) as avg_count_prune,
max(lineage_count) as max_count, max(lineage_count_prune) as max_count_prune,
min(lineage_count) as min_count, min(lineage_count_prune) as min_count_prune,
avg(lineage_size_mb) as avg_mb, avg(lineage_size_mb_prune) as avg_mb_prune,
max(lineage_size_mb) as max_mb, max(lineage_size_mb_prune) as max_mb_prune,
min(lineage_size_mb) as min_mb, min(lineage_size_mb_prune) as min_mb_prune
from lineage_size""").df())

lineage_size = get_data(f"fade_data/scale_sf_april26.csv", 1000)
print(lineage_size)
print(con.execute("""select sf, qid, prune,
avg(lineage_count) as avg_count, avg(lineage_count_prune) as avg_count_prune,
max(lineage_count) as max_count, max(lineage_count_prune) as max_count_prune,
min(lineage_count) as min_count, min(lineage_count_prune) as min_count_prune,
avg(lineage_size_mb) as avg_mb, avg(lineage_size_mb_prune) as avg_mb_prune,
max(lineage_size_mb) as max_mb, max(lineage_size_mb_prune) as max_mb_prune,
min(lineage_size_mb) as min_mb, min(lineage_size_mb_prune) as min_mb_prune
from lineage_size group by sf, qid, prune""").df())

print(con.execute("""select sf, prune,
avg(lineage_count) as avg_count, avg(lineage_count_prune) as avg_count_prune,
max(lineage_count) as max_count, max(lineage_count_prune) as max_count_prune,
min(lineage_count) as min_count, min(lineage_count_prune) as min_count_prune,
avg(lineage_size_mb) as avg_mb, avg(lineage_size_mb_prune) as avg_mb_prune,
max(lineage_size_mb) as max_mb, max(lineage_size_mb_prune) as max_mb_prune,
min(lineage_size_mb) as min_mb, min(lineage_size_mb_prune) as min_mb_prune
from lineage_size group by sf, prune""").df())
