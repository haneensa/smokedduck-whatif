import sys
import duckdb
import pandas as pd
from pygg import *
from utils import get_data
from utils import legend, legend_bottom, legend_side

pd.set_option('display.max_rows', None)

con = duckdb.connect(':default:')
def summary_size(table, attrs):
    print(table, attrs)
    print(con.execute(f"""select {attrs},
    avg(lineage_count) as avg_count, avg(lineage_count_prune) as avg_count_prune,
    max(lineage_count) as max_count, max(lineage_count_prune) as max_count_prune,
    min(lineage_count) as min_count, min(lineage_count_prune) as min_count_prune,
    avg(lineage_size_mb) as avg_mb, avg(lineage_size_mb_prune) as avg_mb_prune,
    max(lineage_size_mb) as max_mb, max(lineage_size_mb_prune) as max_mb_prune,
    min(lineage_size_mb) as min_mb, min(lineage_size_mb_prune) as min_mb_prune
    from {table}
    group by {attrs}""").df())
def summary_time(table, attrs):
    print(table, attrs)
    print(con.execute(f"""select {attrs},
    avg(post_time) as avg_post,
    max(post_time) as max_post,
    avg(gen_time) as avg_gen,
    max(gen_time) as max_gen,
    avg(prep_time) as avg_prep,
    max(prep_time) as max_prep,
    avg(prune_time) as avg_prune,
    max(prune_time) as max_prune,
    avg(data_time) as avg_data,
    max(data_time) as max_data,
    avg(code_gen_time) as avg_code,
    max(code_gen_time) as max_code,
    avg(lineage_time) as avg_lineage,
    min(lineage_time) as min_lineage,
    max(lineage_time) as max_lineage,
    from {table}
    group by {attrs}
    order by {attrs}""").df())

lineage_size = get_data(f"lineage_size.csv", 1000)
print(lineage_size)
summary_size("lineage_size", "sf")

lineage_size = get_data(f"fade_data/scale_sf_april26.csv", 1000)
print(lineage_size)
summary_size("lineage_size", "sf, qid, prune")
summary_size("lineage_size", "sf, prune")

#lineage_time = get_data(f"lineage_postprocess_april28.csv", 1000)
lineage_time = get_data(f"testlineage_postprocess_april28.csv", 1000)
print(lineage_time)
lineage_time["post_time"] *= 1000
lineage_time["gen_time"] *= 1000
lineage_time["code_gen_time"] *= 1000
lineage_time["data_time"] *= 1000
lineage_time["prep_time"] *= 1000
lineage_time["prune_time"] *= 1000
lineage_time["lineage_time"] *= 1000
summary_time("lineage_time", "sf, qid, use_gb_backward_lineage")
summary_time("lineage_time", "sf, use_gb_backward_lineage")
