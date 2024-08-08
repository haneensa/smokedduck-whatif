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

fade_single = get_data(f"fade_all_queries_a5.csv", 1000)
lineage_data = con.execute("""select sf, qid, prune, use_gb_backward_lineage, avg(post_time*1000) post_time,
                            avg(gen_time*1000) gen_time,
                            avg(prep_time*1000) prep_time,
                            avg(prune_time*1000) prune_time,
                            avg(data_time*1000) data_time,
                            avg(code_gen_time*1000) code_gen_time,
                            avg(lineage_time*1000) lineage_time,
                            avg(lineage_count) lineage_count,
                            avg(lineage_count_prune) lineage_count_prune,
                            avg(lineage_size_mb) lineage_size_mb,
                            avg(lineage_size_mb_prune) lineage_size_mb_prune
                            from fade_single where n=1
                            and itype='DENSE_DELETE'
                            and num_threads=1 and is_scalar='True'
                            group by sf, qid, prune, use_gb_backward_lineage
                            order by sf, qid
                            """).df()
print(lineage_data)
summary_size("lineage_data", "sf")
summary_size("lineage_data", "sf, qid, prune")

print("Provenance on SF=1 is on average XMB (x − xMB) and YMB (y − yMB) after pruning;")

summary_size("lineage_data", "sf, prune")

summary_time("lineage_data", "sf, qid, use_gb_backward_lineage")
summary_time("lineage_data", "sf, use_gb_backward_lineage")

lineage_data = pd.read_csv('fade_data/lineage_overhead.csv')
lineage_data = pd.read_csv('fade_data/lineage_overhead_all_april30_v2.csv')
lineage_data["query"] = "Q"+lineage_data["qid"].astype(str)
lineage_data["sf_label"] = "SF="+lineage_data["sf"].astype(str)
print("======== Lineage Overheaad Summary =============")
#summary_data = con.execute("""
#select qid, sf, avg(query_timing) as qtiming, avg(lineage_timing) as ltiming,
#avg(dense_data.ksemimodule_timing) as ktiming,
#avg(lineage_timing-query_timing) as lineage_overhead,
#avg(dense_data.ksemimodule_timing-query_timing) as ksemimodule_overhead,
#from lineage_data JOIN dense_data USING (qid, sf)
#group by qid, sf
#order by qid, sf
#""").df()
#print(summary_data)
def lineage_details(attrs):
    print(con.execute(f"""select {attrs}, avg(query_timing), avg(lineage_timing),
        avg(lineage_timing/query_timing) slowdown,
        avg( 1000*(lineage_timing-query_timing)) diff_ms,
        avg( ((lineage_timing-query_timing) / query_timing)*100 ) avg_rover,
        max( ((lineage_timing-query_timing) / query_timing)*100 ) max_rover,
        min( ((lineage_timing-query_timing) / query_timing)*100 ) min_rover
    from lineage_data group by {attrs}
    order by {attrs}""").df())
lineage_details("sf, query, model, workload")
lineage_details("sf, model, workload")
lineage_details("model, workload")

# TODO: get how much extra overhead to cache values
