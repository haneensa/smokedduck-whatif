import duckdb
import pandas as pd
from pygg import *
from utils import get_data
from utils import legend, legend_bottom, legend_side

postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
    """

pd.set_option('display.max_rows', None)

dbt_prob = 0.1
con = duckdb.connect(':default:')
plot = True
print_summary = True
include_dbt = True
dense = True
single = True
plot_scale = False
include_incremental_random = True

# contains overhead of original query without lineage capture, then with, then with intermediates
lineage_data = pd.read_csv('fade_data/lineage_overhead.csv')
lineage_data["query"] = "Q"+lineage_data["qid"].astype(str)
lineage_data["sf_label"] = "SF="+lineage_data["sf"].astype(str)

# benchmark.sh set: lineiten.i
#data_spec = pd.read_csv('search_dense.csv')
data_spec = get_data('dense_predicate_search_april27.csv', 1000)
data_spec = con.execute("""select sf, prune, prune_time*1000 as prune_time_ms, eval_time_ms, query, qid, n, incremental, num_threads, is_scalar  from data_spec
where n=2048
UNION ALL select sf, prune,  prune_time*1000 as prune_time_ms, (2048.0/n)*eval_time_ms as eval_time_ms, query, qid, 2048 as n, incremental, num_threads, is_scalar  from data_spec
where query='Q1'
UNION ALL select sf, prune,  prune_time*1000 as prune_time_ms, (2048.0/n)*eval_time_ms as eval_time_ms, query, qid, 2048 as n, incremental, num_threads, is_scalar  from data_spec
where sf>1 and prune='False'
""").df()
# predicate search
#data_search = pd.read_csv('search_m2.csv')
#data_search = get_data('predicate_search_april27.csv', 1000)
#data_search = get_data('search_april28.csv', 1000)
data_search = get_data('search_april29.csv', 1000)
data_search = con.execute("""select sf,  prune, post_time, gen_time, code_gen_time, data_time, prep_time, lineage_time, prune_time*1000 as prune_time_ms, eval_time_ms, query, qid, n, incremental, num_threads, is_scalar from data_search""").df()
data_search["post_time"] *= 1000
data_search["gen_time"] *= 1000
data_search["code_gen_time"] *= 1000
data_search["data_time"] *= 1000
data_search["prep_time"] *= 1000
data_search["lineage_time"] *= 1000
def summary_time(table, attrs):
    print(table, attrs)
    print(con.execute(f"""select {attrs},
    avg(post_time) as avg_post,
    max(post_time) as max_post,
    avg(gen_time) as avg_gen,
    max(gen_time) as max_gen,
    avg(prep_time) as avg_prep,
    max(prep_time) as max_prep,
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
summary_time("data_search", "sf, incremental")
summary_time("data_search", "sf, qid, incremental")

# from: benchmark_cube.sh
#data_cube = pd.read_csv('fade_data/cube_search.csv')
data_cube = pd.read_csv('cube_april27.csv')
data_cube["eval_time_ms"]=1000*data_cube["timing"]
data_cube["query"] = "Q"+data_cube["qid"].astype(str)
data_cube["n"] = data_cube["distinct"]
data_cube = con.execute("""select 0 as prune_time_ms, sf, eval_time_ms, query, qid, n, num_threads  from data_cube where num_threads=8""").df()

data_spec["sys"] = "FaDE"
data_search["sys"] = "FaDE-Sparse"
data_cube["sys"] = "GB"

data_spec["itype"] = "DD"
data_search["itype"] = "S"
data_cube["itype"] = "GB"
data = pd.concat([data_spec, data_search], axis=0)

data["sys"] = data.apply(lambda row: row["sys"]+"-Inc" if row["incremental"] and row["itype"] == "S" else row["sys"] , axis=1)
#data["sys"] = data.apply(lambda row: row["sys"]+"-p" if row["prune"] else row["sys"] , axis=1)


data["cat"] = data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
data["cat"] = data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
data["cat"] = data.apply(lambda row: row["cat"] + "+P" if row["prune"]  else row["cat"] , axis=1)
data["cat"] = data.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)
    
data_cube["cat"] = data_cube.apply(lambda row: row["itype"] + " " + str(row["num_threads"]) + "W", axis=1)

if plot:
    # TODO: add baseline
    plot_data = con.execute("""
    select prune, sys, cat, query, prune_time_ms, eval_time_ms, itype, n, sf, num_threads, n/(eval_time_ms/1000.0) as throughput
        from data where n>1 and itype='S' and incremental='True' and is_scalar='True'
    UNION ALL
    select prune, sys , cat, query, prune_time_ms, eval_time_ms, itype, n, sf, num_threads, n/(eval_time_ms/1000.0) as throughput
        from data where n>1 and itype='S' and incremental='False'
    UNION ALL
    select prune, sys, cat, query, prune_time_ms, eval_time_ms, itype, n, sf, num_threads, n/(eval_time_ms/1000.0) as throughput

        from data where n>1 and itype='DD' and incremental='False'
    UNION ALL
    select 'x' as prune, sys, cat, query, prune_time_ms, eval_time_ms, itype,n, sf, num_threads, n/(eval_time_ms/1000.0) as throughput from data_cube 
            """).df()
    plot_data_best = con.execute("""select itype, sys, query, min(eval_time_ms) as eval_time_ms,
                                min(eval_time_ms+prune_time_ms) as eval_prune_time_ms,
                                avg(prune_time_ms) as prune_time_ms, n, sf
                                from plot_data where n=2048 group by itype, sys, n, sf, query order by sys, query, n, sf
                                """).df()
    cat = "sys"
    print(plot_data_best)
    p = ggplot(plot_data_best, aes(x='query',  y="eval_prune_time_ms", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.9)
    p += axis_labels('Query', "Run time (ms, log)", "discrete", "log10")
    p += legend_bottom
    p += facet_grid(".~sf", scales=esc("free_y"))
    ggsave("figures/fade_search_fade_2048.png", p, postfix=postfix, width=10, height=4, scale=0.8)
    
    cat = "sys"
    plot_data_best_sf10 = con.execute("""select *, 'sf='||sf as sf_label  from plot_data_best where sf=10""").df()
    p = ggplot(plot_data_best_sf10, aes(x='query',  y="eval_prune_time_ms", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.9)
    p += axis_labels('Query', "Run time (ms, log)", "discrete", "log10")
    p += legend_bottom
    p += facet_grid(".~sf_label", scales=esc("free_y"))
    ggsave("figures/fade_search_fade_sf10.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)


    # todo: add baseline
    # read lineage_overhead.csv, get tpch runtimes without lineage
    lineage_data = pd.read_csv('fade_data/lineage_overhead_all_april30_v2.csv')
    lineage_data["query"] = "Q"+lineage_data["qid"].astype(str)
    """
            UNION ALL
            select 'sf='||sf as sf_label, t1.sys, sf, n, query, max(base.eval_time_ms / t1.eval_prune_time_ms) as speedup,
            min(t1.eval_time_ms) as sys_eval, min(base.eval_time_ms) as base_eval,
            max(n/(t1.eval_prune_time_ms/1000.0)) as throughput,
            avg(t1.prune_time_ms) as prune_eval, 
            from (select * from plot_data_best where  itype='GB') as t1 JOIN
            (select * from plot_data_best where itype='GB') as base using (sf, n, query)
            group by sf, n, query, t1.sys
    """
    plot_data = con.execute("""
            select 'sf='||sf as sf_label, t1.sys, sf, n, query, max(base.eval_time_ms / t1.eval_prune_time_ms) as speedup,
            min(t1.eval_time_ms) as sys_eval, min(base.eval_time_ms) as base_eval,
            max(n/(t1.eval_prune_time_ms/1000.0)) as throughput,
            avg(t1.prune_time_ms) as prune_eval, 
            from (select * from plot_data_best where  itype<>'GB') as t1 JOIN
            (select * from plot_data_best where itype='GB') as base using (sf, n, query)
            group by sf, n, query, t1.sys
            UNION ALL
            select 'sf='||sf as sf_label, 'Baseline' as sys, sf, 1, query, 0 speedup, 0 sys_eval, 0 base_eval,
            max(1/query_timing) as throughput, 0 as prune_eval
            from lineage_data
            where workload='tpch'
            group by sf, query, workload
            """).df()
    print("check")
    print(plot_data)
    p = ggplot(plot_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
    p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
    p += legend_bottom
    p += facet_grid(".~sf_label", scales=esc("free_y"))
    ggsave("figures/fade_search_fade_2048_speedup.png", p,  width=8, height=2.5, scale=0.8)
    
    sf10_plot_data = con.execute("""select 'sf='||sf as sf_label,*  from plot_data where sf=10""").df()
    print("check")
    print(sf10_plot_data)
    p = ggplot(sf10_plot_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
    p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
    p += legend_side
    ggsave("figures/fade_search_fade_sf10_2048_speedup.png", p,  postfix=postfix,width=7, height=2.5, scale=0.8)
    
    p = ggplot(sf10_plot_data, aes(x='query',  y="throughput", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
    p += axis_labels('Query', "Interventions / Sec (log)", "discrete", "log10",
                ykwargs=dict(breaks=[10,1000,100000, 1000000],  labels=list(map(esc,['10', '1K', '100K', '1M']))))
    p += legend_side
    ggsave("figures/fade_search_fade_sf10_2048_throughput.png", p,  postfix=postfix,width=7, height=2.5, scale=0.8)
    
    
    summary = con.execute("""select sf, sys,
    avg(speedup) as as, max(speedup) maxs, min(speedup) mins,
    avg(sys_eval) asys, 
    avg(base_eval) abase,
    avg(throughput),
    max(throughput)
    from plot_data
    group by sf, sys
    order by sf, sys
    """).df()
    print(summary)
    summary = con.execute("""select sf, sys,
    avg(speedup) as as, max(speedup) maxs, min(speedup) mins,
    avg(sys_eval) asys,
    avg(base_eval) abase, 
    avg(throughput),
    max(throughput)
    from plot_data
    where query<>'Q1'
    group by sf, sys
    order by sf, sys
    """).df()
    print(summary)
