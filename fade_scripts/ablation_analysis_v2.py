
import sys
import duckdb
import pandas as pd
from pygg import *
from utils import get_data
from utils import legend, legend_bottom, legend_side

pd.set_option('display.max_rows', None)

lines = []
prob = 0.1
con = duckdb.connect(':default:')
plot = True
print_summary = True
plot_scale = False

batching =  True
pruning = True
vec = True
workers = True
best = True
best_distinct = True

d_prefix = "DELETE_"

fade_data = get_data(f"fade_all_queries_a5.csv", 1000)
fade_data["spec"] = fade_data.apply(lambda row: '' if type(row["spec"]) is not str else row["spec"], axis=1)
dense_fade = con.execute("select * from fade_data where use_gb_backward_lineage='False' and itype='DENSE_DELETE' ").df()
dense_fade = dense_fade[dense_fade['spec']!='lineitem.i']

postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5','Q6',  'Q7', 'Q8', 'Q9', 'Q10', 'Q12', 'Q14', 'Q19'))
"""

prefix = f"ALL_{d_prefix}"
use_sample = True
exclude_sample = False #True #False

if use_sample:
    prefix = f"SAMPLE_{d_prefix}"
    selected_queries = "query IN  ('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12') "
else:
    selected_queries = " true "

if exclude_sample:
    use_sample = False
    prefix = f"EXTRA_{d_prefix}"
    selected_queries = "query IN  ('Q6', 'Q8', 'Q14', 'Q19') "

dense_fade = con.execute(f"select * from dense_fade where {selected_queries}").df()

def summary_custom(table, groups, aggs, whr=''):
    print(table, groups, aggs, whr)
    lines.append(f"{table}, {groups}, {whr}")
    return con.execute(f"""select {groups}, {aggs}
    from {table} {whr} group by {groups} order by {groups}""").df()

if best:
    print("--->======== DENSE best =============")
    # combined optimization wins
    # t1: best setting 8 workers, vectorized, pruned
    # base: single, 1 worker, scalar, pruned
    best_data_all = con.execute(f"""select sf, prob, t1.prune_label, t1.query, t1.cat,qid, t1.n, t1.num_threads, t1.is_scalar,
                        avg(t1.eval_time_ms) as eval_time_ms,
                        avg(base.eval_time_ms) as base_eval_time_ms,
                        avg(base.eval_time_ms*t1.n) as base_batching,
                        avg((t1.n*base.eval_time_ms) / (t1.eval_time_ms+t1.prune_time_ms)) as speedup_setup,
                        avg((t1.n*base.eval_time_ms) / (t1.eval_time_ms)) as speedup,
                        avg(t1.n / (t1.eval_time)) as throughput
    from ( select * from dense_fade where num_threads=8 and is_scalar='False' and prune='True') as t1 JOIN
         ( select * from dense_fade where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1 and prune='True') as base
         USING (sf, qid, prob)
    group by sf, t1.prune_label,t1. query, t1.cat, qid, t1.n, t1.num_threads, t1.is_scalar, prob
         """).df()
    
    cat = 'n'
    best_data  = con.execute(f"select * from best_data_all where prob={prob}").df()
    cat = "query"
    p = ggplot(best_data, aes(x='n',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_point(stat=esc('identity'))
    p += geom_line()
    p += axis_labels('n', "Latency (ms, log)", "log10", "log10",
            ykwargs=dict(breaks=[1,10,100, 1000],  labels=list(map(esc,['1','10','100', '1000']))),
            xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),)
    p += legend_side
    ggsave(f"figures/{prefix}_1_FaDE_B_W8_D_P_latency_sf1_line.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

    p = ggplot(best_data, aes(x='n',  y="speedup", color=cat, fill=cat, group=cat))
    p += geom_point(stat=esc('identity'))
    p += geom_line()
    p += axis_labels('n', "Speedup", "log10", "log10",
            xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),)
    p += legend_side
    ggsave(f"figures/{prefix}_2_FaDE_B_W8_D_P_speedup_sf1_line.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
    
    p = ggplot(best_data, aes(x='n',  y="speedup_setup", color=cat, fill=cat, group=cat))
    p += geom_point(stat=esc('identity'))
    p += geom_line()
    p += axis_labels('n', "Speedup", "log10", "log10",
            xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),)
    p += legend_side
    ggsave(f"figures/{prefix}_3_FaDE_B_W8_D_P_speedup_with_pcost_sf1_line.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

    # x-axis: batch size, y-axis: throughput
    p = ggplot(best_data, aes(x='n',  y="throughput", color=cat, fill=cat, group=cat))
    p += geom_point(stat=esc('identity'))
    p += geom_line()
    p += axis_labels('n', "Interventions / Sec (log)", "log10", "log10",
            ykwargs=dict(breaks=[10000,100000,1000000],  labels=list(map(esc,['10e4','10e5','10e6']))),
            xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),)
    p += legend_side
    ggsave(f"figures/{prefix}_4_FaDE_B_W8_D_P_throughput_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

    lines.append("======== DENSE Best =============")
    # average / max / min latency across all queries
    lines.append("FaDE_B_W8_D_P vs FaDE_W1_P")

    out = summary_custom('best_data_all', 'sf', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """)
    lines.append(out.to_string(index=False))
    
    out = summary_custom('best_data_all', 'sf, qid', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """)
    lines.append(out.to_string(index=False))
    
    out = summary_custom('best_data_all', 'sf, n', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """)
    lines.append(out.to_string(index=False))
    
    out = summary_custom('best_data_all', 'sf, qid', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """, "where n=2048")
    lines.append(out.to_string(index=False))
    
    out = summary_custom('best_data_all', 'sf', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """, "where n=2048")
    lines.append(out.to_string(index=False))
    
    out = summary_custom('best_data_all', 'sf', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """, "where query<>'Q1' and n=2048")
    lines.append(out.to_string(index=False))
    

    
    # for each batch, select the best runtime across (SIMD, num_threads, pruning)
    data = con.execute("""select sf, qid, query, prob, n,  min(eval_time) as eval_time, 
    min(eval_time_ms) as eval_time_ms, min(prune_time_ms) as prune_time_ms
    from dense_fade where n>1 group by sf, qid, query, prob, n""").df()
    # across all batch sizes, select if it is better to batch or not
    best_best_data_all = con.execute(f"""select sf, prob, t1.query, qid, t1.n,
                        t1.eval_time_ms eval_time_ms, base.eval_time_ms base_eval_time_ms, base.eval_time_ms*t1.n,
                        (t1.n*base.eval_time_ms) / (t1.eval_time_ms+t1.prune_time_ms) as speedup_setup,
                        (t1.n*base.eval_time_ms) / (t1.eval_time_ms) as speedup,
                        t1.n / (t1.eval_time) as throughput
    from ( select * from data) as t1 JOIN
         ( select * from dense_fade where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1 and prune='True') as base
         USING (sf, qid, prob) where prob={prob} 
         """).df()
    cat = "query"
    p = ggplot(best_best_data_all, aes(x='n',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_point(stat=esc('identity'))
    p += geom_line()
    p += axis_labels('n', "Latency (ms, log)", "log10", "log10",
            ykwargs=dict(breaks=[1,10,100, 1000],  labels=list(map(esc,['1','10','100', '1000']))),
            xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),)
    p += legend_side
    ggsave(f"figures/{prefix}_5_FaDE_opt_latency_sf1_line.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

    p = ggplot(best_best_data_all, aes(x='n',  y="throughput", color=cat, fill=cat, group=cat))
    p += geom_point(stat=esc('identity'))
    p += geom_line()
    p += axis_labels('n', "Interventions / Sec (log)", "log10", "log10",
            ykwargs=dict(breaks=[10000,100000,1000000],  labels=list(map(esc,['10e4','10e5','10e6']))),
            xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),)
    p += legend_side
    ggsave(f"figures/{prefix}_6_FaDE_opt_throughput_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
    # average / max / min latency across all queries
    lines.append("FaDE_opt vs FaDE_W1_P")

    out = summary_custom('best_best_data_all', 'sf', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """)
    lines.append(out.to_string(index=False))
    
    out = summary_custom('best_best_data_all', 'sf, qid', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """)
    lines.append(out.to_string(index=False))
    out = summary_custom('best_best_data_all', 'sf, qid, n', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
    avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
    avg(throughput), max(throughput)
    """)
    lines.append(out.to_string(index=False))
    print(lines)
    


level1_data = con.execute(f"""select sf, qid, prune, prob, t1.query, t1.cat, t1.n, t1.num_threads,
    t1.is_scalar, t1.eval_time_ms, t1.gen_time * 1000 as gen_time_ms, base.eval_time_ms*t1.n as single_eval,
    (base.eval_time_ms * t1.n)/ t1.eval_time_ms as speedup, 
    (base.eval_time_ms * t1.n)/ (t1.eval_time_ms+ t1.prune_time_ms) as speedup_setup, 
    t1.prune_time_ms,
    (t1.n / (t1.eval_time)) as throughput
    from ( select * from dense_fade where incremental='False' and num_threads=1 and is_scalar='True' and sf=1) as t1 JOIN
         ( select * from dense_fade where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1) as base
         USING (sf, qid, prune, prob) where prob={prob} ORDER BY sf, qid, prune, prob
         """).df()
if batching:
    # Batching latency performance varying number of interventions
    print("======== DENSE Batching=============")
    level1_data["prune_label"] = level1_data.apply(lambda row:"FaDE-Prune" if row["prune"] else "FaDE" , axis=1)
    if plot:
        cat = 'query'
        # compare batch size of 1 to batch size of n to evaluate n interventions
        p = ggplot(level1_data, aes(x='n',  y="eval_time_ms", color=cat, fill=cat, linetype='prune_label'))
        p += geom_line(stat=esc('identity'))
        p += axis_labels('Batch Size (log)', "Latency (ms, log)", "log10", "log10",
                ykwargs=dict(breaks=[1,10,100, 1000],  labels=list(map(esc,['1','10','100', '1000']))),
                xkwargs=dict(breaks=[1,64,  512,2048],  labels=list(map(esc,['1','64',  '512','2K']))),
                )
        p += legend_side
        p += facet_grid(".~sf~prune_label", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_7_fade_batching_latency_sf1_line.png", p, postfix=postfix, width=5, height=2.5, scale=0.8)

        
        data = con.execute("select * from level1_data where n>1").df()
        p = ggplot(data, aes(x='n',  y="speedup", color=cat, fill=cat, linetype='prune_label'))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                xkwargs=dict(breaks=[64,  512,2048],  labels=list(map(esc,['64',  '512','2K']))),
                )
        p += legend_side
        p += facet_grid(".~sf~prune_label", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_8_fade_batching_speedup_sf1_line.png", p, postfix=postfix, width=5, height=3, scale=0.8)
    
        p = ggplot(data, aes(x='n',  y="throughput", color=cat, fill=cat, group=cat))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('n', "Interventions / Sec (log)", "log10", "log10",
                ykwargs=dict(breaks=[10000,100000,1000000],  labels=list(map(esc,['10e4','10e5','10e6']))),
                xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),)
        p += legend_side
        p += facet_grid(".~sf~prune_label", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_9_FaDE_batching_throughput_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        # average / max / min latency across all queries
        lines.append("========== Batching Summary ============")
        lines.append("FaDE_B_W1 vs FaDE_W1")
        out = summary_custom('level1_data', 'sf, prune_label', """avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms),
        avg(speedup), max(speedup), avg(speedup_setup), max(speedup_setup),
        avg(throughput), max(throughput)
        """, "where n > 1")
        lines.append(out.to_string(index=False))
        whr = "where n > 1 and prune_label='FaDE'"
        out = summary_custom("level1_data", "sf, n, prune",  """
        max(speedup), avg(speedup), min(speedup), 
        avg(eval_time_ms), avg(single_eval), max(eval_time_ms), max(single_eval),
        min(eval_time_ms), min(eval_time_ms) 
        """, whr)
        lines.append(out.to_string(index=False))
        out = summary_custom("level1_data", "sf, qid",  """
        max(speedup), avg(speedup), min(speedup), 
        avg(eval_time_ms), avg(single_eval), max(eval_time_ms), max(single_eval),
        min(eval_time_ms), min(eval_time_ms) 
        """, whr)
        lines.append(out.to_string(index=False))
        out = summary_custom("level1_data", "sf",  """
        max(speedup), avg(speedup), min(speedup), 
        avg(eval_time_ms), avg(single_eval), max(eval_time_ms), max(single_eval),
        min(eval_time_ms), min(eval_time_ms) 
        """, whr)
        lines.append("batching wins only: xyz")
        lines.append("batching add X average speedup (min, max)")
        lines.append(out.to_string(index=False))

if workers:
    print("======== DENSE Threads =============")
    lines.append("======== DENSE Threads =============")
    # Additional threading speedup for 2, 4 and 8 threads over batched execution
    threading_data = con.execute(f"""select sf, qid, prune, t1.prune_label, n, t1.sf_label, t1.query, t1.cat, t1.num_threads,
        t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms*n as single_eval
        from (select * from level1_data where prob={prob}) as base JOIN 
        (select * from dense_fade where is_scalar='True' and sf=1 and prob={prob}) as t1
         USING (sf, qid, n, prune)""").df()

    if plot:
        p = ggplot(threading_data, aes(x='num_threads',  y="eval_time_ms", color="query", fill="query", group="query"))
        p += geom_line(stat=esc('identity')) + geom_point(stat=esc('identity'))
        p += axis_labels('Query', "Latency (ms, log)", "continous", "log10")
        p += legend_side
        p += facet_grid(".~sf~prune~n", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_17_fade_threading_latency_sf1_line.png", p, postfix=postfix, width=10, height=8, scale=0.8)
        
        threading_data["prune_label"] = threading_data.apply(lambda row:"FaDE-Prune" if row["prune"] else "FaDE" , axis=1)
        p = ggplot(threading_data, aes(x='num_threads',  y="speedup", color="query", fill="query", linetype='prune_label'))
        p += geom_line(stat=esc('identity'))  + geom_point(stat=esc('identity'))
        p += axis_labels('# Threads', "Speedup")
        p += legend_side
        p += facet_grid(".~sf_label~n", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_18_fade_threading_speedup_sf1_line.png", p,postfix=postfix, width=10, height=4, scale=0.8)
        
        
        # average / max / min latency across all queries
        lines.append("Threading wins against FaDE_W1")
        out = summary_custom('threading_data', 'sf, num_threads, prune_label', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """, "where num_threads>1 and prune_label='FaDE'")
        lines.append(out.to_string(index=False))
        out = summary_custom('threading_data', 'sf, num_threads, prune_label', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """, "where num_threads>1 and prune_label='FaDE' and qid<>10 and n=2048")
        lines.append(out.to_string(index=False))
        out = summary_custom('threading_data', 'sf, num_threads, prune_label', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """, "where num_threads>1 and prune_label='FaDE' and qid=10 and n=2048")
        lines.append(out.to_string(index=False))
        out = summary_custom('threading_data', 'sf, num_threads, qid, prune_label', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """, "where num_threads>1 and prune_label='FaDE'")
        lines.append(out.to_string(index=False))
        out = summary_custom('threading_data', 'sf', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """, "where num_threads>1 and prune_label='FaDE'")
        lines.append(out.to_string(index=False))
        



if pruning:
    print("======== DENSE Pruning =============")
    prune_data = con.execute(f"""select sf, n, qid, prob, t1.query, t1.cat, t1.prune_time_ms,
        (base.eval_time_ms)/ (t1.eval_time_ms) as speedup_nosetup,
        (base.eval_time_ms)/ (t1.eval_time_ms+t1.prune_time_ms) as speedup,
        t1.eval_time_ms, base.eval_time_ms as base_eval
        from ( select * from level1_data where prune='True' ) as t1 JOIN
             ( select * from level1_data where prune='False') as base
             USING (sf, qid, n, prob) where prob={prob} ORDER BY sf, qid, n, prob""").df()

    prune_data_breakdown = con.execute(f"""
        select sf, n, prob, t1.query, qid,
        t1.prune_time_ms,
        'ALL' as cat,
        (base.eval_time_ms+base.gen_time_ms)/ (t1.eval_time_ms+t1.gen_time_ms) as speedup_nosetup,
        (base.eval_time_ms+base.gen_time_ms)/ (t1.eval_time_ms+t1.prune_time_ms+t1.gen_time_ms) as speedup,
        t1.eval_time_ms, base.eval_time_ms as base_eval
        from ( select * from level1_data where prune='True') as t1 JOIN
             ( select * from level1_data where prune='False') as base
             USING (sf, qid, n, prob) 
        where prob={prob}
        UNION ALL
        select sf, n, prob, t1.query, qid, 
        t1.prune_time_ms,
        'Gen' as cat,
        (base.gen_time_ms)/ (t1.gen_time_ms) as speedup_nosetup,
        (base.gen_time_ms)/ (t1.prune_time_ms+t1.gen_time_ms) as speedup,
        t1.eval_time_ms, base.eval_time_ms as base_eval
        from ( select * from level1_data where prune='True') as t1 JOIN
             ( select * from level1_data where prune='False') as base
             USING (sf, qid, n, prob) 
        where prob={prob}
        UNION ALL
        select sf, n, prob, t1.query, qid, 
        t1.prune_time_ms,
        'Eval' as cat,
        (base.eval_time_ms)/ (t1.eval_time_ms) as speedup_nosetup,
        (base.eval_time_ms)/ (t1.eval_time_ms+t1.prune_time_ms) as speedup,
        t1.eval_time_ms, base.eval_time_ms as base_eval
        from ( select * from level1_data where prune='True') as t1 JOIN
             ( select * from level1_data where prune='False') as base
             USING (sf, qid, n, prob) 
        where prob={prob}
        order by sf, qid, n
             """).df()

    if plot:
        lines.append("======== DENSE Pruning =============")
        
        p = ggplot(prune_data, aes(x='n',  y="speedup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity'))  + geom_point(stat=esc('identity'))
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,64,512,2048],  labels=list(map(esc,['1','64', '512', '2K']))),
            )
        p += legend_side
        ggsave(f"figures/{prefix}_11_fade_pruning_batch_wsetup.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        p = ggplot(prune_data, aes(x='n',  y="speedup_nosetup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),
            )
        p += legend_side
        ggsave(f"figures/{prefix}_12_fade_pruning_batch_nosetup.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        # average / max / min latency across all queries
        lines.append("FaDE_B vs FaDE_B_P speedup")
        out = summary_custom('prune_data', 'sf',
                """avg(speedup), max(speedup), min(speedup), avg(speedup_nosetup), max(speedup_nosetup), min(speedup_nosetup)""")
        lines.append(out.to_string(index=False))
        out = summary_custom('prune_data', 'sf',
                """avg(speedup), max(speedup), min(speedup), avg(speedup_nosetup), max(speedup_nosetup), min(speedup_nosetup)""",
                "where n=1")
        lines.append(out.to_string(index=False))
        out = summary_custom('prune_data', 'sf, qid', """avg(speedup), max(speedup), min(speedup), avg(speedup_nosetup), max(speedup_nosetup), min(speedup_nosetup)""")
        lines.append(out.to_string(index=False))
        
        prune_data_all = con.execute("select * from prune_data_breakdown where cat='ALL'").df()
        p = ggplot(prune_data_all, aes(x='n',  y="speedup", color="query", fill="query",  shape='cat', linetype='cat'))
        p += geom_line(stat=esc('identity'))  + geom_point(stat=esc('identity'))
        p += axis_labels('Batch Size', "Speedup (log)", "log10", "log10",
                xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
            )
        p += legend_side
        ggsave(f"figures/{prefix}_13_fade_pruning_batch_all.png", p, postfix=postfix, width=4, height=3, scale=0.8)
        
        # average / max / min latency across all queries
        lines.append("FaDE_B vs FaDE_B_P speedupe includes all overheads")
        out = summary_custom('prune_data_all', 'sf, qid', """avg(speedup), max(speedup), min(speedup)""")
        lines.append(out.to_string(index=False))

    prune_data_detailed = con.execute(f"""select sf, n, t1.query, t1.cat, qid,
        t1.prune_time_ms,is_scalar, num_threads,
        (base.eval_time_ms)/ t1.eval_time_ms as speedup_nosetup,
        (base.eval_time_ms)/ (t1.eval_time_ms+t1.prune_time_ms) as speedup,
        t1.eval_time_ms, base.eval_time_ms as base_eval
        from ( select * from dense_fade where incremental='False' and  sf=1 and prob={prob} and prune='True') as t1 JOIN
             ( select * from dense_fade where incremental='False' and  sf=1 and prob={prob} and prune='False') as base
             USING (sf, qid, n, is_scalar, num_threads) where n > 1 """).df()
    if plot:
        p = ggplot(prune_data_detailed, aes(x='n',  y="speedup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1', '64', '256', '512','1K','2K']))),
            )
        p += legend_side
        p += facet_grid(".~is_scalar~num_threads", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_14_fade_pruning_batch_wsetup_compined.png", p, postfix=postfix, width=8, height=5, scale=0.8)
        
        p = ggplot(prune_data_detailed, aes(x='n',  y="speedup_nosetup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,64, 256, 512,1024,2048],  labels=list(map(esc,['1', '64', '256', '512','1K','2K']))),
            )
        p += legend_side
        p += facet_grid(".~is_scalar~num_threads", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_15_fade_pruning_batch_nosetup_combined.png", p, postfix=postfix, width=8, height=5, scale=0.8)
        
        
        def summary_prune(table, attrs, whr=""):
            print(table, attrs)
            return con.execute(f"""select {attrs},
                                avg(speedup_nosetup), max(speedup_nosetup), min(speedup_nosetup),
                                avg(speedup), max(speedup), min(speedup)
                                from {table} {whr}
                                group by {attrs} order by {attrs}""").df()

        #lines.append("pruning speedup for evaluation only, intervention generation, combined")
        #out = summary_prune("prune_data_breakdown", "sf, n, cat")
        #lines.append(out.to_string(index=False))
        #lines.append("pruning speedup for batch size = 1:")
        #out = summary_prune("prune_data_breakdown", "sf,cat", "where n=1")
        #lines.append(out.to_string(index=False))
        lines.append("pruning speedup all batch size =1 for all probs:")
        out = summary_prune("prune_data_breakdown", "sf,cat,prob", "where n=1")
        lines.append(out.to_string(index=False))
        lines.append("Lineage pruning cost:")
        out = con.execute("""select 
        avg(prune_time_ms), max(prune_time_ms), min(prune_time_ms),
        from prune_data
        group by sf
        order by sf""").df()
        lines.append(out.to_string(index=False))
        
        out = con.execute("""select qid,
        avg(prune_time_ms), max(prune_time_ms), min(prune_time_ms),
        from prune_data
        group by sf, qid
        order by sf, qid""").df()
        lines.append(out.to_string(index=False))
        
        # average / max / min latency across all queries
        lines.append("FaDE pruning speedup as we vary number of workers and SIMD")
        out = summary_custom('prune_data_detailed', 'sf, is_scalar, num_threads', """avg(speedup), max(speedup), min(speedup),
                avg(speedup_nosetup), max(speedup_nosetup), min(speedup_nosetup)""")
        lines.append(out.to_string(index=False))
        lines.append("pruning speedup across all queries and batches:")
        out = summary_custom('prune_data', 'sf', """avg(speedup), max(speedup), min(speedup),
                avg(speedup_nosetup), max(speedup_nosetup), min(speedup_nosetup)""")
        lines.append(out.to_string(index=False))
    
if vec:
    print("======== DENSE Vec =============")
    lines.append("======== DENSE Vec =============")

    # Additional vectorization speedup over batched execution of  varying numbers of interventions
    vec_data = con.execute(f"""select sf, t1.sf_label, prune, qid, n, t1.prune_label, t1.query, t1.cat, t1.num_threads, prob,
        t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms*n as single_eval
        from (select * from level1_data) as base
        JOIN (select * from dense_fade where num_threads=1 and is_scalar='False' and sf=1) as t1
        USING (sf, qid, n, prune, prob) where prob={prob}""").df()
    vec_data["prune_label"] = vec_data.apply(lambda row:"FaDE-Prune" if row["prune"] else "FaDE" , axis=1)
    def summary_vec2(table, attrs):
        print(table, attrs)
        return con.execute(f"""select {attrs},
        avg(vec_data_prune.speedup), avg(vec_data_no.speedup),
        avg(vec_data_prune.speedup-vec_data_no.speedup) as avg_diff,
        max(vec_data_prune.speedup-vec_data_no.speedup) as max_diff,
        min(vec_data_prune.speedup-vec_data_no.speedup) as min_diff
        from (select * from {table} where prune='True') as vec_data_prune JOIN
        (select * from {table} where prune='False') as vec_data_no
        USING (sf, qid, n, prob)
        group by {attrs}
        order by {attrs}
        """).df()
    if plot:
        p = ggplot(vec_data, aes(x='n',  y="speedup", color="query", fill="query"))
        p += geom_line(stat=esc('identity'))  + geom_point(stat=esc('identity'))
        p += legend_side
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                xkwargs=dict(breaks=[1, 64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),
            )
        p += facet_grid(".~prune_label", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_16_fade_vec.png", p, postfix=postfix, width=6, height=2, scale=0.8)
        
        # average / max / min latency across all queries
        lines.append("SIMD wins against FaDE_W1")

        out = summary_custom('vec_data', 'sf, qid, prune_label', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """)
        lines.append(out.to_string(index=False))
        out = summary_custom('vec_data', 'sf, n, prune_label', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """)
        lines.append(out.to_string(index=False))
        out = summary_custom('vec_data', 'sf, prune_label', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """)
        lines.append(out.to_string(index=False))
        
        out = summary_vec2("vec_data", "qid, n, prob")
        lines.append(out.to_string(index=False))
        out = summary_vec2("vec_data", "qid")
        lines.append(out.to_string(index=False))
        out = summary_vec2("vec_data", "sf")
        lines.append(out.to_string(index=False))
        
        out = summary_custom('vec_data', 'sf', """avg(speedup), max(speedup), min(speedup),
                avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
                """)
        lines.append(out.to_string(index=False))
        


if best_distinct:
    # speedup avg, max, min
    print("======== DENSE Best Distinct =============")
    lines.append("======== DENSE Best Distinct =============")
    # x-axis: adding optimization one at a time
    # y-axis: speedup
    #dense_fade_iters = get_data(f"vec_sf1_may1.csv", 1000)
    #dense_fade_iters = con.execute("select * from dense_fade_iters where use_gb_backward_lineage='False'").df()
    #dense_fade_iters["cat"] = dense_fade_iters.apply(lambda row: row["cat"] + "+P" if row["prune"] else row["cat"], axis=1)
    #dense_fade_iters["cat"] = dense_fade_iters.apply(lambda row: row["cat"] + "+B="+ str(row["n"]), axis=1)
    
    dense_fade["cat"] = dense_fade.apply(lambda row: row["cat"] + "+P" if row["prune"] else row["cat"], axis=1)
    dense_fade["cat"] = dense_fade.apply(lambda row: row["cat"] + "+B="+ str(row["n"]), axis=1)
    #        select sf, qid, n, prob, sf_label, prune_label, prune, query, cat, num_threads, is_scalar, eval_time_ms,
    #        gen_time, prune_time_ms, eval_time, prune_time
    #        from dense_fade_iters 
    #        UNION ALL
    
    
    dense_fade_iters = con.execute("""
            select sf, qid, n, prob, sf_label, prune, prune_label, query, cat, num_threads, is_scalar,
            avg(eval_time_ms) eval_time_ms,
            avg(gen_time)*1000.0 gen_time_ms, avg(gen_time) as gen_time, avg(prune_time_ms) prune_time_ms,
            avg(eval_time) eval_time, avg(prune_time) prune_time, 
            from dense_fade
            group by sf, qid, n, prob, sf_label, prune, prune_label, query, cat, num_threads, is_scalar
            """).df()

    # 1 intervention 1 worker.   batch, 1 worker.  batch 8 worker.   batch 8 worker + simd.   batch 8 worker + pruning
    data_distinct = con.execute(f"""select sf, qid, t1.n, prob, t1.sf_label, t1.prune_label, t1.query, t1.cat, t1.num_threads, t1.is_scalar,
                        t1.eval_time_ms,
                        ((t1.n*base.eval_time_ms) / (t1.eval_time_ms)) as speedup,
                        ((t1.n*base.eval_time_ms+base.gen_time) / (t1.eval_time_ms+t1.prune_time_ms+(t1.gen_time_ms))) as speedupall,
                        t1.gen_time_ms,
                        ((t1.n*base.eval_time_ms) / (t1.eval_time_ms+t1.prune_time_ms)) as speedupwprune,
                        t1.n / t1.eval_time as throughput,
                        t1.n / (t1.eval_time+t1.prune_time) as throughputwprune,
                        t1.n / (t1.eval_time+t1.prune_time+t1.gen_time) as throughputall
    from ( select * from dense_fade_iters) as t1 JOIN
         ( select * from dense_fade_iters where n=1 and num_threads=1 and is_scalar='true' and prune='False') as base 
         USING (sf, qid, prob)
         where prob={prob}
         """).df()

    if plot:
        # aggregate over queries
        vec_data_distinct = con.execute("""select sf, n, sf_label, prune_label, query, avg(t1.speedup/base.speedup) as speedup,
        avg(t1.speedupwprune/base.speedupwprune) as speedupwprune
         from
         (select * from data_distinct where num_threads=8 and is_scalar='False') as t1 JOIN
         (select * from data_distinct where num_threads=8 and is_scalar='True') as base using (sf, n, sf_label, query, prune_label)
         group by (sf, n, sf_label, prune_label, query)
                """).df()
        p = ggplot(vec_data_distinct, aes(x='n',  y="speedup", color="query", fill="query"))
        p += geom_line(stat=esc('identity'))  + geom_point(stat=esc('identity'))
        p += legend_side
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                xkwargs=dict(breaks=[1, 64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),
            )
        p += facet_grid(".~prune_label", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_19_fade_vec_8.png", p, postfix=postfix, width=6, height=2, scale=0.8)
        
        out = summary_custom('vec_data_distinct', 'sf', """avg(speedup), max(speedup), min(speedup),
                avg(speedupwprune), max(speedupwprune),
                """)
        lines.append(out.to_string(index=False))
        out = summary_custom('vec_data_distinct', 'sf', """avg(speedup), max(speedup), min(speedup),
                avg(speedupwprune), max(speedupwprune),
                """, "where query='Q1'")
        lines.append(out.to_string(index=False))
        out = summary_custom('vec_data_distinct', 'sf', """avg(speedup), max(speedup), min(speedup),
                avg(speedupwprune), max(speedupwprune),
                """, "where query<>'Q1'")
        lines.append(out.to_string(index=False))
        
        # TODO: rename
        if exclude_sample:
            vec_data_distinct_samples = con.execute("select * from vec_data_distinct where query in ('Q6','Q8','Q14','Q19')").df()
        else:
            vec_data_distinct_samples = con.execute("select * from vec_data_distinct where query in ('Q1','Q5','Q9','Q12')").df()

        vec_data_distinct_samples["sys_label"] = "+B+W8"
        vec_data_distinct_samples["sys_label"] = vec_data_distinct_samples.apply(lambda row: row["sys_label"] + "+P" if row["prune_label"]=='FaDE-P' else row["sys_label"] , axis=1)
        vec_data_distinct_samples["sys_label"] = vec_data_distinct_samples.apply(lambda row: row["sys_label"] + "+D" , axis=1)
        p = ggplot(vec_data_distinct_samples, aes(x='n',  y="speedup", color="sys_label", fill="sys_label"))
        p += geom_line(stat=esc('identity'))  + geom_point(stat=esc('identity'))
        p += legend_side
        p += axis_labels('Batch Size', "Speedup", "log10",
                xkwargs=dict(breaks=[64, 512,2048],  labels=list(map(esc,['64',  '512','2K  ']))),
            )
        p += facet_grid(".~query", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_20_fade_vec_queries.png", p, postfix=postfix, width=8, height=2, scale=0.8)
    
        vec_data_distinct_samples = con.execute("select * from vec_data_distinct where n IN  (64, 512, 2048)").df()
        p = ggplot(vec_data_distinct_samples, aes(x='query',  y="speedup", color="prune_label", fill="prune_label"))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += legend_side
        p += axis_labels('query', "Speedup (log)", "discrete", "log10",
            )
        p += facet_grid(".~n", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_21_fade_vec_q8.png", p, postfix=postfix, width=10, height=2.5, scale=0.8)
        
        p = ggplot(vec_data_distinct, aes(x='n',  y="speedupwprune", color="query", fill="query"))
        p += geom_line(stat=esc('identity'))  + geom_point(stat=esc('identity'))
        p += legend_side
        p += axis_labels('Batch Size (log)', "Speedup (log)", "log10", "log10",
                xkwargs=dict(breaks=[1, 64, 256, 512,1024,2048],  labels=list(map(esc,['1','64', '256', '512','1K','2K']))),
            )
        p += facet_grid(".~prune_label", scales=esc("free_y"))
        ggsave(f"figures/{prefix}_22_fade_vec_8_wprune.png", p, postfix=postfix, width=6, height=2, scale=0.8)
        def compare(table, sys, base):
            q =  f"""select query, t1.cat2, ( t1.speedup / base.speedup) as speedup,
            (t1.speedupwprune / base.speedupwprune) as speedupwprune from
            (select * from {table} where cat='{sys}') as t1 JOIN
            (select * from {table} where cat='{base}') as base using
            (query)"""
            lines.append(q)
            out = con.execute(q).df()
            lines.append(out.to_string(index=False))
            q_avg = f"""select avg(speedup), max(speedup),min(speedup),
            avg(speedupwprune), max(speedupwprune), min(speedupwprune) from ({q})"""
            lines.append(q_avg)
            out = con.execute(q_avg).df()
            lines.append(out.to_string(index=False))
            lines.append(f"+++++++++++++++++")

        for nv in [2048]:
            # Baseline, +B, +B+W, +B+W+D, +B+W+D+P
            def label_cat(c):
                if c==f'1W+B=1':
                    return 'Baseline'
                elif c==f'1W+B={nv}':
                    return '+B'
                elif c==f'8W+B={nv}':
                    return '+B+W'
                elif c==f'8W+D+B={nv}':
                    return '+B+W+D'
                elif c==f'8W+D+P+B={nv}':
                    return '+B+W+P+D'
                elif c==f'8W+P+B={nv}':
                    return '+B+W+P'
                else:
                    return c
            data_distinct["cat2"] = data_distinct.apply(lambda row: label_cat(row["cat"]), axis=1)
            postfixcat = postfix + """
            data$cat = factor(data$cat, levels=c('1W', '1W+D', '1W+P', '1W+D+P',
            '2W', '2W+D', '2W+P', '2W+D+P',
            '4W', '4W+D', '4W+P', '4W+D+P',
            '8W', '8W+D', '8W+P', '8W+D+P'))
            data$cat2 = factor(data$cat2, levels=c('Baseline', '+B', '+B+W', '+B+W+D', '+B+W+P', '+B+W+P+D'))
                """
            data_distinct_q = con.execute(f"""
            select 'avg' as typ, prob ,cat, cat2, query, sf_label, prune_label, n, avg(throughput) throughput, avg(speedup) as speedup
            ,avg(speedupwprune) as speedupwprune, avg(throughputwprune) as throughputwprune
            ,max(speedupall) as speedupall, max(throughputall) as throughputall
            from data_distinct
            where prob={prob} and cat IN ('1W+B=1', '1W+B={nv}', '8W+B={nv}', '8W+D+B={nv}', '8W+P+B={nv}', '8W+D+P+B={nv}')
            group by cat, sf_label, prune_label, n, prob, typ, query, cat2
            """).df()
            print(f"++++++++++Summary {nv}+++++++")
            data_distinct_q_vec = con.execute("""select query, t1.cat2, prune_label,( t1.speedup / base.speedup) as speedup from
            (select * from data_distinct_q where cat2='+B+W+P+D' or cat2='+B+W+D') as t1 JOIN
            (select * from data_distinct_q where cat2='+B+W+P'  or cat2='+B+W') as base using
            (prune_label, query)""").df()
            p = ggplot(data_distinct_q_vec, aes(x='query',  y="speedup", color='cat2', fill='cat2'))
            p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
            p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
            p += legend_bottom
            p += legend_side
            ggsave(f"figures/{prefix}_23_fade_{nv}_sf1_speedup_vec.png", p,  postfix=postfixcat,width=8, height=2.5, scale=0.8)

            data_distinct_q = con.execute("select * from data_distinct_q where cat2<>'+B+W+D'").df()
            p = ggplot(data_distinct_q, aes(x='query',  y="speedup", color='cat2', fill='cat2'))
            p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
            p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
            p += legend_bottom
            p += legend_side
            ggsave(f"figures/{prefix}_24_fade_{nv}_sf1_speedup.png", p,  postfix=postfixcat,width=8, height=2.5, scale=0.8)
            
            p = ggplot(data_distinct_q, aes(x='query',  y="throughput", color='cat2', fill='cat2'))
            p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
            p += axis_labels('Query', "Interventions / Sec (log)", "discrete", "log10",
                ykwargs=dict(breaks=[10,1000,1000000],  labels=list(map(esc,['10','1K','1M']))))
            p += legend_bottom
            p += legend_side
            ggsave(f"figures/{prefix}_25_fade_{nv}_sf1_throughput.png", p, postfix=postfixcat, width=8, height=2.5, scale=0.8)
            
            p = ggplot(data_distinct_q, aes(x='query',  y="speedupwprune", color='cat2', fill='cat2'))
            p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
            p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
            p += legend_bottom
            p += legend_side
            ggsave(f"figures/{prefix}_26_fade_{nv}_sf1_speedup_prunecost.png", p,  postfix=postfixcat, width=8, height=2, scale=0.8)
            
            p = ggplot(data_distinct_q, aes(x='query',  y="throughputwprune", color='cat2', fill='cat2'))
            p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
            p += axis_labels('Query', "Intervention / Sec (log)", "discrete", "log10",
                ykwargs=dict(breaks=[10,1000,100000, 1000000],  labels=list(map(esc,['10','1K','100K', '1M']))))
            p += legend_bottom
            p += legend_side
            ggsave(f"figures/{prefix}_27_fade_{nv}_sf1_throughput_prunecost.png", p, postfix=postfixcat, width=8, height=2, scale=0.8)
            def summary_nv(attrs, whr=""):
                print(attrs, whr)
                lines.append(f"summary_nv: {attrs}, {whr}")
                return con.execute(f"""select {attrs},
                avg(speedupwprune) as avg_speedupP, max(speedupwprune) as max_speedupP, min(speedupwprune) as min_speedupP,
                avg(speedup) as avg_speedup, max(speedup) as max_speedup,
                avg(speedupALL) as avg_speedupALL, max(speedupALL) as max_speedupALL,
                avg(throughputwprune) as avg_thp,max(throughputwprune) as max_thp, min(throughputwprune) as min_thp,
                avg(throughputall) as avg_thal, max(throughputall) as max_thal, min(throughputall) as min_thal
                from data_distinct_q {whr} group by {attrs} order by {attrs}
                """).df()
            out = summary_nv("sf_label, cat2, cat, query")
            lines.append(out.to_string(index=False))
            out = summary_nv("sf_label, cat2, cat")
            lines.append(out.to_string(index=False))
            out = summary_nv("sf_label, query")
            lines.append(out.to_string(index=False))
            lines.append(f" (+B) that batching {nv} interventions is ?× faster than the baseline. ")
            out = summary_nv(f"sf_label, cat2", f"where cat='1W+B={nv}'")
            lines.append(out.to_string(index=False))
            lines.append(f"(+B+W) improves throughput by on average ?× over the baseline when using 8 workers")
            out = summary_nv(f"sf_label, cat2", f"where cat='8W+B={nv}'")
            lines.append(out.to_string(index=False))
            # VERIFY: a single intervention, that pruning increases throughput by on average 30× (1−190×)
            lines.append(f"(+B+W+P) is on average 579× (31−2104×) faster than the baseline.")
            out = summary_nv(f"sf_label, cat2", f"where cat='8W+P+B={nv}'")
            lines.append(out.to_string(index=False))
            lines.append(f"+B+W+P+D) is on average 621× (2149.−32×) faster than the baseline,")
            out = summary_nv(f"sf_label, cat2", f"where cat='8W+D+P+B={nv}'")
            lines.append(out.to_string(index=False))

        compare('data_distinct_q', '1W+B=2048', '1W+B=1')
        compare('data_distinct_q', '8W+B=2048', '1W+B=2048')
        compare('data_distinct_q', '8W+P+B=2048', '8W+B=2048')
        compare('data_distinct_q', '8W+D+P+B=2048', '8W+P+B=2048')

        lines.append(f"on average × faster than +B+W+P (up to ).")
        q = """select sf, prune_label, avg(speedup), max(speedup), avg(speedupwprune), max(speedupwprune) 
        from vec_data_distinct where query<>'Q1' and  n=2048 group by prune_label, sf"""
        lines.append(q)
        out = con.execute(q).df()
        lines.append(out.to_string(index=False))
        q = """select sf, prune_label, avg(speedup), max(speedup), avg(speedupwprune), max(speedupwprune) 
        from vec_data_distinct
        where n=2048
        group by prune_label, sf"""
        out = con.execute(q).df()
        lines.append(q)
        lines.append(out.to_string(index=False))
        q = """select sf, prune_label, query,avg(speedup), max(speedup), avg(speedupwprune), max(speedupwprune) 
        from vec_data_distinct
        where n=2048 and prune_label='FaDE-P'
        group by prune_label, sf, query
        """
        out = con.execute(q).df()
        lines.append(q)
        lines.append(out.to_string(index=False))
        
        


with open(f"{prefix}_ablation_analysis_summary.txt", "w") as file:
    for line in lines:
        file.write(line + "\n")
