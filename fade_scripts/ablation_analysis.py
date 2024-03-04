# TODO: use compiled version of lineage pruning
import duckdb
import pandas as pd
from pygg import *

pd.set_option('display.max_rows', None)

legend = theme_bw() + theme(**{
    "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
    "legend.justification":"c(1,0)",
    "legend.position":"c(1,0)",
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=8, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=8),
    "plot.background": element_blank(),
    "panel.border": element_rect(color=esc("#e0e0e0")),
    "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333")),
    "legend.position": esc('none'),
    "legend.margin": margin(t = 0, r = 0, b = 0, l = 0, unit = esc("pt")),
    "legend.text": element_text(colour = "'#333333'", size=9, family = "'Arial'"),
    "legend.key.size": unit(8, esc('pt')),
})

legend_bottom = legend + theme(**{
  "legend.position":esc("bottom"),
  "legend.spacing": "unit(-.5, 'cm')"
})

legend_side = legend + theme(**{
  "legend.position":esc("right"),
})

dbt_prob = 0.05
con = duckdb.connect(':default:')
plot = True
print_summary = True
include_dbt = True
dense = True
single = True
plot_scale = False
include_incremental_random = True

def get_data(fname, scale):
    local_data = pd.read_csv(fname)
    local_data["eval_time_ms"] = scale * local_data["eval_time"]
    local_data["query"] = "Q"+ local_data["qid"].astype(str)
    local_data["cat"] = local_data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
    local_data["cat"] = local_data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    local_data["n"] = local_data["distinct"]
    local_data["sf_label"] = "SF="+ local_data["sf"].astype(str)
    local_data["prune_label"] = local_data.apply(lambda row:"P" if row["prune"] else "NP" , axis=1)
    return local_data
    
if dense:
    dense_data_v2 = get_data(f"fade_data/dense_sf1_v4.csv", 1000)
    dense_single = get_data(f"fade_data/dense_single.csv", 1000)
    postfix = """
    data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
        """

    print("======== DENSE Batching=============")

    print(con.execute("select * from dense_data_v2 where incremental='False' and n>1").df())
    # Batching latency performance varying number of interventions
    level1_data = con.execute(f"""select 
        (base.eval_time_ms * t1.n)/ t1.eval_time_ms as speedup, t1.prune_time*1000 as prune_time,
        sf, prune, t1.query, t1.cat, qid, t1.n, t1.num_threads, t1.distinct,
        t1.num_threads, t1.is_scalar, t1.eval_time_ms, base.eval_time_ms*t1.n as single_eval
        from ( select * from dense_data_v2 where incremental='False' and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1) as t1 JOIN
             ( select * from dense_data_v2 where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1) as base
             USING (sf, qid, prune) """).df()
    
    cat = 'distinct'
    if plot:
        p = ggplot(level1_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_latency_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

        # Batching speedup over single intervention performance varying number of interventions
        p = ggplot(level1_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        
        level1_data_noprune = con.execute("select * from level1_data where prune='False'").df()
        p = ggplot(level1_data_noprune, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_speedup_sf1_noprune.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        p = ggplot(level1_data_noprune, aes(x='n',  y="speedup", color='query', fill='query', group='query'))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('Batch Size', "Interventions / Sec (log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_speedup_sf1_noprune_line.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        p = ggplot(level1_data_noprune, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_latency_sf1_noprune.png", p, postfix=postfix, width=8, height=3, scale=0.8)

    print("======== DENSE Pruning =============")
    prune_data = con.execute(f"""select sf, n, t1.query, t1.cat, qid, t1.distinct,
        t1.prune_time,
        (base.eval_time_ms)/ t1.eval_time_ms as speedup_nosetup,
        (base.eval_time_ms)/ (t1.eval_time_ms+t1.prune_time) as speedup,
        t1.eval_time_ms, base.eval_time_ms as base_eval
        from ( select * from level1_data where prune='True') as t1 JOIN
             ( select * from level1_data where prune='False') as base
             USING (sf, qid, n) """).df()
    
    if plot:
        p = ggplot(prune_data, aes(x='n',  y="speedup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size (log)', "Speedup (log)", "discrete", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,512,1024,2048,2560],  labels=list(map(esc,['1','512','1024','2048','2560']))),
            )
        p += legend_side
        ggsave(f"figures/fade_pruning_batch.png", p, postfix=postfix, width=3, height=2, scale=0.8)
        
        p = ggplot(prune_data, aes(x='n',  y="speedup_nosetup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size (log)', "Speedup (log)", "discrete", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,512,1024,2048,2560],  labels=list(map(esc,['1','512','1024','2048','2560']))),
            )
        p += legend_side
        ggsave(f"figures/fade_pruning_batch_setup.png", p, postfix=postfix, width=3, height=2, scale=0.8)
    prune_data_detailed = con.execute(f"""select sf, n, t1.query, t1.cat, qid, t1.distinct,
        t1.prune_time*1000,is_scalar, num_threads,
        (base.eval_time_ms)/ t1.eval_time_ms as speedup_nosetup,
        (base.eval_time_ms)/ (t1.eval_time_ms+t1.prune_time*1000) as speedup,
        t1.eval_time_ms, base.eval_time_ms as base_eval
        from ( select * from dense_data_v2 where incremental='False' and  sf=1 and prob=0.1 and prune='True') as t1 JOIN
             ( select * from dense_data_v2 where incremental='False' and  sf=1 and prob=0.1 and prune='False') as base
             USING (sf, qid, n, is_scalar, num_threads) """).df()
    if plot:
        p = ggplot(prune_data_detailed, aes(x='n',  y="speedup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size (log)', "Speedup (log)", "discrete", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,512,1024,2048,2560],  labels=list(map(esc,['1','512','1024','2048','2560']))),
            )
        p += legend_side
        p += facet_grid(".~is_scalar~num_threads", scales=esc("free_y"))
        ggsave(f"figures/fade_pruning_batch_compined.png", p, postfix=postfix, width=8, height=2, scale=0.8)
        
        p = ggplot(prune_data_detailed, aes(x='n',  y="speedup_nosetup", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size (log)', "Speedup (log)", "discrete", "log10",
                ykwargs=dict(breaks=[0.01,0.1,0,10,100],  labels=list(map(esc,['0.01','0.1','0','10','100']))),
                xkwargs=dict(breaks=[1,512,1024,2048,2560],  labels=list(map(esc,['1','512','1024','2048','2560']))),
            )
        p += legend_side
        p += facet_grid(".~is_scalar~num_threads", scales=esc("free_y"))
        ggsave(f"figures/fade_pruning_batch_setup_combined.png", p, postfix=postfix, width=8, height=2, scale=0.8)
    
        

    print("======== DENSE Vec =============")

    # Additional vectorization speedup over batched execution of  varying numbers of interventions
    vec_data = con.execute("""select sf, t1.sf_label, prune, qid, n, t1.prune_label, t1.query, t1.cat, t1.num_threads,
        t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms*n as single_eval from level1_data as base JOIN 
            (select * from dense_data_v2 where num_threads=1 and is_scalar='False' and sf=1) as t1
            USING (sf, qid, n, prune)""").df()
    vec_data["prune_label"] = vec_data.apply(lambda row:"FaDE-P" if row["prune"] else "FaDE" , axis=1)
    if plot:
        p = ggplot(vec_data, aes(x='n',  y="speedup", color="query", fill="query", linetype='prune_label'))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('Batch Size', "Speedup (log)", "discrete", "log10",
                xkwargs=dict(breaks=[1,512,1024,2048,2560],  labels=list(map(esc,['1','512','1024','2048','2560']))),
            )
        p += legend_side
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave(f"figures/fade_vec.png", p, width=4, height=2.5, scale=0.8)

    print("======== DENSE Threads =============")
    # Additional threading speedup for 2, 4 and 8 threads over batched execution
    vec_data = con.execute("""select sf, qid, prune, t1.prune_label, n, t1.sf_label, t1.query, t1.cat, t1.num_threads,
        t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms*n as single_eval from level1_data as base JOIN 
            (select * from dense_data_v2 where is_scalar='True' and sf=1 and n=2560) as t1
            USING (sf, qid, n, prune)""").df()

    if plot:
        cat = 'num_threads'
        p = ggplot(vec_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_threading_latency_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        p = ggplot(vec_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_threading_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        vec_data["prune_label"] = vec_data.apply(lambda row:"FaDE-P" if row["prune"] else "FaDE" , axis=1)
        p = ggplot(vec_data, aes(x='num_threads',  y="speedup", color="query", fill="query", linetype='prune_label'))
        p += geom_line(stat=esc('identity')) 
        p += axis_labels('# threads', "Speedup (log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave(f"figures/fade_threading_speedup_sf1_line.png", p, width=4, height=2.5, scale=0.8)


    print("======== DENSE Best =============")
    data_distinct = con.execute("""select t1.sf_label, sf, t1.prune_label, t1.query, t1.cat,qid, n, t1.num_threads, t1.is_scalar,
                        t1.eval_time_ms, base.eval_time_ms / t1.eval_time_ms as speedup,
                        n / (t1.eval_time_ms/1000.0) as throughput
    from ( select * from dense_data_v2 where n=2560 ) as t1 JOIN
         ( select * from dense_data_v2 where n=2560 and num_threads=1 and is_scalar='true' and prune='False') as base 
         USING (sf, qid, n) """).df()

    if plot:
        cat = 'cat'
        p = ggplot(data_distinct, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
        p += legend_bottom
        p += legend_side
        p += facet_grid(".~sf_label~prune_label", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_speedup.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        p = ggplot(data_distinct, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_bottom
        p += legend_side
        p += facet_grid(".~sf_label~prune_label", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_latency.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        cat = "query"
        scalar_data = con.execute("select * from data_distinct where is_scalar='False' and prune_label='P'").df()
        p = ggplot(scalar_data, aes(x='num_threads',  y="throughput", color=cat, fill=cat, group=cat))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('# Threads', "Interventions / Sec (log)", "discrete", "log10",
                ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
                xkwargs=dict(breaks=[1,2,4,8], labels=list(map(esc,['1.0', '2.0','4.0','8.0']))))
        p += legend_bottom
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_throughput_vec_p.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        p = ggplot(scalar_data, aes(x='num_threads',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('# Threads', "Speedup (log)", "discrete", "log10",
                xkwargs=dict(breaks=[1,2,4,8], labels=list(map(esc,['1.0', '2.0','4.0','8.0']))))
        p += legend_bottom
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_speedup_vec_p.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        data_distinct["cat"] = data_distinct.apply(lambda row: row["cat"]+"+P" if row["prune_label"]=='P' else  row["cat"], axis=1)
        # speedup of vectorization. x-axis: queries, y-axis: speedup
        cat = "cat"
        scalar_data = con.execute("""select * from data_distinct where num_threads=1 and 
        ( (prune_label='NP' and is_scalar='False') or (prune_label='P' and is_scalar='True') or (prune_label='P' and is_scalar='False') ) """).df()
        p = ggplot(scalar_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
        p += legend_bottom
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_opt_speedup.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        print(con.execute("select * from dense_data_v2 where incremental='False' and n=1 and num_threads=8 and is_scalar='True' and sf=1 and prob=0.1 and prune='True'").df())
        
        # combined optimization wins
        best_data = con.execute(f"""select sf, t1.prune_label, t1.query, t1.cat,qid, t1.n, t1.distinct, t1.num_threads, t1.is_scalar,
                            t1.eval_time_ms, base.eval_time_ms, base.eval_time_ms*t1.n,
                            (t1.n*base.eval_time_ms) / (t1.eval_time_ms+t1.prune_time*1000) as speedup_setup,
                            (t1.n*base.eval_time_ms) / (t1.eval_time_ms) as speedup,
                            t1.n / (t1.eval_time_ms/1000.0) as throughput
        from ( select * from dense_data_v2 where num_threads=8 and is_scalar='False' and prune='True') as t1 JOIN
             ( select * from dense_single where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1 and prune='True') as base
             USING (sf, qid) """).df()
        print("======== DENSE best =============")
        cat = 'distinct'
        p = ggplot(best_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_best_distinct_latency_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        p = ggplot(best_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_best_distinct_speedup_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        cat = "query"
        p = ggplot(best_data, aes(x='n',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('Batch Size', "Speedup (log)", "discrete", "log10",)
        p += legend_side
        ggsave("figures/fade_best_distinct_speedup_sf1_line.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

        # x-axis: batch size, y-axis: throughput
        p = ggplot(best_data, aes(x='n',  y="throughput", color=cat, fill=cat, group=cat))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('Batch Size', "Interventions / Sec (log)", "discrete", "log10",
                ykwargs=dict(breaks=[10000,100000,1000000],  labels=list(map(esc,['10e4','10e5','10e6']))),)
        p += legend_side
        ggsave("figures/fade_best_distinct_throughput_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

        

batching = False
pruning = False
vec = True
workers = False
best = False
if print_summary:
    if batching:
        print("========== Batching ============")
        level1_data = con.execute(f"""select 
            (base.eval_time_ms * t1.n)/ t1.eval_time_ms as speedup,t1.prune_time*1000 as prune_time,
            sf, prune, t1.query, t1.cat, qid, t1.n, t1.num_threads, t1.distinct,
            t1.num_threads, t1.is_scalar, t1.eval_time_ms, base.eval_time_ms*t1.n as single_eval
            from ( select * from dense_data_v2 where incremental='False' and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1) as t1 JOIN
                 ( select * from dense_data_v2 where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1) as base
                 USING (sf, qid, prune) """).df()
        print(con.execute("""select max(speedup), avg(speedup), min(speedup), 
        avg(eval_time_ms), avg(single_eval), max(eval_time_ms), max(single_eval),
        min(eval_time_ms), min(eval_time_ms) from level1_data where prune='False'""").df())
        
    if pruning:
        print("======== DENSE Pruning =============")
        prune_data = con.execute(f"""select sf, n, t1.query, t1.cat, qid, t1.distinct,
            t1.prune_time,
            (base.eval_time_ms)/ t1.eval_time_ms as speedup_nosetup,
            (base.eval_time_ms)/ (t1.eval_time_ms+t1.prune_time) as speedup,
            t1.eval_time_ms, base.eval_time_ms as base_eval
            from ( select * from level1_data where prune='True') as t1 JOIN
                 ( select * from level1_data where prune='False') as base
                 USING (sf, qid, n) """).df()
        
        print("Lineage pruning cost:")
        print(con.execute("""select 
        avg(prune_time), max(prune_time), min(prune_time),
        from prune_data
        group by sf
        order by sf""").df())
        
        print(con.execute("""select qid,
        avg(prune_time), max(prune_time), min(prune_time),
        from prune_data
        group by sf, qid
        order by sf, qid""").df())
        
        print("Pruning wins:")
        print(con.execute("""select n, qid, max(speedup_nosetup), avg(speedup_nosetup), min(speedup_nosetup), 
        from prune_data
        group by sf, n, qid
        order by sf, n, qid""").df())
        
        print(con.execute("""select n, max(speedup_nosetup), avg(speedup_nosetup), min(speedup_nosetup), 
        from prune_data
        group by sf, n
        order by n""").df())
        
        print(con.execute("""select qid, max(speedup_nosetup), avg(speedup_nosetup), min(speedup_nosetup), 
        from prune_data
        group by sf, qid
        order by qid""").df())
        
        print(con.execute("""select sf, max(speedup_nosetup), avg(speedup_nosetup), min(speedup_nosetup), 
        from prune_data
        group by sf""").df())
        
        print("Pruning wins including pruning cost:")
        print(con.execute("""select n, qid, max(speedup), avg(speedup), min(speedup), 
        from prune_data
        group by sf, n, qid
        order by sf, n, qid""").df())
        
        print(con.execute("""select n, max(speedup), avg(speedup), min(speedup), 
        from prune_data
        group by sf, n
        order by n""").df())
        
        print(con.execute("""select qid, max(speedup), avg(speedup), min(speedup), 
        from prune_data
        group by sf, qid
        order by qid""").df())
        
        print(con.execute("""select max(speedup), avg(speedup), min(speedup), 
        from prune_data
        group by sf""").df())
        
        
    if vec:
        print("======== DENSE Vec =============")

        # Additional vectorization speedup over batched execution of  varying numbers of interventions
        vec_data = con.execute("""select sf, prune, qid, n, t1.query, t1.cat, t1.num_threads,
            t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
            base.eval_time_ms*n as single_eval from level1_data as base JOIN 
                (select * from dense_data_v2 where num_threads=1 and is_scalar='False' and sf=1) as t1
                USING (sf, qid, n, prune)""").df()
        
        print(con.execute("""select n, qid, prune, max(speedup), avg(speedup), min(speedup), 
        from vec_data
        group by sf, n, qid, prune
        order by sf, n, qid, prune""").df())
        
        print(con.execute("""select n,  max(speedup), avg(speedup), min(speedup), 
        from vec_data
        group by sf, n
        order by n""").df())
        
        print(con.execute("""select qid,  max(speedup), avg(speedup), min(speedup), 
        from vec_data
        group by sf, qid
        order by qid""").df())
        
        print(con.execute("""select prune, max(speedup), avg(speedup), min(speedup), 
        from vec_data
        group by sf, prune
        order by prune""").df())
        
        print(con.execute("""select max(speedup), avg(speedup), min(speedup), 
        from vec_data
        group by sf""").df())

    if workers:
        print("======== DENSE Threads =============")
        # Additional threading speedup for 2, 4 and 8 threads over batched execution
        thread_data = con.execute("""select sf, qid, prune, t1.prune_label, n, t1.sf_label, t1.query, t1.cat, t1.num_threads,
            t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
            base.eval_time_ms*n as single_eval from level1_data as base JOIN 
                (select * from dense_data_v2 where is_scalar='True' and sf=1 and n=2560) as t1
                USING (sf, qid, n, prune)""").df()
        
        print(con.execute("""select num_threads, qid, prune, max(speedup), avg(speedup), min(speedup), 
        from thread_data
        group by sf, num_threads, qid, prune
        order by sf, num_threads, qid, prune""").df())
        
        print(con.execute("""select num_threads, qid,  max(speedup), avg(speedup), min(speedup), 
        from thread_data
        group by sf, num_threads, qid
        order by sf, num_threads, qid""").df())
        
        
        print(con.execute("""select num_threads, max(speedup), avg(speedup), min(speedup), 
        from thread_data
        group by sf, num_threads
        order by sf, num_threads""").df())
        
    if best:
        print("======== DENSE Best =============")
        print(con.execute("""select qid, n, max(speedup), avg(speedup), min(speedup), 
        max(throughput), avg(throughput), min(throughput),
        max(eval_time_ms), avg(eval_time_ms), min(eval_time_ms)
        from best_data
        group by sf, qid, n
        order by sf, qid, n""").df())
        
        print(con.execute("""select qid,
        max(speedup), avg(speedup), min(speedup), 
        max(throughput), avg(throughput), min(throughput),
        max(eval_time_ms), avg(eval_time_ms), min(eval_time_ms)
        from best_data
        group by sf, qid
        order by sf, qid""").df())
        
        print(con.execute("""select n, 
        max(speedup), avg(speedup), min(speedup), 
        max(throughput), avg(throughput), min(throughput),
        max(eval_time_ms), avg(eval_time_ms), min(eval_time_ms)
        from best_data
        group by sf,  n
        order by sf,  n""").df())
        
        print(con.execute("""select max(speedup), avg(speedup), min(speedup), 
        max(throughput), avg(throughput), min(throughput),
        max(eval_time_ms), avg(eval_time_ms), min(eval_time_ms)
        from best_data
        group by sf
        order by sf""").df())
