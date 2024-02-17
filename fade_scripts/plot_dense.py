import duckdb
import pandas as pd
from pygg import *

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

con = duckdb.connect(':default:')
plot = True
print_summary = True
include_dbt = True
dense = True
single = True
plot_scale = True

# for each query,
#data = pd.read_csv('dense_feb9_sf1_timings.csv')
if include_dbt:
    dbt_data = pd.read_csv("dbtoast.csv")
    dbt_data["eval_time_ms"]=dbt_data["eval_time"]
    dbt_data["query"] = "Q"+dbt_data["qid"].astype(str)
    dbt_data["cat"] = dbt_data.apply(lambda row: "DBT_prune" if row["prune"] else "DBT", axis=1)
    dbt_data["n"] = dbt_data["distinct"]
    dbt_data["sf_label"] = "SF="+dbt_data["sf"].astype(str)
    print(dbt_data)
    cat = "prob"
    p = ggplot(dbt_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
    p += axis_labels('Query', "Run time (ms)", "discrete", "log10")
    p += legend_bottom
    p += facet_grid(".~prune", scales=esc("free_y"))
    ggsave("figures/dbt.png", p,width=10, height=8, scale=0.8)
    
lineage_data = pd.read_csv('fade_data/lineage_overhead.csv')
lineage_data["query"] = "Q"+lineage_data["qid"].astype(str)
lineage_data["sf_label"] = "SF="+lineage_data["sf"].astype(str)

if plot_scale:
    scale_data = pd.read_csv('dense_scale_all.csv')
    scale_data["eval_time_ms"] = 1000 * scale_data["eval_time"]
    scale_data["query"] = "Q"+scale_data["qid"].astype(str)
    scale_data["cat"] = scale_data.apply(lambda row: str(row["num_threads"]) + " Threads", axis=1)
    scale_data["cat"] = scale_data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    scale_data["n"] = scale_data["distinct"]
    scale_data["sf_label"] = "SF="+scale_data["sf"].astype(str)
    
    scale_fig_data = con.execute("""select *, query, sf, n, is_scalar,
        n / eval_time as throughput 
        from scale_data where n=2560 and is_scalar='False'""").df()
    postfix = """
    data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
        """
    p = ggplot(scale_fig_data, aes(x='sf',  y="throughput", color='query', group='query', shape='query'))
    p += geom_point(stat=esc('identity'))
    p +=  geom_line()
    p += axis_labels('SF (log)', "Interventions / Seconds (log)", "log10", "log10", xkwargs=dict(breaks=[1,5,10]),
            ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
            )
    p += legend_side
    ggsave("figures/fade_throughput.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

# figure 1: single intervention latency
if single:
    data_single_v2 = pd.read_csv('dense_single_v4.csv')
    data_single_v2["eval_time_ms"]=1000*data_single_v2["eval_time"]
    data_single_v2["query"] = "Q"+data_single_v2["qid"].astype(str)
    data_single_v2["cat"] = data_single_v2.apply(lambda row: str(row["num_threads"]) + " Threads", axis=1)
    data_single_v2["cat"] = data_single_v2.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    data_single_v2["cat"] = data_single_v2.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)
    data_single_v2["n"] = data_single_v2["distinct"]
    data_single_v2["prune_label"] = data_single_v2.apply(lambda row:"Fade-P" if row["prune"] else "Fade" , axis=1)
    data_single_v2["sf_label"] = "SF="+data_single_v2["sf"].astype(str)


    # TODO: add FaDe-Incr
    single_data = pd.read_csv('dense_single_test.csv')
    single_data["eval_time_ms"]=1000*single_data["eval_time"]
    single_data["query"] = "Q"+single_data["qid"].astype(str)
    single_data["cat"] = single_data.apply(lambda row: str(row["num_threads"]) + " Threads", axis=1)
    single_data["cat"] = single_data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    single_data["n"] = single_data["distinct"]
    single_data["sf_label"] = "SF="+single_data["sf"].astype(str)
    if print_summary:
        print("======== Summary =============")
        summary_data = con.execute("""
        select qid, n, sf, prune,
        max(post_time+gen_time+prep_time+lineage_time) as prep_time,
        avg(post_time+gen_time+prep_time+lineage_time) as avg_prep_time,
        max(compile_time),
        max(eval_time), avg(eval_time), min(eval_time),
        max(prune_time), avg(prune_time)
        from single_data
        group by qid, sf, n, prune
        order by sf, qid, n, prune
        """).df()
        print(summary_data)

    dbt_q = ""
    if include_dbt:
        dbt_q = """
        UNION ALL select 'DBT-prune' as system, query, prune, sf, sf_label, eval_time_ms as eval_time from dbt_data where prob=0.1 and prune='True'
        UNION ALL select 'DBT' as system, query, prune, sf, sf_label, eval_time_ms as eval_time from dbt_data where prob=0.1 and prune='False'
        """
    #'Fade' as system, query, prune, sf, sf_label, eval_time_ms from single_data where n=1 and num_threads=1 and prune='False'
    #UNION ALL select 'Fade-prune' as system, query, prune, sf, sf_label, eval_time_ms from single_data where n=1 and num_threads=1 and prune='True'
    fig1_data = con.execute(f"""
        select 'Fade-prune' as system, query, prune, sf, sf_label,  eval_time_ms from data_single_v2 where n=1 and num_threads=1  and prune='True'
        UNION ALL select 'Fade' as system, query, prune, sf, sf_label, eval_time_ms from data_single_v2 where n=1 and num_threads=1  and prune='False'
        UNION ALL select 'Q' as system, query, 'False' as prune, sf, sf_label,query_timing*1000 as eval_time from lineage_data
        UNION ALL select 'Q+' as system, query, 'False' as prune, sf, sf_label, lineage_timing*1000 as eval_time from lineage_data
        UNION ALL select 'Q++' as system, query, 'False' as prune, sf, sf_label, ksemimodule_timing*1000 as eval_time from single_data where n=1 and num_threads=1
            {dbt_q} """).df()

    cat = "system"
    if plot:
        postfix = """
        data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
        data$sf_label = factor(data$sf_label, levels=c('SF=1.0', 'SF=5.0', 'SF=10.0'))
            """
        p = ggplot(fig1_data, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
        p += legend_side
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave("figures/fade_single.png", p, postfix=postfix, width=8, height=3, scale=0.8)

        fig1_data_sf1 = con.execute("select * from fig1_data where sf=1").df()
        p = ggplot(fig1_data_sf1, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
        p += legend_side
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave("figures/fade_single_sf1.png", p, postfix=postfix, width=5, height=3, scale=0.8)

postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
data$cat = factor(data$cat, levels=c('1 Threads', '2 Threads', '4 Threads', '8 Threads', '1 Threads+SIMD', '2 Threads+SIMD', '4 Threads+SIMD', '8 Threads+SIMD'))
    """
if dense:
    data = pd.read_csv('dense_delete_all_sf1.csv')
    data["eval_time_ms"]=1000*data["eval_time"]
    data["query"] = "Q"+data["qid"].astype(str)
    data["cat"] = data.apply(lambda row: str(row["num_threads"]) + " Threads", axis=1)
    data["cat"] = data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    data["n"] = data["distinct"]
    data["prune_label"] = data.apply(lambda row:"Fade-P" if row["prune"] else "Fade" , axis=1)
    if print_summary:
        print("======== Summary =============")
        summary_data = con.execute("""
        select query, n,
        max(compile_time),
        max(eval_time), avg(eval_time), min(eval_time)
        from data
        group by query, sf, n
        """).df()
        print(summary_data)

    # Batching latency performance varying number of interventions
    fig2_data = con.execute("""select sf, prune, t1.query, t1.cat, qid, t1.n, t1.num_threads, t1.distinct,
        t1.num_threads, t1.is_scalar, (base.eval_time_ms * t1.n)/ t1.eval_time_ms as speedup,
        t1.eval_time_ms
        from ( select * from data where num_threads=1 and is_scalar='true' and sf=1 ) as t1 JOIN
             ( select * from data where n=1 and num_threads=1 and is_scalar='true' and sf = 1) as base
             USING (sf, qid, prune) """).df()
    cat = 'distinct'
    if plot:
        p = ggplot(fig2_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms)", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_latency_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

        # Batching speedup over single intervention performance varying number of interventions
        p = ggplot(fig2_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)


    # Additional vectorization speedup over batched execution of  varying numbers of interventions
    fig3_data = con.execute("""select sf, prune, qid, n, t1.query, t1.cat, t1.num_threads,
        t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms from fig2_data as base JOIN 
            (select * from data where num_threads=1 and is_scalar='false' and sf=1) as t1
            USING (sf, qid, n, prune)""").df()

    if plot:
        p = ggplot(fig3_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_vec_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

    # Additional threading speedup for 2, 4 and 8 threads over batched execution
    fig4_data = con.execute("""select sf, qid, prune,  n, t1.query, t1.cat, t1.num_threads,
        t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms from fig2_data as base JOIN 
            (select * from data where is_scalar='true' and sf=1 and n=2560) as t1
            USING (sf, qid, n, prune)""").df()

    cat = 'num_threads'
    if plot:
        p = ggplot(fig4_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_threading_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

        data_distinct = con.execute("select * from data where num_threads=8 and is_scalar='false'").df()
        cat = 'distinct'
        p = ggplot(data_distinct, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms)", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_best_distinct_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)


        data_distinct = con.execute("select * from data where n=2560").df()
        cat = 'cat'
        p = ggplot(data_distinct, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Latency (ms)", "discrete")
        p += legend_bottom
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

    data_distinct = con.execute("""select sf, prune, t1.query, t1.cat,qid, n, t1.num_threads, t1.is_scalar,
    base.eval_time_ms / t1.eval_time_ms as speedup from ( select * from data where n=2560 ) as t1 JOIN
    (select * from data where n=2560 and num_threads=1 and is_scalar='true') as base USING (sf, qid, n, prune) """).df()

    if plot:
        cat = 'cat'
        p = ggplot(data_distinct, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_bottom
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_speedup.png", p, postfix=postfix, width=8, height=3, scale=0.8)

# TODO: add incremental single evaluation. hold the indices of elements to be deleted. this would be equivalent eval to dbtoast

if True:
    data_spec = pd.read_csv('dense_delete_spec_sf1.csv')
    data_search = pd.read_csv('search_sf1.csv')
    data_spec["itype"] = "DD"
    data_search["itype"] = "S"
    data = pd.concat([data_spec, data_search], axis=0)
    
    data["eval_time_ms"]=1000*data["eval_time"]
    data["query"] = "Q"+data["qid"].astype(str)
    data["cat"] = data.apply(lambda row: str(row["num_threads"]) + " Threads", axis=1)
    data["cat"] = data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    data["cat"] = data.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)
    data["n"] = data["distinct"]

    # SEARCH (non-incremental, incremental) vs DENSE_DELETE_SPEC
    # x-axis: n_interventions, y-axis: latency, facet: query, cat: itype+incremental
    # speedup: base x-axis DENSE_DELETE_SPEC

    cat = "cat"
    p = ggplot(data, aes(x='prune',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
    p += axis_labels('Query', "Run time (ms)", "discrete")
    p += legend_bottom
    p += facet_grid(".~query~n", scales=esc("free_y"))
    ggsave("figures/fade_search.png", p,width=10, height=8, scale=0.8)

    if include_dbt:
        # fig 1:
        # x-axis prob, y-axis dbt normalized latency against fade
        dbt_fade_data = con.execute("""
        select sf, query, fade.prune_label, dbt.prune dbt_prune, fade.prune, fade.num_threads, dbt.prob,
        (dbt.eval_time_ms / fade.eval_time_ms) as nor,
        dbt.eval_time_ms, fade.eval_time_ms
        from (select * from dbt_data where prune='True') as dbt JOIN
        (select * from  data_single_v2 where n=1 and num_threads=1) as fade
        USING (query, sf)
        """).df()
        #(select * from  data where itype='S' and incremental='True' and n=1 and num_threads=1 and prune='True') as fade
        postfix = """
        data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
            """
        p = ggplot(dbt_fade_data, aes(x='prob',  y="nor", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += geom_hline(aes(yintercept=1, linetype=esc("Fade")))
        p += axis_labels('Deletion Probability', "Nor. Latency  (log)", "continuos", "log10",
            ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
            xkwargs=dict(breaks=[0.01, 0.05, 0.1,],  labels=list(map(esc,['0.01','0.05','0.1']))),
            )
        p += legend_side
        p += facet_grid(".~prune_label", scales=esc("free_x"), space=esc("free_x"))
        ggsave(f"figures/fade_dbt_vs_fade.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        dbt_fade_data_prune = con.execute("""select * from dbt_fade_data where prune_label='Fade-P'""").df()
        p = ggplot(dbt_fade_data_prune, aes(x='prob',  y="nor", color="query", fill="query",  group="query"))
        p += geom_line(stat=esc('identity')) 
        p += geom_hline(aes(yintercept=1, linetype=esc("Fade")))
        p += axis_labels('Deletion Probability', "Nor. Latency  (log)", "continuos", "log10",
            ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
            xkwargs=dict(breaks=[0.01, 0.05, 0.1,],  labels=list(map(esc,['0.01','0.05','0.1']))),
            )
        p += legend_side
        ggsave(f"figures/fade_dbt_vs_fade_prune.png", p, postfix=postfix,  width=3, height=2, scale=0.8)

        # fig 2:
        # reexecute
        fig1_data = con.execute("""
            select 'Fade-prune+' as system, query, prune, sf, eval_time_ms from data_single_v2 where n=1 and num_threads=1  and prune='True'
            UNION ALL select 'Fade+' as system, query, prune, sf, eval_time_ms from data_single_v2 where n=1 and num_threads=1  and prune='False'
            UNION ALL select 'Fade-prune' as system, query, prune, sf, eval_time_ms from data where n=1 and num_threads=1 and itype='DD' and prune='True'
            UNION ALL select 'Fade' as system, query, prune, sf, eval_time_ms from data where n=1 and num_threads=1 and itype='DD' and prune='False'
            UNION ALL select 'Q' as system, query, 'False' as prune, sf, query_timing*1000 as eval_time from lineage_data
            UNION ALL select 'Q+' as system, query, 'False' as prune, sf, lineage_timing*1000 as eval_time from lineage_data
            UNION ALL select 'DBT-prune' as system, query, prune, sf, eval_time_ms as eval_time from dbt_data where prob=0.1 and prune='True'
            UNION ALL select 'DBT' as system, query, prune, sf, eval_time_ms as eval_time from dbt_data where prob=0.1 and prune='False'
                """).df()
        cat = "system"
        p = ggplot(fig1_data, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', 'Latency (ms) log', 'discrete', 'log10')
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_dbt_single.png", p, width=8, height=3, scale=0.8)

        # x-axis query, y-axis latency
