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
    
if plot_scale:
    scale_data = get_data('fade_data/dense_scale_all.csv', 1000)
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

if include_incremental_random:
    incremental_data = get_data("fade_data/single_incremental_random_sparse.csv", 1000)

# for each query,
if include_dbt:
    dbt_data = get_data("fade_data/dbtoast.csv", 1)
    dbt_data["cat"] = dbt_data.apply(lambda row: "DBT_prune" if row["prune"] else "DBT", axis=1)
    
    cat = "prob"
    if plot:
        p = ggplot(dbt_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Run time (ms)", "discrete", "log10")
        p += legend_bottom
        p += facet_grid(".~prune", scales=esc("free_y"))
        ggsave("figures/dbt.png", p,width=10, height=8, scale=0.8)

    # print summary
    print("======== DBT Eval Summary =============")
    summary_data = con.execute("""
    select qid, sf, prune,
    max(eval_time_ms), avg(eval_time_ms), min(eval_time_ms)
    from dbt_data
    group by qid, sf, prune
    order by sf, qid, prune
    """).df()
    print(summary_data)
    
    summary_data = con.execute("""
    select sf, prune,
    max(eval_time_ms), avg(eval_time_ms), min(eval_time_ms)
    from dbt_data
    group by sf, prune
    """).df()
    print(summary_data)
    
    summary_data = con.execute("""
    select sf,
    max(dbt.eval_time_ms/dbtp.eval_time_ms) as speedup_max,
    avg(dbt.eval_time_ms/dbtp.eval_time_ms) as speedup_avg,
    min(dbt.eval_time_ms/dbtp.eval_time_ms) as speedup_min
    from (select * from dbt_data where prune='True') as dbtp
    JOIN (select * from dbt_data where prune='False') as dbt
    USING (sf, qid, prob)
    group by sf
    """).df()
    print(summary_data)
    
# contains overhead of original query without lineage capture, then with, then with intermediates
lineage_data = pd.read_csv('fade_data/lineage_overhead.csv')
lineage_data["query"] = "Q"+lineage_data["qid"].astype(str)
lineage_data["sf_label"] = "SF="+lineage_data["sf"].astype(str)

#data = pd.read_csv('dense_delete_all_sf1.csv')
#dense_data = get_data('fade_data/dense_all_sf1_v2.csv', 1000)
#dense_data = get_data('dense_sf1_v2.csv', 1000)
dense_data = get_data('dense_sf1_v2.csv', 1000)
dense_data["cat"] = dense_data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
dense_data["cat"] = dense_data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
dense_data["cat"] = dense_data.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)

if print_summary:
    print("======== Lineage Overheaad Summary =============")
    summary_data = con.execute("""
    select qid, sf, avg(query_timing) as qtiming, avg(lineage_timing) as ltiming,
    avg(dense_data.ksemimodule_timing) as ktiming,
    avg(lineage_timing-query_timing) as lineage_overhead,
    avg(dense_data.ksemimodule_timing-query_timing) as ksemimodule_overhead,
    from lineage_data JOIN dense_data USING (qid, sf)
    group by qid, sf
    order by qid, sf
    """).df()
    print(summary_data)

    print("======== Pruning Summary =============")
    summary_data = con.execute("""
    select qid, sf,
    max(prune_time), avg(prune_time)
    from dense_data
    where prune='True'
    group by qid, sf
    order by sf, qid
    """).df()
    print(summary_data)

    print("======== Lineage Time  and Post TimeSummary =============")
    summary_data = con.execute("""
    select qid, sf,
    max(lineage_time) as lin_time,
    avg(lineage_time) as avg_lin_time,
    max(post_time) as total_post_time,
    avg(post_time) as total_avg_post_time,
    from dense_data
    group by qid, sf
    order by sf, qid
    """).df()
    print(summary_data)

    print("======== Code Gen and Mem Alloc Summary =============")
    summary_data = con.execute("""
    select qid, n, sf,
    max(prep_time) as total_prep_time,
    avg(prep_time) as total_avg_prep_time,
    max(post_time+prep_time+lineage_time) as total_time,
    avg(post_time+prep_time+lineage_time) as total_avg_time
    from dense_data
    group by qid, sf, n
    order by sf, qid, n
    """).df()
    print(summary_data)
    
    print("======== Compile Time Summary =============")
    summary_data = con.execute("""
    select
    max(compile_time), avg(compile_time)
    from dense_data
    """).df()
    print(summary_data)
    
    print("======== Evaluation Summary =============")
    summary_data = con.execute("""
    select qid, n, sf,
    max(eval_time), avg(eval_time), min(eval_time)
    from dense_data
    group by qid, sf, n
    order by sf, qid, n
    """).df()
    print(summary_data)

    if include_dbt:
        print("======== DBT vs FaDe Summary =============")
        summary_data = con.execute("""
        select qid, sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='False' and dbt_data.prune='False'
        and f.incremental='False' and f.num_threads=1
        group by qid, sf
        order by sf, qid
        """).df()
        print(summary_data)
        
        print("======== DBT vs FaDe-P Summary =============")
        summary_data = con.execute("""
        select qid, sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='True' and dbt_data.prune='False'
        and f.incremental='False' and f.num_threads=1
        group by qid, sf
        order by sf, qid
        """).df()
        print(summary_data)
        
        print("======== DBT vs FaDe-I Summary =============")
        summary_data = con.execute("""
        select qid, sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='False' and dbt_data.prune='False'
        and f.incremental='True' and f.num_threads=1
        group by qid, sf
        order by sf, qid
        """).df()
        print(summary_data)
        
        print("======== DBT-P vs FaDe Summary =============")
        summary_data = con.execute("""
        select qid, sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='False' and dbt_data.prune='True'
        and f.incremental='False' and f.num_threads=1
        group by qid, sf
        order by sf, qid
        """).df()
        print(summary_data)
        
        print("======== DBT-P vs FaDe-I Summary =============")
        summary_data = con.execute("""
        select qid, sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='False' and dbt_data.prune='True'
        and f.incremental='True' and f.num_threads=1
        group by qid, sf
        order by sf, qid
        """).df()
        print(summary_data)
        
        print("======== DBT-P vs FaDe-P Summary =============")
        summary_data = con.execute("""
        select qid, sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='True' and dbt_data.prune='True'
        and f.incremental='False' and f.num_threads=1
        group by qid, sf
        order by sf, qid
        """).df()
        print(summary_data)
        
        print("======== DBT-P vs FaDe-P-I Summary =============")
        summary_data = con.execute("""
        select qid, sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='True' and dbt_data.prune='True'
        and f.incremental='True' and f.num_threads=1
        group by qid, sf
        order by sf, qid
        """).df()
        print(summary_data)
        
        

        if False:
            print("======== DBT-P vs FaDe-P Summary Per Prob =============")
            summary_data = con.execute("""
            select qid, sf, prob,
            avg(dbt_data.eval_time) as deval,
            avg(f.eval_time_ms) as feval,
            max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
            avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
            min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
            from dense_data as f JOIN dbt_data
            USING (qid, sf, prob)
            where f.n=1 and f.prune='True' and dbt_data.prune='True'
            and f.incremental='False' and f.num_threads=1
            group by qid, sf, prob
            order by sf, qid, prob
            """).df()
            print(summary_data)
            
            print("======== DBT-P vs FaDe Summary Per Prob Prune =============")
            summary_data = con.execute("""
            select qid, sf, prob,
            avg(dbt_data.eval_time) as deval,
            avg(f.eval_time_ms) as feval,
            max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
            avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
            min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
            from dense_data as f JOIN dbt_data
            USING (qid, sf, prob)
            where f.n=1 and f.prune='False' and dbt_data.prune='True'
            and f.incremental='False' and f.num_threads=1
            group by qid, sf, prob
            order by sf, qid, prob
            """).df()
            print(summary_data)

# figure 1: single intervention latency
if single:
    cat = "system"
    if plot:
        dbt_q = ""
        if include_dbt:
            dbt_q = f"""
            UNION ALL select 'DBT-p' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental from dbt_data where prob={dbt_prob} and prune='True'
            UNION ALL select 'DBT' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental from dbt_data where prob={dbt_prob} and prune='False'
            """
        #UNION select 'Fade-p-I' as system, query, prune, sf, sf_label,  eval_time_ms, incremental
        #    from dense_data where n=1 and num_threads=1  and prune='True' and prob={dbt_prob} and incremental='True'
        #UNION ALL select 'Fade-I' as system, query, prune, sf, sf_label, eval_time_ms, incremental
        #    from dense_data where n=1 and num_threads=1  and prune='False' and prob={dbt_prob} and incremental='True'
        #UNION ALL select 'Q' as system, query, 'False' as prune, sf, sf_label, query_timing*1000 as eval_time, 'False' as incremental
        #    from lineage_data
        #UNION ALL select 'Q+' as system, query, 'False' as prune, sf, sf_label, lineage_timing*1000 as eval_time, 'False' as incremental
        #    from lineage_data
        #UNION ALL select 'Q++' as system, query, 'False' as prune, sf, sf_label, ksemimodule_timing*1000 as eval_time, 'False' as incremental
        fig1_data = con.execute(f"""
            select 'Fade-p' as system, query, prune, sf, sf_label,  eval_time_ms, incremental
                from dense_data where n=1 and num_threads=1 and prune='True' and prob={dbt_prob} and incremental='False'
            UNION ALL select 'Fade' as system, query, prune, sf, sf_label, eval_time_ms, incremental
                from dense_data where n=1 and num_threads=1  and prune='False' and prob={dbt_prob} and incremental='False'
                {dbt_q} """).df()
        postfix = """
        data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
        data$sf_label = factor(data$sf_label, levels=c('SF=1.0', 'SF=5.0', 'SF=10.0'))
            """
        print("======== Single Summary =============")
        summary_data = con.execute("""
        select sf, system, avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
        from fig1_data
        group by sf, system
        """).df()
        print(summary_data)
        
        summary_data = con.execute("""
        select sf, query,
        f.eval_time_ms as fms, fp.eval_time_ms as fpms,
        d.eval_time_ms as dms, dp.eval_time_ms as dpms,
        f.eval_time_ms / fp.eval_time_ms as fp_speedup,
        d.eval_time_ms / f.eval_time_ms as f_d_speedup,
        dp.eval_time_ms / f.eval_time_ms as f_dp_speedup,
        d.eval_time_ms / fp.eval_time_ms as fp_d_speedup,
        dp.eval_time_ms / fp.eval_time_ms as fp_dp_speedup
        from (select * from fig1_data where system='Fade-p') as fp JOIN
        (select * from fig1_data where system='Fade') as f using (query, sf) JOIN
        (select * from fig1_data where system='DBT') as d using (query, sf) JOIN
        (select * from fig1_data where system='DBT-p') as dp using (query, sf)
        """).df()
        print(summary_data)
    
        if plot:
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
            
            fig1_data_sf1 = con.execute("select * from fig1_data where sf=1 and system<>'Q' and system<>'Q+' and system <>'Q++'").df()
            p = ggplot(fig1_data_sf1, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
            p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
            p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
            p += legend_side
            p += facet_grid(".~sf_label", scales=esc("free_y"))
            ggsave("figures/fade_single_sf1_sys.png", p, postfix=postfix, width=5, height=3, scale=0.8)
            
        # fig 1:
        # x-axis prob, y-axis dbt normalized latency against fade
        dbt_fade_data = con.execute("""
        select 'FaDe' as system, sf, query, prob, fade.incremental, fade.prune_label, dbt.prune dbt_prune, fade.prune, fade.num_threads,
        (dbt.eval_time_ms / fade.eval_time_ms) as nor,
        dbt.eval_time_ms, fade.eval_time_ms
        from (select * from dbt_data where prune='True') as dbt JOIN
        (select * from  dense_data where n=1 and num_threads=1) as fade
        USING (query, sf, prob)
        UNION ALL
        select 'IFaDe' as system, sf, query, prob, fade.incremental, fade.prune_label, dbt.prune dbt_prune, fade.prune, fade.num_threads,
        (dbt.eval_time_ms / fade.eval_time_ms) as nor,
        dbt.eval_time_ms, fade.eval_time_ms
        from (select * from dbt_data where prune='True') as dbt JOIN
        (select * from  incremental_data where n=1 and num_threads=1 and sf=1) as fade
        USING (query, sf, prob)
        """).df()
        postfix = """
        data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
            """
        dbt_fade_data = con.execute("select * from dbt_fade_data where system='IFaDe'").df()
        if plot:
            p = ggplot(dbt_fade_data, aes(x='prob',  y="nor", color="query", fill="query",  group="query"))
            p += geom_line(stat=esc('identity')) 
            p += geom_hline(aes(yintercept=1, linetype=esc("Fade")))
            p += axis_labels('Deletion Probability (log)', "Normalized Latency (log)", "log10", "log10",
                ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
                xkwargs=dict(breaks=[0.001, 0.01, 0.1,],  labels=list(map(esc,['0.001','0.01','0.1']))),
                )
            p += legend_side
            p += facet_grid(".~prune_label~incremental", scales=esc("free_x"), space=esc("free_x"))
            ggsave(f"figures/fade_dbt_vs_fade.png", p, postfix=postfix, width=5, height=3, scale=0.8)
            
            dbt_fade_data_prune = con.execute("""select * from dbt_fade_data where prune_label='P'""").df()
            p = ggplot(dbt_fade_data_prune, aes(x='prob',  y="nor", color="query", fill="query",  group="query"))
            p += geom_line(stat=esc('identity')) 
            p += geom_hline(aes(yintercept=1, linetype=esc("Fade")))
            p += axis_labels('Deletion Probability (log)', "Normalized Latency  (log)", "log10", "log10",
                ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
                xkwargs=dict(breaks=[0.001, 0.01, 0.1,],  labels=list(map(esc,['0.001','0.01','0.1']))),
                )
            p += legend_side
            ggsave(f"figures/fade_dbt_vs_fade_prune.png", p, postfix=postfix,  width=3, height=2, scale=0.8)

if dense:
    #dense_data_v2 = get_data(f"fade_data/dense_all_sf1_0.1.csv", 1000)
    dense_data_v2 = get_data(f"fade_data/dense_sf1_v4.csv", 1000)
    #dense_data_v2["cat"] = dense_data_v2.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)

    #data$cat = factor(data$cat, levels=c('1 Threads', '2 Threads', '4 Threads', '8 Threads', '1 Threads+SIMD', '2 Threads+SIMD', '4 Threads+SIMD', '8 Threads+SIMD'))
    postfix = """
    data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
        """

    print("======== DENSE Batching=============")

    print(con.execute("select * from dense_data_v2 where incremental='False' and n>1").df())
    # Batching latency performance varying number of interventions
    fig2_data = con.execute(f"""select 
        (base.eval_time_ms * t1.n)/ t1.eval_time_ms as speedup,
        sf, prune, t1.query, t1.cat, qid, t1.n, t1.num_threads, t1.distinct,
        t1.num_threads, t1.is_scalar, t1.eval_time_ms, base.eval_time_ms*t1.n as single_eval
        from ( select * from dense_data_v2 where incremental='False' and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1) as t1 JOIN
             ( select * from dense_data_v2 where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1) as base
             USING (sf, qid, prune) """).df()
    print(fig2_data)
    
    print(con.execute("""select max(speedup), avg(speedup), min(speedup), 
    avg(eval_time_ms), avg(single_eval), max(eval_time_ms), max(single_eval),
    min(eval_time_ms), min(eval_time_ms) from fig2_data where prune='False'""").df())
    cat = 'distinct'
    if plot:
        p = ggplot(fig2_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
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
        
        fig2_data_noprune = con.execute("select * from fig2_data where prune='False'").df()
        p = ggplot(fig2_data_noprune, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_speedup_sf1_noprune.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        p = ggplot(fig2_data_noprune, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_batching_latency_sf1_noprune.png", p, postfix=postfix, width=8, height=3, scale=0.8)

    print("======== DENSE Pruning =============")
    prune_data = con.execute(f"""select sf, n, t1.query, t1.cat, qid, t1.n, t1.num_threads, t1.distinct,
        t1.num_threads, t1.is_scalar, (base.eval_time_ms * t1.n)/ t1.eval_time_ms as speedup,
        t1.eval_time_ms
        from ( select * from dense_data_v2 where incremental='False' and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1 and prune='True') as t1 JOIN
             ( select * from dense_data_v2 where incremental='False' and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1 and prune='False') as base
             USING (sf, qid, n) """).df()
    print(prune_data)
    cat = 'distinct'
    if plot:
        p = ggplot(prune_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_pruning_latency_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

        # Batching speedup over single intervention performance varying number of interventions
        p = ggplot(prune_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_pruning_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

    print("======== DENSE Vec =============")

    # Additional vectorization speedup over batched execution of  varying numbers of interventions
    fig3_data = con.execute("""select sf, prune, qid, n, t1.query, t1.cat, t1.num_threads,
        t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms*n as single_eval from fig2_data as base JOIN 
            (select * from dense_data_v2 where num_threads=1 and is_scalar='False' and sf=1) as t1
            USING (sf, qid, n, prune)""").df()

    print(fig3_data)
    print(con.execute("""select max(speedup), avg(speedup), min(speedup), 
    avg(eval_time_ms), avg(single_eval), max(eval_time_ms), max(single_eval),
    min(eval_time_ms), min(eval_time_ms) from fig3_data where prune='False'""").df())
    if plot:
        p = ggplot(fig3_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_vec_latency_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

        p = ggplot(fig3_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_vec_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)

    print("======== DENSE Threads =============")
    # Additional threading speedup for 2, 4 and 8 threads over batched execution
    fig4_data = con.execute("""select sf, qid, prune,  n, t1.query, t1.cat, t1.num_threads,
        t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
        base.eval_time_ms*n as single_eval from fig2_data as base JOIN 
            (select * from dense_data_v2 where is_scalar='True' and sf=1 and n=2560) as t1
            USING (sf, qid, n, prune)""").df()

    print(fig4_data)
    print(con.execute("""select max(speedup), avg(speedup), min(speedup), 
    avg(eval_time_ms), avg(single_eval), max(eval_time_ms), max(single_eval),
    min(eval_time_ms), min(eval_time_ms) from fig4_data where prune='False'""").df())
    if plot:
        cat = 'num_threads'
        p = ggplot(fig4_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_threading_latency_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)
        
        p = ggplot(fig4_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup", "discrete")
        p += legend_side
        p += facet_grid(".~sf~prune", scales=esc("free_y"))
        ggsave("figures/fade_threading_speedup_sf1.png", p, postfix=postfix, width=8, height=3, scale=0.8)


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
        print(scalar_data)
        p = ggplot(scalar_data, aes(x='num_threads',  y="throughput", color=cat, fill=cat, group=cat))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('# Threads', "Interventions / Sec (log)", "discrete", "log10",
                ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
                xkwargs=dict(breaks=[1,2,4,8], labels=list(map(esc,['1.0', '2.0','4.0','8.0']))))
        p += legend_bottom
        p += facet_grid(".~sf_label", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_throughput_vec_p.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
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

        scalar_data = con.execute("""select * from data_distinct where 
        ( (prune_label='NP' and is_scalar='False') or (prune_label='P' and is_scalar='True') or (prune_label='P' and is_scalar='False') ) """).df()
        p = ggplot(scalar_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf~num_threads", scales=esc("free_y"))
        ggsave("figures/fade_2560_sf1_opt_vary_threads_speedup.png", p, postfix=postfix, width=10, height=2.5, scale=0.8)
        

        # dbt vs fade
        data_dbt_distinct = con.execute(f"""select sf, t1.prune_label, t1.query, t1.cat,qid, t1.n, t1.distinct, t1.num_threads, t1.is_scalar,
                            t1.eval_time_ms, base.eval_time_ms, base.eval_time_ms*t1.n,
                            (t1.n*base.eval_time_ms) / t1.eval_time_ms as speedup
        from ( select * from dense_data_v2 where num_threads=8 and is_scalar='False' and prune='True') as t1 JOIN
             ( select * from dbt_data  where prob={dbt_prob} and prune='True') as base 
             USING (sf, qid) """).df()
        # fade vs fade
        data_distinct = con.execute(f"""select sf, t1.prune_label, t1.query, t1.cat,qid, t1.n, t1.distinct, t1.num_threads, t1.is_scalar,
                            t1.eval_time_ms, base.eval_time_ms, base.eval_time_ms*t1.n,
                            (t1.n*base.eval_time_ms) / t1.eval_time_ms as speedup,
                            t1.n / (t1.eval_time_ms/1000.0) as throughput
        from ( select * from dense_data_v2 where num_threads=8 and is_scalar='False' and prune='True') as t1 JOIN
             ( select * from dense_data_v2 where incremental='False' and n=1 and num_threads=1 and is_scalar='True' and sf=1 and prob=0.1 and prune='False') as base
             USING (sf, qid) """).df()
        print("======== DENSE best =============")
        #print(data_distinct)
        cat = 'distinct'
        p = ggplot(data_distinct, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Latency (ms, log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_best_distinct_latency_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        # TODO: add dbt comparison
        p = ggplot(data_distinct, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
        p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_best_distinct_speedup_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

        # x-axis: batch size, y-axis: throughput
        cat = "query"
        p = ggplot(data_distinct, aes(x='n',  y="throughput", color=cat, fill=cat, group=cat))
        p += geom_point(stat=esc('identity'))
        p += geom_line()
        p += axis_labels('Batch Size', "Interventions / Sec (log)", "discrete", "log10")
        p += legend_side
        p += facet_grid(".~sf", scales=esc("free_y"))
        ggsave("figures/fade_best_distinct_throughput_sf1.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)



# TODO: add incremental single evaluation. hold the indices of elements to be deleted. this would be equivalent eval to dbtoast
include_search = True
if include_search:
    data_spec = pd.read_csv('fade_data/dense_delete_spec_sf1.csv')
    data_search = pd.read_csv('fade_data/search_all_feb25.csv')
    data_cube = pd.read_csv('fade_data/cube_search.csv')
    data_spec["itype"] = "DD"
    data_search["itype"] = "S"
    data_cube["itype"] = "GB"
    data = pd.concat([data_spec, data_search], axis=0)
    
    data["eval_time_ms"]=1000*data["eval_time"]
    data["query"] = "Q"+data["qid"].astype(str)
    data["cat"] = data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
    data["cat"] = data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    data["cat"] = data.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)
    data["n"] = data["distinct"]

    data_cube["eval_time_ms"]=1000*data_cube["timing"]
    data_cube["query"] = "Q"+data_cube["qid"].astype(str)
    data_cube["cat"] = data_cube.apply(lambda row: row["itype"] + " " + str(row["num_threads"]) + "W", axis=1)
    data_cube["n"] = data_cube["distinct"]

    # SEARCH (non-incremental, incremental) vs DENSE_DELETE_SPEC
    # x-axis: n_interventions, y-axis: latency, facet: query, cat: itype+incremental
    # speedup: base x-axis DENSE_DELETE_SPEC

    print(con.execute("select * from data where n>1 and itype='S' and incremental='False' and is_scalar='True'").df())
    if plot:
        postfix = """
        data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
            """
        plot_data = con.execute("""
        select query, eval_time_ms, prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data where n>1 and itype='S' and incremental='True'
        UNION ALL
        select query, eval_time_ms, prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data where n>1 and itype='S' and incremental='False' and is_scalar='True'
        UNION ALL
        select query, eval_time_ms, False as prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data_cube
        UNION ALL
        select query, eval_time_ms, True as prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data_cube
                """).df()
        cat = "cat"
        p = ggplot(plot_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Run time (ms, log)", "discrete", "log10")
        p += legend_bottom
        p += facet_grid(".~prune~n~sf", scales=esc("free_y"))
        ggsave("figures/fade_search_fade25.png", p, postfix=postfix, width=10, height=10, scale=0.8)
        sf1_plot_data = con.execute("select * from plot_data where sf=1 and num_threads=8 and prune='True'").df()
        print("check")
        print(sf1_plot_data)
        p = ggplot(sf1_plot_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Run time (ms, log)", "discrete", "log10")
        p += legend_bottom
        p += facet_grid(".~n", scales=esc("free_y"))
        ggsave("figures/fade_search_fade25_sf1.png", p, postfix=postfix, width=6, height=2.5, scale=0.8)

    if include_dbt:
        # fig 2:
        # reexecute
        fig1_data = con.execute(f"""
            select 'Fade-prune+' as system, query, prune, sf, eval_time_ms from dense_data where n=1 and num_threads=1  and prune='True' and incremental='False'
            UNION ALL select 'Fade+' as system, query, prune, sf, eval_time_ms from dense_data where n=1 and num_threads=1  and prune='False' and incremental='False'
            UNION ALL select 'Fade-prune' as system, query, prune, sf, eval_time_ms from data where n=1 and num_threads=1 and prune='True'
            UNION ALL select 'Fade' as system, query, prune, sf, eval_time_ms from data where n=1 and num_threads=1 and prune='False'
            UNION ALL select 'Q' as system, query, 'False' as prune, sf, query_timing*1000 as eval_time from lineage_data
            UNION ALL select 'Q+' as system, query, 'False' as prune, sf, lineage_timing*1000 as eval_time from lineage_data
            UNION ALL select 'DBT-prune' as system, query, prune, sf, eval_time_ms as eval_time from dbt_data where prob={dbt_prob} and prune='True'
            UNION ALL select 'DBT' as system, query, prune, sf, eval_time_ms as eval_time from dbt_data where prob={dbt_prob} and prune='False'
                """).df()
        cat = "system"
        if plot:
            p = ggplot(fig1_data, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
            p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
            p += axis_labels('Query', 'Latency (ms) log', 'discrete', 'log10')
            p += legend_side
            p += facet_grid(".~sf", scales=esc("free_y"))
            ggsave("figures/fade_dbt_single.png", p, width=8, height=3, scale=0.8)
        # x-axis query, y-axis latency

