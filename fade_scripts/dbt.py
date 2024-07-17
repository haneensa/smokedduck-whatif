# TODO: rerun dbt non pruned
import duckdb
import pandas as pd
from pygg import *
from utils import get_data
from utils import legend, legend_bottom, legend_side

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
postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
"""
def summary(table, attrs, extra="", whr=""):
    print(table, attrs, extra, whr)
    return con.execute(f"""
    select  {attrs}, 
    {extra}
    max(eval_time_ms) as eval_time_max, avg(eval_time_ms) as eval_time_avg, min(eval_time_ms) as eval_time_min,
    from {table}
    {whr}
    group by {attrs}
    order by {attrs}
    """).df()

    
lineage_data = pd.read_csv('fade_data/lineage_overhead_all_april30_v2.csv')
lineage_data["query"] = "Q"+lineage_data["qid"].astype(str)
lineage_data["sf_label"] = "SF="+lineage_data["sf"].astype(str)

if True:
    # fade using incremental data processing with sparse intervention representation
    #single_data_all = get_data("single_fade_april6.csv", 1000)
    #single_data_all = get_data("fade_data/dense_single_vary_probs_april7.csv", 1000)
    #single_data_all = get_data(f"fade_data/single_dense_vary_probs_nofilterOnprune.csv", 1000)
    #single_data_all = get_data(f"fade_data/forward_backward_all_probs_april26.csv", 1000)
    single_data_all = get_data(f"fade_signle_jul10.csv", 1000)
    single_data_all = con.execute("""select
    sf, qid, itype, prob, incremental, use_duckdb, is_scalar, prune,
    num_threads, n, batch,
    avg(post_time) as post_time,
    avg(gen_time) as gen_time,
    avg(prep_time) as prep_time,
    avg(compile_time) as compile_time,
    avg(eval_time) as eval_time,
    avg(prune_time) as prune_time,
    avg(lineage_time) as lineage_time,
    avg(ksemimodule_timing) as ksemimodule_timing,
    spec,
    avg(lineage_count) as lineage_count,
    avg(lineage_count_prune) as lineage_count_prune,
    avg(lineage_size_mb) as lineage_size_mb,
    avg(lineage_size_mb_prune) as lineage_size_mb_prune,
    use_gb_backward_lineage,
    avg(code_gen_time) as code_gen_time, avg(data_time) as data_time,
    avg(eval_time_ms) as eval_time_ms,
    avg(prune_time_ms) as prune_time_ms,
    query, cat, sf_label, prune_label, 
    from single_data_all
    group by
    sf, qid, query, cat, sf_label, prune_label, itype, incremental, use_duckdb, is_scalar, prune,
    num_threads, n, batch, prob, spec, use_gb_backward_lineage
    """).df()
    fade_scale = con.execute("select * from single_data_all where itype='SCALE_RANDOM'").df()
    fade_delete = con.execute("select * from single_data_all where itype='DENSE_DELETE'").df()
    #dbt_data = get_data("fade_data/dbtoast_delete_april21.csv", 1)
    #dbt_data = get_data("fade_data/dbtoast.csv", 1)
    #dbt_data = con.execute("select * from dbt_data UNION ALL select * from dbt_data_no_prune where prune='False'").df()
    #dbt_data = get_data("fade_data/dbtoast_may1.csv", 1)
    #dbt_data = con.execute("select * from dbt_data where itype='DELETE'").df()
    dbt_data_all = get_data("fade_data/dbtoast_july9.csv", 1)
    dbt_data = con.execute("""select
    avg(eval_time) as eval_time,
    sf, qid, itype, prob, incremental, use_duckdb, is_scalar, prune,
    num_threads, n, batch,
    avg(post_time) as post_time,
    avg(gen_time) as gen_time,
    avg(prep_time) as prep_time,
    avg(compile_time) as compile_time,
    avg(prune_time) as prune_time,
    avg(eval_time_ms) as eval_time_ms,
    avg(prune_time_ms) as prune_time_ms,
    query, cat, sf_label, prune_label, 
    from dbt_data_all
    group by
    sf, qid, query, cat, sf_label, prune_label, itype, incremental, use_duckdb, is_scalar, prune,
    num_threads, n, batch, prob
    """).df()
    # compute average
    dbt_data["cat"] = dbt_data.apply(lambda row: "DBT_prune" if row["prune"] else "DBT", axis=1)
    print(dbt_data)

    cat = "query"
    p = ggplot(dbt_data, aes(x='prob',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_line(stat=esc('identity')) + geom_point(stat=esc('identity'))
    p += axis_labels('Prob', "Run time (ms)", "continuous", "log10")
    p += legend_bottom
    p += facet_grid(".~prune~num_threads~itype", scales=esc("free_y"))
    ggsave("figures/dbt.png", p, postfix=postfix,width=10, height=8, scale=0.8)

    # TODO: run fade dense again for all probs
    #data = pd.read_csv('dense_delete_all_sf1.csv')
    #dense_data = get_data('fade_data/dense_all_sf1_v2.csv', 1000)
    #dense_data = get_data('fade_data/dense_sf1_v4.csv', 1000)
    #dense_data = get_data(f"fade_data/batch_dense_0.1_nofilterOnprune.csv", 1000)
    dense_data = get_data(f"fade_data/forward_backward_0.1_april26.csv", 1000)
    dense_data["cat"] = dense_data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
    dense_data["cat"] = dense_data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    dense_data["cat"] = dense_data.apply(lambda row: row["cat"] + "_b" if row["use_gb_backward_lineage"] else  row["cat"] + "_f", axis=1)
    
    provsql_data = [{'qid': 1, 'with_prov': False, 'expid': 0, 'time_ms': 3927.064}, {'qid': 3, 'with_prov': False, 'expid': 1, 'time_ms': 476.422}, {'qid': 5, 'with_prov': False, 'expid': 2, 'time_ms': 1234.733}, {'qid': 7, 'with_prov': False, 'expid': 3, 'time_ms': 892.848}, {'qid': 9, 'with_prov': False, 'expid': 4, 'time_ms': 1315.129}, {'qid': 10, 'with_prov': False, 'expid': 5, 'time_ms': 1389.753}, {'qid': 12, 'with_prov': False, 'expid': 6, 'time_ms': 501.138}, {'qid': 3, 'with_prov': True, 'expid': 8, 'time_ms': 5544.203}, {'qid': 5, 'with_prov': True, 'expid': 9, 'time_ms': 1624.657}, {'qid': 7, 'with_prov': True, 'expid': 10, 'time_ms': 1334.478}, {'qid': 9, 'with_prov': True, 'expid': 11, 'time_ms': 68457.627}, {'qid': 10, 'with_prov': True, 'expid': 12, 'time_ms': 23379.272}, {'qid': 12, 'with_prov': True, 'expid': 13, 'time_ms': 3617.771}]
    df_provsql = pd.DataFrame(provsql_data)
    df_provsql["query"] = "Q"+df_provsql["qid"].astype(str)
    df_provsql["sf_label"] = "SF=1.0"

    # figure 1: single intervention latency
    dbt_prob=0.1
    single_data = con.execute(f"""
        select 'FaDE' as system, query, prune, sf, sf_label, eval_time_ms, incremental
            from fade_delete where n=1 and num_threads=1  and prune='False' and prob={dbt_prob} and incremental='False' and use_gb_backward_lineage='False'
        UNION ALL select 'FaDE-Prune' as system, query, prune, sf, sf_label,  eval_time_ms, incremental
            from fade_delete where n=1 and num_threads=1 and prune='True' and prob={dbt_prob} and incremental='False' and use_gb_backward_lineage='False'
        UNION ALL select 'DBT-Prune' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
            from dbt_data where prob={dbt_prob} and prune='True'
            and itype='DELETE'
        UNION ALL select 'DBT' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
            from dbt_data where prob={dbt_prob} and prune='False'
            and itype='DELETE'
        UNION ALL select 'ProvSQL' as system, query, 'False' as prune, 1 as sf, sf_label, time_ms as eval_time, 'False' as incremental
            from df_provsql where with_prov='True'
        UNION ALL select 'Postgres' as system, query,  'False' as prune, 1 as sf, sf_label, time_ms as eval_time, 'False' as incremental
            from df_provsql where with_prov='False'
            """).df()

    cat = "system"

    single_data_sf1 = con.execute("select * from single_data where sf=1").df()
    p = ggplot(single_data_sf1, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.8)
    p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
    p += legend_bottom
    ggsave("figures/fade_single_sf1.png", p, postfix=postfix, width=5, height=3, scale=0.8)
    
    single_data_sf1 = con.execute("select * from single_data where sf=1").df()
    p = ggplot(single_data_sf1, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
    p += legend_side
    ggsave("figures/fade_single_sf1_side.png", p, postfix=postfix, width=7, height=2.5, scale=0.8)

    print(con.execute(
                "select * from lineage_data where sf=1 and model='lineage'").df())
    single_data_speedup = con.execute(f"""
        select t1.system, t1.query, t1.sf, t1.sf_label, t1.eval_time, t2.eval_time, t2.eval_time / t1.eval_time as speedup from 
            (select 'FaDE' as system, query, sf, sf_label,  eval_time_ms as eval_time,
                from fade_delete where n=1 and num_threads=1 and prune='True' and prob={dbt_prob} and incremental='False' and use_gb_backward_lineage='False'
                ) as t1 JOIN (
                select sf, query, sf_label, query_timing*1000 as eval_time from lineage_data where sf=1 and model='lineage' and workload='tpch'
                ) as t2 USING (query, sf, sf_label)
        UNION ALL select t1.system, t1.query, t1.sf, t1.sf_label, t1.eval_time, t2.eval_time, t2.eval_time / t1.eval_time as speedup from
                    (select 'IVM' as system, query, sf, sf_label, eval_time_ms as eval_time from dbt_data where prob={dbt_prob} and prune='False'
            and itype='DELETE') as t1 JOIN (
                select sf, query, sf_label, query_timing*1000 as eval_time from lineage_data where sf=1 and model='lineage' and workload='tpch'
            ) as t2 USING (query, sf, sf_label)
        UNION ALL select t1.system, t1.query, t1.sf, t1.sf_label, t1.eval_time, t2.eval_time, t2.eval_time / t1.eval_time as speedup from 
                    ( select 'Circuit' as system, query, 1 as sf, sf_label, time_ms as eval_time, 
                        from df_provsql where with_prov='True') as t1 JOIN (select 'Postgres' as system, query, 1 as sf,
                        sf_label, time_ms as eval_time from df_provsql where with_prov='False') as t2 USING (query, sf, sf_label)
            """).df()

    postfix_sample = """
    data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
    data$system = factor(data$system, levels=c('IVM', 'FaDE', 'Circuit'))
    """
    print(con.execute("select * from single_data_speedup order by system, query").df())
    # show DBT, ProvSQL, Original Query, ProvSQL, Fade-prune as Fade
    p = ggplot(single_data_speedup, aes(x='query', y='speedup', color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', 'Original Query / WhatIf (log)', 'discrete', 'log10',
        ykwargs=dict(breaks=[0.1,0, 10,100,1000,10000],  labels=list(map(esc,['0.1','0', '10','100','1000','10000']))),
            )
    p += legend_bottom
    ggsave("figures/fade_single_sf1_sample.png", p, postfix=postfix_sample, width=7, height=4, scale=0.8)

    # TODO: show speedup or slowdowns compared to base query -> one bar per system
    # show ProvSQL, Original Query, ProvSQL
    single_data_prov = con.execute("""select * from single_data_speedup where sf=1 and system IN ('IVM', 'Circuit')""").df()
    p = ggplot(single_data_prov, aes(x='query', y='speedup', color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', 'Original Query / WhatIf (log)', 'discrete', 'log10',
        ykwargs=dict(breaks=[0.1, 0, 10,100,1000,10000],  labels=list(map(esc,['0.1','0', '10','100','1000','10000']))),
            )
    p += legend_bottom
    ggsave("figures/fade_single_sf1_provsql_dbt.png", p, postfix=postfix_sample, width=7, height=4, scale=0.8)
    
    fade_delete = con.execute("select * from fade_delete where use_gb_backward_lineage='False' and itype='DENSE_DELETE'").df()
    dense_data = con.execute("select * from dense_data where use_gb_backward_lineage='False'").df()

    # fig 1:
    # x-axis prob, y-axis dbt normalized latency against fade
    dbt_fade_data = con.execute("""
    select 'DBT-Prune / ' || fade.prune_label as system, sf, query, dbt.prob, fade.incremental, fade.prune_label, dbt.prune, 
    fade.prune as fprune, fade.num_threads,
    (dbt.eval_time_ms / fade.eval_time_ms) as nor,
    dbt.eval_time_ms, fade.eval_time_ms
    from (select * from dbt_data where prune='True' and itype='DELETE') as dbt JOIN
    (select * from  fade_delete where n=1 and num_threads=1 and incremental='False') as fade
    USING (query, sf, prob)
    """).df()

    p = ggplot(dbt_fade_data, aes(x='prob',  y="nor", color="query", fill="query"))
    p += geom_line(stat=esc('identity')) 
    p += axis_labels('Deletion Probability (log)', "Speedup (log)", "log10", "log10",
        ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
        xkwargs=dict(breaks=[0.001, 0.01, 0.05, 0.1,0.5],  labels=list(map(esc,['0.001','0.01','0.05', '0.1', '0.5']))),
        )
    p += legend_bottom
    p += legend_side
    p += geom_hline(aes(yintercept=1))
    p += facet_grid(".~system", scales=esc("free_y"))
    ggsave(f"figures/fade_dbt_vs_fade.png", p, postfix=postfix, width=7, height=2.5, scale=0.8)

    dbt_fade_data_prune = con.execute("""select * from dbt_fade_data where fprune='True'""").df()
    p = ggplot(dbt_fade_data_prune, aes(x='prob',  y="nor", color="query", fill="query"))
    p += geom_line(stat=esc('identity')) 
    p += axis_labels('Deletion Probability (log)', "Speedup  (log)", "log10", "log10",
        ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
        xkwargs=dict(breaks=[0.001, 0.01, 0.05, 0.1,0.5],  labels=list(map(esc,['0.001','0.01','0.05', '0.1', '0.5']))),
        )
    p += legend_side
    p += geom_hline(aes(yintercept=1))
    p += facet_grid(".~system", scales=esc("free_y"))
    ggsave(f"figures/fade_dbt_vs_fade_prune.png", p, postfix=postfix,  width=5, height=3, scale=0.8)


    def get_per_prob_summary(fade_data, dbt_prune, fade_prune, is_incremental):
        return con.execute(f"""
        select qid, sf, prob,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from dense_data as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='{fade_prune}' and dbt_data.prune='{dbt_prune}'
        and f.incremental='{is_incremental}' and f.num_threads=1
        group by qid, sf, prob
        order by sf, qid, prob
        """).df()

    def get_summary_hack(fade_data, dbt_prune, fade_prune, is_incremental, group):
        return con.execute(f"""select {group} sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        min(dbt_data.eval_time) as min_deval,
        min(f.eval_time_ms) as min_feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        max(f.eval_time_ms / dbt_data.eval_time) as max_slowdown,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from {fade_data} as f JOIN dbt_data
        USING (qid, sf)
        where f.n=1 and f.prune='{fade_prune}' and dbt_data.prune='{dbt_prune}'
        and dbt_data.prob=0.1 and  f.prob=0.1
        and f.incremental='{is_incremental}' and f.num_threads=1
        group by {group} sf
        order by {group} sf""").df()

    def get_summary(fade_data, dbt_prune, fade_prune, is_incremental, group):
        return con.execute(f"""select {group} sf,
        avg(dbt_data.eval_time) as deval,
        avg(f.eval_time_ms) as feval,
        max(dbt_data.eval_time / f.eval_time_ms) as max_speedup,
        avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
        min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
        from {fade_data} as f JOIN dbt_data
        USING (qid, sf, prob)
        where f.n=1 and f.prune='{fade_prune}' and dbt_data.prune='{dbt_prune}'
        and dbt_data.prob=0.1 and f.prob=0.1
        and f.incremental='{is_incremental}' and f.num_threads=1
        group by {group} sf
        order by {group} sf""").df()


    if print_summary:
        print("======== DBT vs FaDe Summary =============")
        # fade_data, dbt_prune, fade_prune, is_incremental, group
        summary_data = get_summary_hack("single_data_all", "False", "False", "False", "qid,")
        print("***", summary_data)
        summary_data = get_summary_hack("single_data_all", "False", "False", "False", "")
        print("***", summary_data)
        
        
        print("======== DBT-P vs FaDe Summary =============")
        # fade_data, dbt_prune, fade_prune, is_incremental, group
        summary_data = get_summary("single_data_all", "True", "False", "False", "qid,")
        summary_data = get_summary_hack("single_data_all", "True", "False", "False", "qid,")
        print("***", summary_data)
        
        summary_data = get_summary("single_data_all", "True", "False", "False", "prob,")
        summary_data = get_summary_hack("single_data_all", "True", "False", "False", "")
        print("***", summary_data)
        print(f"{round(summary_data['avg_speedup'][0])}X (min: {summary_data['min_speedup'][0]:.4f}, max: {round(summary_data['max_speedup'][0])})")
        
        print("======== DBT-P vs FaDe-P Summary =============")
        # fade_data, dbt_prune, fade_prune, is_incremental, group
        summary_data = get_summary("single_data_all", "True", "True", "False", "qid,")
        print("***", summary_data)
        
        summary_data = get_summary("single_data_all", "True", "True", "False", "prob,")
        print("***", summary_data)
        print(f"{round(summary_data['avg_speedup'][0])}X (min: {summary_data['min_speedup'][0]:.4f}, max: {round(summary_data['max_speedup'][0])})")
        
        
    # print summary
    print("======== DBT Eval Summary =============")
    extra = "max(gen_time) as prune_time_max, avg(gen_time) as prune_time_avg, min(gen_time) as prune_time_min,"
    print(summary("dbt_data", "sf, prune", extra))
    dbtruntime_data = summary("dbt_data", "sf, prune", extra)
    
    speedup_data = con.execute("""
    select sf,
    max(dbt.eval_time_ms/dbtp.eval_time_ms) as speedup_max,
    avg(dbt.eval_time_ms/dbtp.eval_time_ms) as speedup_avg,
    min(dbt.eval_time_ms/dbtp.eval_time_ms) as speedup_min
    from (select * from dbt_data where prune='True') as dbtp
    JOIN (select * from dbt_data where prune='False') as dbt
    USING (sf, qid, prob)
    group by sf
    """).df()
    print(speedup_data)

    print("======== Single Summary =============")
    single_data_summary = con.execute("""
    select sf, system, avg(eval_time_ms) as latency_avg, max(eval_time_ms) as latency_max, min(eval_time_ms) as latency_min
    from single_data
    group by sf, system
    """).df()
    print(single_data_summary)
    
    single_summary_all = con.execute("""
    select sf, prune,
    avg(fade.eval_time_ms) as avg_fade, 
    min(fade.eval_time_ms) as min_fade, 
    max(fade.eval_time_ms) as max_fade, 
    avg(dbt.eval_time_ms) as avg_dbt,
    min(dbt.eval_time_ms) as min_dbt,
    max(dbt.eval_time_ms) as max_dbt,
    avg(dbt.eval_time_ms / fade.eval_time_ms) as avg_speedup,
    min(dbt.eval_time_ms / fade.eval_time_ms) as min_speedup,
    max(dbt.eval_time_ms / fade.eval_time_ms) as max_speedup
    from (select * from single_data where system='FaDE-Prune' OR system='FaDE') as fade JOIN
    (select * from single_data where system='DBT' OR system='DBT-Prune') as dbt using (query, sf, prune)
    group by sf, prune
    """).df()
    print(single_summary_all)

    single_per_q_summary = con.execute("""
    select sf, query,
    f.eval_time_ms as fms, fp.eval_time_ms as fpms,
    d.eval_time_ms as dms, dp.eval_time_ms as dpms,
    f.eval_time_ms / fp.eval_time_ms as fp_speedup,
    d.eval_time_ms / f.eval_time_ms as f_d_speedup,
    dp.eval_time_ms / f.eval_time_ms as f_dp_speedup,
    d.eval_time_ms / fp.eval_time_ms as fp_d_speedup,
    dp.eval_time_ms / fp.eval_time_ms as fp_dp_speedup
    from (select * from single_data where system='FaDE-Prune') as fp JOIN
    (select * from single_data where system='FaDE') as f using (query, sf) JOIN
    (select * from single_data where system='DBT') as d using (query, sf) JOIN
    (select * from single_data where system='DBT-Prune') as dp using (query, sf)
    order by sf, query
    """).df()
    print(single_per_q_summary)

    single_summary_all = con.execute("""
    select sf, 
    avg(f.eval_time_ms) as fms, avg(fp.eval_time_ms) as fpms,
    avg(d.eval_time_ms) as dms, avg(dp.eval_time_ms) as dpms,
    avg(f.eval_time_ms / fp.eval_time_ms) as fp_speedup,
    avg(d.eval_time_ms / f.eval_time_ms) as f_d_speedup,
    avg(dp.eval_time_ms / f.eval_time_ms) as f_dp_speedup,
    avg(d.eval_time_ms / fp.eval_time_ms) as fp_d_speedup,
    avg(dp.eval_time_ms / fp.eval_time_ms) as fp_dp_speedup
    from (select * from single_data where system='FaDE-Prune') as fp JOIN
    (select * from single_data where system='FaDE') as f using (query, sf) JOIN
    (select * from single_data where system='DBT') as d using (query, sf) JOIN
    (select * from single_data where system='DBT-Prune') as dp using (query, sf)
    group by sf
    """).df()
    print(single_summary_all)

    dbtfull_latency_avg = con.execute("select latency_avg from single_data_summary where system='DBT'").df()["latency_avg"][0]
    fade_avg_latency = con.execute("select latency_avg from single_data_summary where system='FaDE'").df()
    fadep_avg_latency = con.execute("select latency_avg from single_data_summary where system='FaDE-Prune'").df()
    fade_latency_avg = (fade_avg_latency["latency_avg"][0] + fadep_avg_latency["latency_avg"][0])/2.0
    fadep_speedup = single_summary_all["fp_dp_speedup"][0]
    fade_slowdown = single_per_q_summary["f_dp_speedup"][5]
    single_text = f"""The figure shows that \sys and \sys-P outperforms \dbtfull for all queries with an average latency of {fade_latency_avg:.1f} compared to {dbtfull_latency_avg:.1f}.
    When compared to \dbtpruned, \sys-P is always faster than \dbtpruned by {fadep_speedup:.1f}.
    \sys is faster than \dbtpruned for all queries except Q7 with {fade_slowdown:.1f} slowdown.
    """
    print(single_text)

    dbtpruned_speedup_avg = round(speedup_data["speedup_avg"][0])
    dbtpruned_speedup_max = round(speedup_data["speedup_max"][0])
    dbt = []
    dbt_str = []
    dbt.append(round(dbtruntime_data["eval_time_avg"][0]))
    dbt.append(round(dbtruntime_data["eval_time_min"][0]))
    dbt.append(round(dbtruntime_data["eval_time_max"][0]))
    dbt.append(round(dbtruntime_data["eval_time_avg"][1]))
    dbt.append(round(dbtruntime_data["eval_time_min"][1]))
    dbt.append(round(dbtruntime_data["eval_time_max"][1]))
    dbt.append(round(dbtruntime_data["prune_time_avg"][0]))
    dbt.append(round(dbtruntime_data["prune_time_min"][0]))
    dbt.append(round(dbtruntime_data["prune_time_max"][0]))
    for val in dbt:
        if val > 1000:
            dbt_str.append(f"{round(val/1000.0)}s")
        else:
            dbt_str.append(f"{val}ms")

    dbt_vs_fade_text = f"""
    \dbtpruned never process data more than \dbtfull at the cost of an expensive pre-processing step per query.
    This results on an average of {dbtpruned_speedup_avg}$\\times$ (min: 0, max: {dbtpruned_speedup_max}) speedup over \dbtfull,
    reducing average runtime from {dbt_str[0]} (max: {dbt_str[2]}) to 
    {dbt_str[3]} (max: {dbt_str[5]}).
    This benefit comes at a high pruning cost {dbt_str[6]} (min: {dbt_str[7]}, max: {dbt_str[8]}), which includes the cost of reading, filtering, and writing the tables referenced by the query of interest.
    Since \dbtpruned runtime evaluation is always faster than \dbtfull (ignoring the high price of data pruning) in the rest of this section, we mainly compare \sys to \dbtpruned.
    """
    print(dbt_vs_fade_text)


if True:
    #fade_scale = get_data("scaling_sf1_may1.csv", 1000)
    cat = 'query'
    p = ggplot(dbt_data_all, aes(x='prob',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_line(stat=esc('identity')) + geom_point(stat=esc('identity'))
    p += axis_labels('Prob', "Run time (ms)", "continuous", "log10")
    p += legend_bottom
    p += facet_grid(".~prune~num_threads~itype", scales=esc("free_y"))
    ggsave("figures/dbt_scale.png", p,width=10, height=8, scale=0.8)
    
    dbt_fade_data = con.execute("""
    select 'DBT-Prune / ' || fade.prune_label as system, sf, query, dbt.prob, fade.incremental, fade.prune_label, dbt.prune, 
    fade.prune as fprune, fade.num_threads,
    (dbt.eval_time_ms / fade.eval_time_ms) as nor,
    dbt.eval_time_ms, fade.eval_time_ms
    from (select * from dbt_data_all where prune='True' and itype='SCALE') as dbt JOIN
    (select * from  fade_scale where n=1 and num_threads=1 and incremental='False') as fade
    USING (query, sf, prob)
    """).df()

    p = ggplot(dbt_fade_data, aes(x='prob',  y="nor", color="query", fill="query"))
    p += geom_line(stat=esc('identity')) 
    p += axis_labels('Deletion Probability (log)', "Speedup (log)", "log10", "log10",
        ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
        xkwargs=dict(breaks=[0.001, 0.01, 0.05, 0.1,0.5],  labels=list(map(esc,['0.001','0.01','0.05', '0.1', '0.5']))),
        )
    p += legend_bottom
    p += legend_side
    p += geom_hline(aes(yintercept=1))
    p += facet_grid(".~system", scales=esc("free_y"))
    ggsave(f"figures/fade_dbt_vs_fade_scale.png", p, postfix=postfix, width=7, height=2.5, scale=0.8)
    
    dbt_prob=0.1
    single_data_scale = con.execute(f"""
        select 'FaDE-D' as system, query, prune, sf, sf_label, eval_time_ms, incremental
            from fade_scale where n=1 and num_threads=1  and prune='False' and prob={dbt_prob} and incremental='False' and itype='DELETE'
        UNION ALL select 'FaDE-S' as system, query, prune, sf, sf_label, eval_time_ms, incremental
            from fade_scale where n=1 and num_threads=1  and prune='False' and prob={dbt_prob} and incremental='False' and itype='SCALE'
        UNION ALL select 'FaDE-Prune-D' as system, query, prune, sf, sf_label,  eval_time_ms, incremental
            from fade_scale  where n=1 and num_threads=1 and prune='True' and prob={dbt_prob} and incremental='False' and itype='DELETE'
        UNION ALL select 'FaDE-Prune-S' as system, query, prune, sf, sf_label,  eval_time_ms, incremental
            from fade_scale  where n=1 and num_threads=1 and prune='True' and prob={dbt_prob} and incremental='False' and itype='SCALE_RANDOM'
        UNION ALL select 'DBT-Prune-D' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
            from dbt_data_all where prob={dbt_prob} and prune='True' and itype='DELETE'
        UNION ALL select 'DBT-Prune-S' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
            from dbt_data_all where prob={dbt_prob} and prune='True'and itype='SCALE'
        UNION ALL select 'DBT-S' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
            from dbt_data_all where prob={dbt_prob} and prune='False' and itype='SCALE'
        UNION ALL select 'DBT-D' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
            from dbt_data_all where prob={dbt_prob} and prune='False' and itype='DELETE'
        UNION ALL select 'ProvSQL' as system, query, 'False' as prune, 1 as sf, sf_label, time_ms as eval_time, 'False' as incremental
            from df_provsql where with_prov='True'
        UNION ALL select  'Postgres' as system, query,  'False' as prune, 1 as sf, sf_label, time_ms as eval_time, 'False' as incremental
            from df_provsql where with_prov='False'
            """).df()

    cat = "system"
    single_data_sf1 = con.execute("select * from single_data_scale where sf=1").df()
    p = ggplot(single_data_sf1, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
    p += legend_side
    ggsave("figures/fade_single_sf1_side_scale.png", p, postfix=postfix, width=7, height=6, scale=0.8)

    
    # compare speedup of delete and scaling on lineitem
    speedup_data = con.execute("""
    select sf, qid,
    max(scale.eval_time_ms/del.eval_time_ms) as speedup_max,
    avg(scale.eval_time_ms/del.eval_time_ms) as speedup_avg,
    min(scale.eval_time_ms/del.eval_time_ms) as speedup_min
    from (select * from dbt_data_all where itype='DELETE') as del
    JOIN (select * from dbt_data_all where itype='SCALE') as scale
    USING (sf, qid, prob)
    where qid<>12
    group by sf, qid
    order by qid
    """).df()
    print(speedup_data)




    detailed_data = con.execute("""
    select sf, qid, prob, del.eval_time_ms, scale.eval_time_ms
    from (select * from dbt_data_all where itype='DELETE') as del
    JOIN (select * from dbt_data_all where itype='SCALE') as scale
    USING (sf, qid, prob)
    where qid<>12
    order by qid, prob
    """).df()
    print(detailed_data)

    summary_data = con.execute("""
    select
    max(scale.eval_time_ms/del.eval_time_ms) as speedup_max,
    avg(scale.eval_time_ms/del.eval_time_ms) as speedup_avg,
    min(scale.eval_time_ms/del.eval_time_ms) as speedup_min
    from (select * from dbt_data_all where itype='DELETE') as del
    JOIN (select * from dbt_data_all where itype='SCALE') as scale
    USING (sf, qid, prob)
    where qid<>12
    """).df()
    print(summary_data)

    print("Fade Scale: ")
    speedup_data = con.execute("""
    select sf, qid, prune,
    max(scale.eval_time_ms/del.eval_time_ms) as speedup_max,
    avg(scale.eval_time_ms/del.eval_time_ms) as speedup_avg,
    min(scale.eval_time_ms/del.eval_time_ms) as speedup_min,
    max(del.eval_time_ms/scale.eval_time_ms) as speedup_max_del,
    avg(del.eval_time_ms/scale.eval_time_ms) as speedup_avg_del,
    min(del.eval_time_ms/scale.eval_time_ms) as speedup_min_del
    from (select * from single_data_all where incremental='False') as del
    JOIN (select * from fade_scale where incremental='False') as scale
    USING (sf, qid, prob, prune)
    where qid<>12
    group by sf, qid, prune
    order by qid
    """).df()
    print(speedup_data)
    detailed_data = con.execute("""
    select sf, qid, prob, prune, del.eval_time_ms, scale.eval_time_ms
    from (select * from single_data_all where incremental='False') as del
    JOIN (select * from fade_scale where incremental='False') as scale
    USING (sf, qid, prob, prune)
    where qid<>12
    order by qid, prob
    """).df()
    print(detailed_data)

    single_summary_all = con.execute("""
    select sf, query, prob,
    avg(dbt.eval_time_ms) as dbt_eval, avg(fade.eval_time_ms) as fade_eval,
    avg(dbt.eval_time_ms / fade.eval_time_ms) as fade_speedup,
    from (select * from fade_scale where prune='True') as fade JOIN
    (select * from dbt_data_all) as dbt using (query, sf, prob) 
    group by sf, query, prob
    order by sf, query, prob
    """).df()
    print(single_summary_all)
    print(summary("single_data_scale", "sf, system", "", "where query<>'Q1'"))
    print(summary("single_data_scale", "sf, system","",  "where query='Q1'"))
    print(summary("single_data_scale", "sf, system"))

    # how much Scale is faster or slower than Delete?
