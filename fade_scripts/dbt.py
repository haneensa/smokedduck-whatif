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
    
incremental_data = get_data("single_incremental_random_sparse.csv", 1000)

dbt_data = get_data("fade_data/dbtoast.csv", 1)
dbt_data["cat"] = dbt_data.apply(lambda row: "DBT_prune" if row["prune"] else "DBT", axis=1)

cat = "prob"
p = ggplot(dbt_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
p += axis_labels('Query', "Run time (ms)", "discrete", "log10")
p += legend_bottom
p += facet_grid(".~prune", scales=esc("free_y"))
ggsave("figures/dbt.png", p,width=10, height=8, scale=0.8)

#data = pd.read_csv('dense_delete_all_sf1.csv')
#dense_data = get_data('fade_data/dense_all_sf1_v2.csv', 1000)
dense_data = get_data('fade_data/dense_sf1_v4.csv', 1000)
dense_data["cat"] = dense_data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
dense_data["cat"] = dense_data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
dense_data["cat"] = dense_data.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)
# figure 1: single intervention latency
dbt_prob=0.1
single_data = con.execute(f"""
    select 'Fade-p' as system, query, prune, sf, sf_label,  eval_time_ms, incremental
        from dense_data where n=1 and num_threads=1 and prune='True' and prob={dbt_prob} and incremental='False'
    UNION ALL select 'Fade' as system, query, prune, sf, sf_label, eval_time_ms, incremental
        from dense_data where n=1 and num_threads=1  and prune='False' and prob={dbt_prob} and incremental='False'
    UNION ALL select 'DBT-p' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
        from dbt_data where prob={dbt_prob} and prune='True'
    UNION ALL select 'DBT' as system, query, prune, sf, sf_label, eval_time_ms as eval_time, 'False' as incremental
        from dbt_data where prob={dbt_prob} and prune='False'
        """).df()

postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
data$sf_label = factor(data$sf_label, levels=c('SF=1.0', 'SF=5.0', 'SF=10.0'))
    """
cat = "system"

p = ggplot(single_data, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
p += legend_side
p += facet_grid(".~sf_label", scales=esc("free_y"))
ggsave("figures/fade_single.png", p, postfix=postfix, width=8, height=3, scale=0.8)

single_data_sf1 = con.execute("select * from single_data where sf=1").df()
p = ggplot(single_data_sf1, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
p += legend_side
p += facet_grid(".~sf_label", scales=esc("free_y"))
ggsave("figures/fade_single_sf1.png", p, postfix=postfix, width=5, height=3, scale=0.8)

# fig 1:
# x-axis prob, y-axis dbt normalized latency against fade
dbt_fade_data = con.execute("""
select 'dense' as system, sf, query, dbt.prob, fade.incremental, fade.prune_label, dbt.prune dbt_prune, fade.prune, fade.num_threads,
(dbt.eval_time_ms / fade.eval_time_ms) as nor,
dbt.eval_time_ms, fade.eval_time_ms
from (select * from dbt_data where prune='True') as dbt JOIN
(select * from  dense_data where n=1 and num_threads=1) as fade
USING (query, sf)
UNION ALL
select 'sparse+inc' as system, sf, query, prob, fade.incremental, fade.prune_label, dbt.prune dbt_prune, fade.prune, fade.num_threads,
(dbt.eval_time_ms / fade.eval_time_ms) as nor,
dbt.eval_time_ms, fade.eval_time_ms
from (select * from dbt_data where prune='True') as dbt JOIN
(select * from  incremental_data where n=1 and num_threads=1 and sf=1) as fade
USING (query, sf, prob)
""").df()

postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
    """
p = ggplot(dbt_fade_data, aes(x='prob',  y="nor", color="query", fill="query", linetype='system'))
p += geom_line(stat=esc('identity')) 
p += axis_labels('Deletion Probability (log)', "Normalized Latency (log)", "log10", "log10",
    ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
    xkwargs=dict(breaks=[0.001, 0.01, 0.1,],  labels=list(map(esc,['0.001','0.01','0.1']))),
    )
p += legend_bottom
p += geom_hline(aes(yintercept=1))
p += facet_grid(".~prune_label", scales=esc("free_y"))
ggsave(f"figures/fade_dbt_vs_fade.png", p, postfix=postfix, width=5, height=3, scale=0.8)

dbt_fade_data_prune = con.execute("""select * from dbt_fade_data where prune_label='P'""").df()
p = ggplot(dbt_fade_data_prune, aes(x='prob',  y="nor", color="query", fill="query", linetype='system'))
p += geom_line(stat=esc('identity')) 
p += axis_labels('Deletion Probability (log)', "Normalized Latency  (log)", "log10", "log10",
    ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
    xkwargs=dict(breaks=[0.001, 0.01, 0.1,],  labels=list(map(esc,['0.001','0.01','0.1']))),
    )
p += legend_side
p += geom_hline(aes(yintercept=1))
ggsave(f"figures/fade_dbt_vs_fade_prune.png", p, postfix=postfix,  width=5, height=3, scale=0.8)

if print_summary:
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

print("======== Single Summary =============")
summary_data = con.execute("""
select sf, system, avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
from single_data
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
from (select * from single_data where system='Fade-p') as fp JOIN
(select * from single_data where system='Fade') as f using (query, sf) JOIN
(select * from single_data where system='DBT') as d using (query, sf) JOIN
(select * from single_data where system='DBT-p') as dp using (query, sf)
""").df()
print(summary_data)
