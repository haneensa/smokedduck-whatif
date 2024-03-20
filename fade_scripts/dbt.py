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

dbt_prob = 0.1
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
    local_data["prune_label"] = local_data.apply(lambda row:"FaDE-P" if row["prune"] else "FaDE" , axis=1)
    return local_data
    
# fade using incremental data processing with sparse intervention representation
incremental_data = get_data("fade_data/single_incremental_random_sparse.csv", 1000)
dbt_data = get_data("fade_data/dbtoast.csv", 1)
dbt_data["cat"] = dbt_data.apply(lambda row: "DBT_prune" if row["prune"] else "DBT", axis=1)

cat = "prob"
p = ggplot(dbt_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
p += axis_labels('Query', "Run time (ms)", "discrete", "log10")
p += legend_bottom
p += facet_grid(".~prune", scales=esc("free_y"))
ggsave("figures/dbt.png", p,width=10, height=8, scale=0.8)

# TODO: run fade dense again for all probs
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
p += legend_side
p += geom_hline(aes(yintercept=1))
p += facet_grid(".~prune_label", scales=esc("free_y"))
ggsave(f"figures/fade_dbt_vs_fade.png", p, postfix=postfix, width=7, height=3, scale=0.8)

dbt_fade_data_prune = con.execute("""select * from dbt_fade_data where prune_label='FaDE-P'""").df()
p = ggplot(dbt_fade_data_prune, aes(x='prob',  y="nor", color="query", fill="query", linetype='system'))
p += geom_line(stat=esc('identity')) 
p += axis_labels('Deletion Probability (log)', "Normalized Latency  (log)", "log10", "log10",
    ykwargs=dict(breaks=[0.1,10,100,1000,10000],  labels=list(map(esc,['0.1','10','100','1000','10000']))),
    xkwargs=dict(breaks=[0.001, 0.01, 0.1,],  labels=list(map(esc,['0.001','0.01','0.1']))),
    )
p += legend_side
p += geom_hline(aes(yintercept=1))
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
    avg(dbt_data.eval_time / f.eval_time_ms) as avg_speedup,
    min(dbt_data.eval_time / f.eval_time_ms) as min_speedup
    from {fade_data} as f JOIN dbt_data
    USING (qid, sf)
    where f.n=1 and f.prune='{fade_prune}' and dbt_data.prune='{dbt_prune}'
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
    and f.incremental='{is_incremental}' and f.num_threads=1
    group by {group} sf
    order by {group} sf""").df()


if print_summary:
    print("======== DBT vs FaDe Summary =============")
    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary_hack("dense_data", "False", "False", "False", "qid,")
    print("***", summary_data)
    summary_data = get_summary_hack("dense_data", "False", "False", "False", "")
    print("***", summary_data)
    
    
    
    print("======== DBT vs FaDe-I Summary =============")
    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary("dense_data", "False", "False", "True", "qid,")
    print("***", summary_data)
    summary_data = get_summary("dense_data", "False", "False", "True", "prob,")
    print("***", summary_data)
    
    
    print("======== DBT-P vs FaDe Summary =============")
    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary("dense_data", "True", "False", "False", "qid,")
    summary_data = get_summary_hack("dense_data", "True", "False", "False", "qid,")
    print("***", summary_data)
    
    summary_data = get_summary("dense_data", "True", "False", "False", "prob,")
    summary_data = get_summary_hack("dense_data", "True", "False", "False", "")
    print("***", summary_data)
    print(f"{round(summary_data['avg_speedup'][0])}X (min: {summary_data['min_speedup'][0]:.4f}, max: {round(summary_data['max_speedup'][0])})")
    
    print("======== DBT-P vs FaDe-P Summary =============")
    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary("dense_data", "True", "True", "False", "qid,")
    print("***", summary_data)
    
    summary_data = get_summary("dense_data", "True", "True", "False", "prob,")
    print("***", summary_data)
    print(f"{round(summary_data['avg_speedup'][0])}X (min: {summary_data['min_speedup'][0]:.4f}, max: {round(summary_data['max_speedup'][0])})")
    
    
    print("======== DBT-P vs FaDe-P-I Summary =============")
    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary("dense_data", "True", "True", "True", "qid,")
    print("***", summary_data)
    
    summary_data = get_summary("dense_data", "True", "True", "True", "prob,")
    print("***", summary_data)
    
    # TODO: check if incremental evaluation converges to DBT
    print("======== DBT vs FaDe-I-Sparse Summary =============")
    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary("incremental_data", "False", "False", "True", "qid,")
    
    print("***", summary_data)
    summary_data = get_summary("incremental_data", "False", "False", "True", "prob,")
    print("***", summary_data)
    
    summary_data = get_summary("incremental_data", "False", "False", "True", "")
    print("***", summary_data)
    print(f"{round(summary_data['avg_speedup'][0])}X (min: {summary_data['min_speedup'][0]:.4f}, max: {round(summary_data['max_speedup'][0])})")
    
    print("======== DBT-P vs FaDe-I-Sparse Summary =============")

    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary("incremental_data", "True", "False", "True", "qid,")
    print("***", summary_data)
    
    summary_data = get_summary("incremental_data", "True", "False", "True", "prob,")
    print("***", summary_data)
    
    summary_data = get_summary("incremental_data", "True", "False", "True", "")
    print("***", summary_data)
    print(f"{round(summary_data['avg_speedup'][0])}X (min: {summary_data['min_speedup'][0]:.4f}, max: {round(summary_data['max_speedup'][0])})")
    
    print("======== DBT-P vs FaDe-P-I-Sparse Summary =============")
    # fade_data, dbt_prune, fade_prune, is_incremental, group
    summary_data = get_summary("incremental_data", "True", "True", "True", "qid,")
    print("***", summary_data)
    
    summary_data = get_summary("incremental_data", "True", "True", "True", "prob,")
    print("***", summary_data)
    
    summary_data = get_summary("incremental_data", "True", "True", "True", "")
    print("***", summary_data)
    print(f"{round(summary_data['avg_speedup'][0])}X (min: {summary_data['min_speedup'][0]:.4f}, max: {round(summary_data['max_speedup'][0])})")
    if False:
        print("======== DBT-P vs FaDe-P Summary Per Prob =============")
        summary_data = get_per_prob_summary("dense_data", "True", "True", "False")
        print(summary_data)
        
        print("======== DBT-P vs FaDe Summary Per Prob Prune =============")
        summary_data = get_per_prob_summary("dense_data", "True", "False", "False")
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

dbtruntime_data = con.execute("""
select sf, prune,
max(eval_time_ms) as eval_time_max, avg(eval_time_ms) as eval_time_avg, min(eval_time_ms) as eval_time_min,
max(gen_time) as prune_time_max, avg(gen_time) as prune_time_avg, min(gen_time) as prune_time_min
from dbt_data
group by sf, prune
order by prune
""").df()
print(dbtruntime_data)

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

single_per_q_summary = con.execute("""
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
from (select * from single_data where system='Fade-p') as fp JOIN
(select * from single_data where system='Fade') as f using (query, sf) JOIN
(select * from single_data where system='DBT') as d using (query, sf) JOIN
(select * from single_data where system='DBT-p') as dp using (query, sf)
group by sf
""").df()
print(single_summary_all)

dbtfull_latency_avg = single_data_summary["latency_avg"][3]
fade_latency_avg = (single_data_summary["latency_avg"][0] + single_data_summary["latency_avg"][2])/2.0
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
