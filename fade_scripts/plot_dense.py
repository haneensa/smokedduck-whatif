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

# for each query,
data_scalar = pd.read_csv('fade_data/dense_feb7_sf1_vary_interventions_scalar.csv')
data_simd = pd.read_csv('fade_data/dense_feb7_sf1_vary_interventions_simd.csv')
data_single = pd.read_csv('fade_data/dense_d1_sf1_5_10.csv')
data = pd.concat([data_scalar, data_simd, data_single], ignore_index=True)


data["eval_time_ms"]=1000*data["eval_time"]
data["query"] = "Q"+data["qid"].astype(str)
data["cat"] = data.apply(lambda row: str(row["num_threads"]) + " Threads", axis=1)
data["cat"] = data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
data["n"] = data["distinct"]
print(data)

postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
"""

# TODO: add original query runtime and query with lineage enabled
# figure 1: single intervention latency
fig1_data = con.execute("select 'Fade' as system, * from data where n=1").df()
cat = "system"
p = ggplot(fig1_data, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', 'Latency (ms)', 'discrete', 'log10')
p += legend_side
p += facet_grid(".~sf", scales=esc("free_y"))
ggsave("figures/fade_single.png", p, postfix=postfix, width=6, height=3, scale=0.8)

# Batching latency performance varying number of interventions
fig2_data = con.execute("""select sf, t1.query, t1.cat,qid, t1.n, t1.num_threads, t1.distinct,
    t1.num_threads, t1.is_scalar, (base.eval_time_ms * t1.n)/ t1.eval_time_ms as speedup,
    t1.eval_time_ms
    from ( select * from data where num_threads=1 and is_scalar='true' and sf=1 ) as t1 JOIN
         ( select * from data where n=1 and num_threads=1 and is_scalar='true' and sf = 1) as base
         USING (sf, qid) """).df()
cat = 'distinct'
p = ggplot(fig2_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Latency (ms)", "discrete")
p += legend_side
p += facet_grid(".~sf", scales=esc("free_y"))
ggsave("figures/fade_batching_latency_sf1.png", p, postfix=postfix, width=4, height=3, scale=0.8)

# Batching speedup over single intervention performance varying number of interventions
p = ggplot(fig2_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Speedup", "discrete")
p += legend_side
p += facet_grid(".~sf", scales=esc("free_y"))
ggsave("figures/fade_batching_speedup_sf1.png", p, postfix=postfix, width=4, height=3, scale=0.8)


# Additional vectorization speedup over batched execution of  varying numbers of interventions
fig3_data = con.execute("""select sf, qid, n, t1.query, t1.cat, t1.num_threads,
    t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
    base.eval_time_ms from fig2_data as base JOIN 
        (select * from data where num_threads=1 and is_scalar='false' and sf=1) as t1
        USING (sf, qid, n)""").df()

print(fig3_data)
p = ggplot(fig3_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Speedup", "discrete")
p += legend_side
p += facet_grid(".~sf", scales=esc("free_y"))
ggsave("figures/fade_vec_speedup_sf1.png", p, postfix=postfix, width=4, height=3, scale=0.8)

# Additional threading speedup for 2, 4 and 8 threads over batched execution
fig4_data = con.execute("""select sf, qid, n, t1.query, t1.cat, t1.num_threads,
    t1.distinct, t1.is_scalar, (base.eval_time_ms / t1.eval_time_ms) as speedup, t1.eval_time_ms,
    base.eval_time_ms from fig2_data as base JOIN 
        (select * from data where is_scalar='true' and sf=1 and n=2560) as t1
        USING (sf, qid, n)""").df()

print(fig4_data)
cat = 'num_threads'
p = ggplot(fig4_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Speedup", "discrete")
p += legend_side
p += facet_grid(".~sf", scales=esc("free_y"))
ggsave("figures/fade_threading_speedup_sf1.png", p, postfix=postfix, width=4, height=3, scale=0.8)

data_distinct = con.execute("select * from data where num_threads=8 and is_scalar='false'").df()
cat = 'distinct'
p = ggplot(data_distinct, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Latency (ms)", "discrete")
p += legend_side
ggsave("figures/fade_best_distinct_sf1.png", p, postfix=postfix, width=4, height=3, scale=0.8)


print(con.execute("select * from data").df())
data_distinct = con.execute("select * from data where n=2560").df()
cat = 'cat'
p = ggplot(data_distinct, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
p += axis_labels('Query', "Latency (ms)", "discrete")
p += legend_bottom
p += legend_side
postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
data$cat = factor(data$cat, levels=c('1 Threads', '2 Threads', '4 Threads', '8 Threads', '1 Threads+SIMD', '2 Threads+SIMD', '4 Threads+SIMD', '8 Threads+SIMD'))
"""
ggsave("figures/fade_2560_sf1.png", p, postfix=postfix, width=5, height=3, scale=0.8)

data_distinct = con.execute("""select sf, t1.query, t1.cat,qid, n, t1.num_threads, t1.is_scalar,
base.eval_time_ms / t1.eval_time_ms as speedup from ( select * from data where n=2560 ) as t1 JOIN
(select * from data where n=2560 and num_threads=1 and is_scalar='true') as base USING (sf, qid, n) """).df()
print(data_distinct)

cat = 'cat'
p = ggplot(data_distinct, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
p += axis_labels('Query', "Speedup", "discrete")
p += legend_bottom
p += legend_side
postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
data$cat = factor(data$cat, levels=c('1 Threads', '2 Threads', '4 Threads', '8 Threads', '1 Threads+SIMD', '2 Threads+SIMD', '4 Threads+SIMD', '8 Threads+SIMD'))
"""
ggsave("figures/fade_2560_sf1_speedup.png", p, postfix=postfix, width=5, height=3, scale=0.8)
