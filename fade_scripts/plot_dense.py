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

con = duckdb.connect('db.out')

# for each query,
data_scalar = pd.read_csv('fade_data/dense_feb7_sf1_vary_interventions_scalar.csv')
data_simd = pd.read_csv('fade_data/dense_feb7_sf1_vary_interventions_simd.csv')
data = pd.concat([data_scalar, data_simd], ignore_index=True)


data["eval_time_ms"]=1000*data["eval_time"]
data["query"] = "Q"+data["qid"].astype(str)
data["cat"] = data.apply(lambda row: str(row["num_threads"]) + " Threads", axis=1)
data["cat"] = data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
data["n"] = data["distinct"]
print(data)

data_distinct = con.execute("select * from data where num_threads=8 and is_scalar='false'").df()
cat = 'distinct'
p = ggplot(data_distinct, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Latency (ms)", "discrete")
p += legend_bottom
p += legend_side
postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
"""
ggsave("figures/fade_best_distinct_sf1.png", p, postfix=postfix, width=4, height=3, scale=0.8)

#p += facet_grid(".~is_scalart", scales=esc("free_y"))

print(con.execute("select * from data").df())
data_distinct = con.execute("select * from data where n=1024").df()
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
ggsave("figures/fade_1024_sf1.png", p, postfix=postfix, width=5, height=3, scale=0.8)

data_distinct = con.execute("""select sf, t1.query, t1.cat,qid, n, t1.num_threads, t1.is_scalar, base.eval_time_ms / t1.eval_time_ms as speedup from ( select * from data where n=1024 ) as t1 JOIN
(select * from data where n=1024 and num_threads=1 and is_scalar='true') as base USING (sf, qid, n) """).df()
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
ggsave("figures/fade_1024_sf1_speedup.png", p, postfix=postfix, width=5, height=3, scale=0.8)
