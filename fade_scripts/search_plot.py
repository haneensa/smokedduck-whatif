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

# contains overhead of original query without lineage capture, then with, then with intermediates
lineage_data = pd.read_csv('fade_data/lineage_overhead.csv')
lineage_data["query"] = "Q"+lineage_data["qid"].astype(str)
lineage_data["sf_label"] = "SF="+lineage_data["sf"].astype(str)

# TODO: add incremental single evaluation. hold the indices of elements to be deleted. this would be equivalent eval to dbtoast
include_search = True
if include_search:
    # benchmark.sh set: lineiten.i
    data_spec = pd.read_csv('search_dense.csv')
    # predicate search
    data_search = pd.read_csv('search_m2.csv')
    # from: benchmark_cube.sh
    data_cube = pd.read_csv('fade_data/cube_search.csv')

    data_spec["sys"] = "FaDe"
    data_search["sys"] = "FaDe-Sparse"
    data_cube["sys"] = "GB"

    data_spec["itype"] = "DD"
    data_search["itype"] = "S"
    data_cube["itype"] = "GB"
    data = pd.concat([data_spec, data_search], axis=0)
    
    data["eval_time_ms"]=1000*data["eval_time"]
    data["query"] = "Q"+data["qid"].astype(str)

    data["cat"] = data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
    data["cat"] = data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    data["cat"] = data.apply(lambda row: row["itype"]+" ^"+row["cat"] if row["incremental"] and row["itype"] == "S" else row["itype"] + row["cat"], axis=1)
    
    data["sys"] = data.apply(lambda row: row["sys"]+"-inc" if row["incremental"] and row["itype"] == "S" else row["sys"] , axis=1)

    data["n"] = data["distinct"]

    data_cube["eval_time_ms"]=1000*data_cube["timing"]
    data_cube["query"] = "Q"+data_cube["qid"].astype(str)
    data_cube["cat"] = data_cube.apply(lambda row: row["itype"] + " " + str(row["num_threads"]) + "W", axis=1)
    data_cube["n"] = data_cube["distinct"]

    # SEARCH (non-incremental, incremental) vs DENSE_DELETE_SPEC
    # x-axis: n_interventions, y-axis: latency, facet: query, cat: itype+incremental
    # speedup: base x-axis DENSE_DELETE_SPEC

    if plot:
        postfix = """
        data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
            """
        plot_data = con.execute("""
        select sys, query,eval_time_ms, itype, prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data where n>1 and itype='S' and incremental='True'
        UNION ALL
        select sys, query, eval_time_ms, itype, prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data where n>1 and itype='S' and incremental='False'
        UNION ALL
        select sys, query, eval_time_ms, itype, prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data where n>1 and itype='DD' and incremental='False'
        UNION ALL
        select sys, query, eval_time_ms, itype, prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data where n>1 and itype='DD' and incremental='True'
        UNION ALL
        select sys, query, eval_time_ms, itype, False as prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data_cube
        UNION ALL
        select sys, query,  eval_time_ms, itype, True as prune, n, sf, cat, num_threads, n/(eval_time_ms/1000.0) as throughput from data_cube
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
        
        sf1_plot_data = con.execute("select * from plot_data where sf=1 and num_threads=8 and prune='True' and n=512").df()
        print("check")
        print(sf1_plot_data)
        p = ggplot(sf1_plot_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Run time (ms, log)", "discrete", "log10")
        p += legend_bottom
        p += facet_grid(".~n", scales=esc("free_y"))
        ggsave("figures/fade_search_fade25_sf1_512.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)
        
        postfix = """
        data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
        data$cat = factor(data$cat, levels=c('DD8W+SIMD', 'S8W+SIMD', 'S ^8W'))
            """
        sf1_plot_data = con.execute("""select *, base.eval_time_ms / t1.eval_time_ms as speedup
                from (select * from plot_data where prune='True' and (cat='DD8W' or cat='S8W' or cat='DD8W+SIMD' or cat='S8W+SIMD' or cat='S ^8W')) as t1 JOIN
                (select * from plot_data where itype='GB') as base using (sf, n, num_threads, prune, query)
                """).df()
        print("check")
        print(sf1_plot_data)
        p = ggplot(sf1_plot_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
        p += legend_bottom
        p += facet_grid(".~n", scales=esc("free_y"))
        ggsave("figures/fade_search_fade25_sf1_512_speedup.png", p,  width=8, height=2.5, scale=0.8)
        
        sf1_plot_data = con.execute("""select *, t1.eval_time_ms as sys_eval, base.eval_time_ms as base_eval, base.eval_time_ms / t1.eval_time_ms as speedup
                from (select * from plot_data where n=2560 and prune='True' and (cat='DD8W+SIMD' or cat='S8W+SIMD' or cat='S ^8W')) as t1 JOIN
                (select * from plot_data where itype='GB') as base using (sf, n, num_threads, prune, query)
                """).df()
        print("check")
        print(sf1_plot_data)

        cat = "sys"
        p = ggplot(sf1_plot_data, aes(x='query',  y="speedup", color=cat, fill=cat, group=cat))
        p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.88)
        p += axis_labels('Query', "Speedup (log)", "discrete", "log10")
        p += legend_bottom
        p += facet_grid(".~n", scales=esc("free_y"))
        ggsave("figures/fade_search_fade25_sf1_2560_speedup_vec.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

        summary = con.execute("""select sf, query, cat, avg(speedup), max(speedup), min(speedup),
        avg(sys_eval), avg(base_eval)
        from sf1_plot_data
        group by sf, query, cat
        """).df()
        print(summary)
        
        summary = con.execute("""select sf, cat, avg(speedup), max(speedup), min(speedup),
        avg(sys_eval), avg(base_eval)
        from sf1_plot_data
        group by sf, cat
        """).df()
        print(summary)