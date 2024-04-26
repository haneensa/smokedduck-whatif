import sys
import duckdb
import pandas as pd
from pygg import *

def get_data(fname, scale):
    local_data = pd.read_csv(fname)
    local_data["eval_time_ms"] = scale * local_data["eval_time"]
    local_data["query"] = "Q"+ local_data["qid"].astype(str)
    local_data["cat"] = local_data.apply(lambda row: str(row["num_threads"]) + "W", axis=1)
    local_data["cat"] = local_data.apply(lambda row: row["cat"] + "+SIMD" if row["is_scalar"] == False else row["cat"] , axis=1)
    local_data["sf_label"] = "SF="+ local_data["sf"].astype(str)
    local_data["prune_label"] = local_data.apply(lambda row:"Fade-Prune" if row["prune"] else "Fade" , axis=1)
    local_data.rename(columns={'distinct': 'n'}, inplace=True)
    return local_data
    
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

