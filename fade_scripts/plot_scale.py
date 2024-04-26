import sys
import duckdb
import pandas as pd
from pygg import *

from utils import get_data
from utils import legend, legend_bottom, legend_side

pd.set_option('display.max_rows', None)

con = duckdb.connect(':default:')
    
# scale_data = get_data('fade_data/dense_scale_all.csv', 1000)
#scale_data = get_data('dense_delete_th_8_16_april10.csv', 1000)
scale_data = get_data('dense_best_delete_sf_1_5_10.csv', 1000)

# vary batch size
scale_fig_data = con.execute("""select *, (n /( eval_time )) as throughput
    from scale_data where  is_scalar='False' and num_threads=8
    """).df()
print(scale_fig_data)
postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
    """
p = ggplot(scale_fig_data, aes(x='n',  y="throughput", color='query', shape='sf_label', linetype="sf_label"))
p += geom_point(stat=esc('identity'))
p +=  geom_line()
p += axis_labels('Batch Size (log)', "Interventions / Seconds (log)", "log10", "log10", 
        ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
        )
p += legend_side
#p += facet_grid(".~num_threads", scales=esc("free_y"))
ggsave("figures/fade_throughput.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

p = ggplot(scale_fig_data, aes(x='n',  y="eval_time_ms", color='query', shape='sf_label', linetype="sf_label"))
p += geom_point(stat=esc('identity'))
p +=  geom_line()
p += axis_labels('Batch Size (log)', "Latency (ms, log)", "log10", "log10", 
        ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
        )
p += legend_side
#p += facet_grid(".~num_threads", scales=esc("free_y"))
ggsave("figures/fade_latency.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

p = ggplot(scale_fig_data, aes(x='sf',  y="throughput", color='query', shape='query'))
p += geom_point(stat=esc('identity'))
p +=  geom_line(state=esc('identity'))
p += axis_labels('SF (log)', "Interventions / Seconds (log)", "log10", "log10", 
        ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
        )
p += legend_side
p += facet_grid(".~n", scales=esc("free_y"))
ggsave("figures/fade_throughput_per_n.png", p, postfix=postfix, width=6, height=2.5, scale=0.8)

p = ggplot(scale_fig_data, aes(x='sf',  y="eval_time_ms", color='query', shape='query'))
p += geom_point(stat=esc('identity'))
p +=  geom_line(state=esc('identity'))
p += axis_labels('SF (log)', "Latency (ms, log)", "log10", "log10", 
        ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
        )
p += legend_side
p += facet_grid(".~n", scales=esc("free_y"))
ggsave("figures/fade_latency_per_n.png", p, postfix=postfix, width=6, height=2.5, scale=0.8)

scale_fig_data_best = con.execute("select sf, query, max(throughput) as throughput, min((2048/n) *eval_time_ms) as time_ms  from scale_fig_data group by query, sf").df()
p = ggplot(scale_fig_data_best, aes(x='sf',  y="throughput", color='query', shape='query'))
p += geom_point(stat=esc('identity'))
p +=  geom_line(state=esc('identity'))
p += axis_labels('SF (log)', "Interventions / Seconds (log)", "log10", "log10", 
        ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
        xkwargs=dict(breaks=[1,5,10],  labels=list(map(esc,['1','5','10']))),
        )
p += legend_side
ggsave("figures/fade_throughput_best.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

p = ggplot(scale_fig_data_best, aes(x='sf',  y="time_ms", color='query', shape='query'))
p += geom_point(stat=esc('identity'))
p +=  geom_line(state=esc('identity'))
p += axis_labels('SF (log)', "Latency (ms, log)", "log10", "log10", 
        ykwargs=dict(breaks=[1000,10000,100000,1000000],  labels=list(map(esc,['10e3','10e4','10e5','10e6']))),
        xkwargs=dict(breaks=[1,5,10],  labels=list(map(esc,['1','5','10']))),
        )
p += legend_side
ggsave("figures/fade_latency_2048.png", p, postfix=postfix, width=4, height=2.5, scale=0.8)

