import duckdb
from pygg import *
import pandas as pd
from utils import get_data
from utils import legend, legend_bottom, legend_side

con = duckdb.connect()

single_data_all = get_data("fade_data/dense_single_vary_probs_april7.csv", 1000)

provsql_data = [{'qid': 1, 'with_prov': False, 'expid': 0, 'time_ms': 3927.064}, {'qid': 3, 'with_prov': False, 'expid': 1, 'time_ms': 476.422}, {'qid': 5, 'with_prov': False, 'expid': 2, 'time_ms': 1234.733}, {'qid': 7, 'with_prov': False, 'expid': 3, 'time_ms': 892.848}, {'qid': 9, 'with_prov': False, 'expid': 4, 'time_ms': 1315.129}, {'qid': 10, 'with_prov': False, 'expid': 5, 'time_ms': 1389.753}, {'qid': 12, 'with_prov': False, 'expid': 6, 'time_ms': 501.138}, {'qid': 3, 'with_prov': True, 'expid': 8, 'time_ms': 5544.203}, {'qid': 5, 'with_prov': True, 'expid': 9, 'time_ms': 1624.657}, {'qid': 7, 'with_prov': True, 'expid': 10, 'time_ms': 1334.478}, {'qid': 9, 'with_prov': True, 'expid': 11, 'time_ms': 68457.627}, {'qid': 10, 'with_prov': True, 'expid': 12, 'time_ms': 23379.272}, {'qid': 12, 'with_prov': True, 'expid': 13, 'time_ms': 3617.771}]
df_provsql = pd.DataFrame(provsql_data)
df_provsql["query"] = "Q"+df_provsql["qid"].astype(str)

dbt_prob=0.001
# get fade data
single_data = con.execute(f"""
    select 'Fade-Prune' as system, query, sf,  eval_time_ms
        from single_data_all where n=1 and num_threads=1 and prune='True' and prob={dbt_prob} and incremental='False'
    UNION ALL select 'Fade' as system, query, sf, eval_time_ms
        from single_data_all where n=1 and num_threads=1  and prune='False' and prob={dbt_prob} and incremental='False'
    UNION ALL select 'ProvSQL' as system, query,  1 as sf, time_ms as eval_time_ms
        from df_provsql where with_prov='True'
    UNION ALL select 'Postgres' as system, query,  1 as sf, time_ms as eval_time_ms
        from df_provsql where with_prov='False'
        """).df()
print(single_data)
postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
    """
cat = "system"
single_data_sf1 = con.execute("select * from single_data where sf=1").df()
p = ggplot(single_data_sf1, aes(x='query', y='eval_time_ms', color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', 'Latency (ms, log)', 'discrete', 'log10')
p += legend_bottom
ggsave("figures/fade_provsql_single_sf1.png", p, postfix=postfix, width=5, height=3, scale=0.8)

summary = con.execute("""select query,
    fade.eval_time_ms as fade_ms, fade_prune.eval_time_ms as fadep_ms,
    provsql.eval_time_ms as provsql_ms, postgres.eval_time_ms as postgres_ms,
    provsql.eval_time_ms/postgres.eval_time_ms as provsql_slowdown,
    provsql.eval_time_ms/fade.eval_time_ms as fade_speedup,
    provsql.eval_time_ms/fade_prune.eval_time_ms as fadep_speedup
    from (select * from single_data where system='Fade') as fade JOIN
    (select * from single_data where system='Fade-Prune') as fade_prune using (query) JOIN
    (select * from single_data where system='ProvSQL') as provsql using (query) JOIN
    (select * from single_data where system='Postgres') as postgres using (query)
    """).df()
print(summary)

print(con.execute("select avg(fade_speedup), avg(provsql_slowdown) from summary").df())
