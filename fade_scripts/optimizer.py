# forward vs backward 
# generate workload: vary number of output groups, vary skew, vary input cardinality
# add option: round robin vs equal buckets without memory allocation

# group aggs: on q1 vary number of batches (1, 2, 4, 8) --> best 4

import sys
import duckdb
import pandas as pd
from pygg import *
from utils import get_data
from utils import legend, legend_bottom, legend_side

pd.set_option('display.max_rows', None)
con = duckdb.connect(':default:')


micro = pd.read_csv("forward_vs_backward_v5.csv")
micro["eval_time_ms"] = 1000 * micro["eval_time"]
micro["system"] = micro.apply(lambda row: 'backward' if row['use_gb_backward_lineage'] else 'forward' , axis=1)
micro["vec_label"] = micro.apply(lambda row: 'scalar' if row['is_scalar'] else 'SIMD' , axis=1)
micro.rename(columns={'distinct': 'n'}, inplace=True)
micro.rename(columns={'group': 'g'}, inplace=True)
print(micro)
cat = "g"
micro_summary = con.execute("select n, system, is_scalar, num_threads, g, avg(eval_time_ms) as avg_eval_time_ms from micro where alpha=1 group by n, g, system, is_scalar, num_threads").df()
p = ggplot(micro_summary, aes(x='system',  y="avg_eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.4)
p += axis_labels('query', "Latency (ms, log)", 'discrete')
p += legend_side
p += facet_grid(".~is_scalar~num_threads", scales=esc("free_y"))
ggsave(f"figures/micro_forward_backward.png", p, width=10, height=10, scale=0.8)

cat = 'g'
micro_summary = con.execute("select system, is_scalar, g, alpha, avg(eval_time_ms) as avg_eval_time_ms from micro where n=1024 and num_threads=8 group by alpha, g, system, is_scalar").df()
p = ggplot(micro_summary, aes(x='system',  y="avg_eval_time_ms", color=cat, fill=cat, group=cat))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.4)
p += axis_labels('query', "Latency (ms, log)", 'discrete')
p += legend_side
p += facet_grid(".~is_scalar~alpha", scales=esc("free_y"))
ggsave(f"figures/micro_forward_backward_2.png", p, width=10, height=10, scale=0.8)

# multi-threading wins
print(con.execute("""select system, card, g, n, is_scalar, alpha, 
    t1.eval_time_ms, t2.eval_time_ms, t4.eval_time_ms, t8.eval_time_ms
    from (select * from micro where num_threads=1) t1 JOIN
    (select * from micro where num_threads=2) t2
    USING (card, system, is_scalar, g, alpha, n) JOIN
    (select * from micro where num_threads=4) t4 
    USING (card, system, is_scalar, g, alpha, n) JOIN
    (select * from micro where num_threads=8) t8
    USING (card, system, is_scalar, g, alpha, n)
    """).df())

print(con.execute("""select system, card, g, n, is_scalar, alpha, 
    t1.eval_time_ms, t1.eval_time_ms/t2.eval_time_ms, t1.eval_time_ms/t4.eval_time_ms, t1.eval_time_ms/t8.eval_time_ms
    from (select * from micro where num_threads=1) t1 JOIN
    (select * from micro where num_threads=2) t2
    USING (card, system, is_scalar, g, alpha, n) JOIN
    (select * from micro where num_threads=4) t4 
    USING (card, system, is_scalar, g, alpha, n) JOIN
    (select * from micro where num_threads=8) t8
    USING (card, system, is_scalar, g, alpha, n)
    where g=10 and alpha=1
    order by  is_scalar, g, n, alpha, system
    """).df())
scale_data = con.execute("""select system, card, g, n, is_scalar, alpha, t2.num_threads,
        'n='||card as nlabel, 'g='||g as glabel,t2.vec_label, 'a='||alpha as alabel,
    t2.eval_time_ms, t1.eval_time_ms/t2.eval_time_ms as speedup
    from (select * from micro where num_threads=1) t1 JOIN
    (select * from micro) t2
    USING (card, system, is_scalar, g, alpha, n)
    """).df()

postfix = """
data$glabel = factor(data$glabel, levels=c('g=8', 'g=16', 'g=32', 'g=64', 'g=1000', 'g=10000'))
    """
cat = "system"
p = ggplot(scale_data, aes(x='num_threads',  y="speedup", color=cat, fill=cat, linetype='vec_label'))
p += geom_line(stat=esc('identity'))
p += axis_labels('#threads', "Speedup", 'continuous')
p += legend_side
p += facet_grid(".~glabel~alabel", scales=esc("free_y"))
ggsave(f"figures/agg_forward_vs_backward.png", p, postfix=postfix,  width=8, height=8, scale=0.8)

scale_data_pick = con.execute("select * from scale_data where (g=8 or g=64 or g=1000 or g=10000) ").df()
cat = "system"
p = ggplot(scale_data_pick, aes(x='num_threads',  y="speedup", color=cat, fill=cat, linetype='vec_label'))
p += geom_line(stat=esc('identity'))
p += axis_labels('#threads', "Speedup", 'continuous')
p += legend_bottom
p += facet_grid(".~nlabel~alabel~glabel", scales=esc("free_y"))
ggsave(f"figures/agg_forward_vs_backward_sample.png", p, postfix=postfix, width=6, height=4, scale=0.8)

cat = "system"
p = ggplot(scale_data_pick, aes(x='num_threads',  y="eval_time_ms", color=cat, fill=cat, linetype='vec_label'))
p += geom_line(stat=esc('identity'))
p += axis_labels('#threads', "Speedup", 'continuous')
p += legend_bottom
p += facet_grid(".~nlabel~alabel~glabel", scales=esc("free_y"))
ggsave(f"figures/agg_forward_vs_backward_latency_sample.png", p, postfix=postfix, width=6, height=4, scale=0.8)


# x-axis: num_threads, y-axis: speedup
if False:
    forward_q1_multi = get_data(f"forward_q1_multi.csv", 1000)
    backward_q1_multi = get_data(f"backward_q1_multi.csv", 1000)

    forward = get_data(f"forward_multi_1_2048.csv", 1000)
    backward = get_data(f"backward_multi_1_2048.csv", 1000)

    # plot x-axis agg batch size; y-axis eval time; facet over threads;
    cat = "system"
    direction_data = con.execute("select 'forward' as system, * from forward UNION select 'backward' as syste, * from backward").df()
    p = ggplot(direction_data, aes(x='query',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.4)
    p += axis_labels('query', "Latency (ms, log)", 'discrete')
    p += legend_side
    p += facet_grid(".~n~is_scalar~num_threads", scales=esc("free_y"))
    ggsave(f"figures/agg_forward_backward.png", p, width=10, height=10, scale=0.8)

    cat = "system"
    q1_data = con.execute("select 'forward' as system, * from forward_q1_multi UNION select 'backward' as syste, * from backward_q1_multi").df()
    p = ggplot(q1_data, aes(x='system',  y="eval_time_ms", color=cat, fill=cat, group=cat))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.9), width=0.4)
    p += axis_labels('Q1', "Latency (ms, log)", 'discrete', "log10")
    p += legend_side
    p += facet_grid(".~n~is_scalar~num_threads", scales=esc("free_y"))
    ggsave(f"figures/agg_q1_forward_backward.png", p, width=10, height=10, scale=0.8)

    # print forward over backward speedup on tpch queries
    print(con.execute("""select is_scalar, n, num_threads, query, backward.eval_time_ms/forward.eval_time_ms
        from (select * from q1_data where system='forward') as forward JOIN
        (select * from q1_data where system='backward') as backward USING 
        (is_scalar, n, num_threads, query)
        """).df())
    print(con.execute("""select is_scalar, n, num_threads, query, backward.eval_time_ms/forward.eval_time_ms
        from (select * from direction_data where system='forward') as forward JOIN
        (select * from direction_data where system='backward') as backward USING 
        (is_scalar, n, num_threads, query)
        """).df())
    print(con.execute("""select is_scalar, avg(backward.eval_time_ms/forward.eval_time_ms)
        from (select * from direction_data where system='forward') as forward JOIN
        (select * from direction_data where system='backward') as backward USING 
        (is_scalar, n, num_threads, query)
        group by is_scalar
        """).df())

    # forward is 2X faster than backward on q1

    print(con.execute("""select is_scalar, avg(backward.eval_time_ms/forward.eval_time_ms)
        from (select * from q1_data where system='forward') as forward JOIN
        (select * from q1_data where system='backward') as backward USING 
        (is_scalar, n, num_threads, query)
        group by is_scalar
        """).df())
    # forward vs backward lineage
    # for each scalar, vec, threads: what is the best batch value?
    def summary_detailed(table_name, attrs):
        print(f"======== {table_name} ==========")
        return con.execute(f"""select count(), {attrs},
        avg(eval_time_ms), max(eval_time_ms), min(eval_time_ms)
        from {table_name} 
        group by {attrs}
        order by {attrs}""").df()

    attrs = "use_gb_backward_lineage"
    print(summary_detailed("q1_data", attrs))

    attrs = "is_scalar,  use_gb_backward_lineage, num_threads, n"
    print(summary_detailed("direction_data", attrs))

    attrs = "is_scalar,  use_gb_backward_lineage, n"
    print(summary_detailed("direction_data", attrs))

    attrs = "query,  use_gb_backward_lineage"
    print(summary_detailed("direction_data", attrs))


    attrs = "use_gb_backward_lineage, n"
    print(summary_detailed("direction_data", attrs))

    batching = True
    if batching:
        forward_q1_multi_batch = get_data(f"forward_q1_multi_batch.csv", 1000)
        backward_q1_multi_batch = get_data(f"backward_q1_multi_batch.csv", 1000)
        data = con.execute("""select 'forward' as system, * from forward_q1_multi_batch
        UNION ALL select 'backward' as system, * from backward_q1_multi_batch
        """).df()
        print("==== batching ===")
        def compare_batches(attrs):
            print(attrs)
            print(con.execute(f""" select {attrs},
                avg(b1.eval_time_ms), avg(b2.eval_time_ms), avg(b4.eval_time_ms), avg(b8.eval_time_ms),
                avg(b1.eval_time_ms/b4.eval_time_ms) b1b4wins, avg(b2.eval_time_ms/b4.eval_time_ms) b2b4wins,
                avg(b8.eval_time_ms/b4.eval_time_ms) b8b4wins
                from (select * from data where batch=1) as b1 JOIN
                (select * from data where batch=2) as b2 USING (is_scalar, num_threads, use_duckdb, n, system) JOIN
                (select * from data where batch=4) as b4 USING (is_scalar, num_threads, use_duckdb, n, system) JOIN
                (select * from data where batch=8) as b8 USING (is_scalar, num_threads, use_duckdb, n, system)
                group by {attrs}
                order by {attrs}
                """).df())
        compare_batches("system")
        compare_batches("system, is_scalar")
        compare_batches("is_scalar")
        compare_batches("is_scalar, num_threads")
        compare_batches("n")
        compare_batches("num_threads")
        print("evaluating 4 aggregates at a time is best for non vectorized approach with 3X wins")

