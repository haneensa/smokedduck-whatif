import sys
import duckdb
import pandas as pd
from pygg import *
from utils import get_data
from utils import legend, legend_bottom, legend_side

pd.set_option('display.max_rows', None)

prob = 0.1
con = duckdb.connect(':default:')

postfix = """
data$query = factor(data$query, levels=c('Q1', 'Q3', 'Q5', 'Q7', 'Q9', 'Q10', 'Q12'))
    """

#simd_sf5 = get_data("fade_simd_test_sf5.csv", 1000)
simd= get_data("fade_simd_test_sf5.csv", 1000)
#simd_scalarFilter_sf5 = get_data("fade_simd_test_scalarFilter_sf5.csv", 1000)
#simd_scalarJoinFilter_sf5 = get_data("fade_simd_test_scalarJoinFilter_sf5.csv", 1000)
#simd = get_data("fade_simd_test_scalarJoinFilter_sf5.csv", 1000)
#simd_scalarJoin_sf5 = get_data("fade_simd_test_scalarJoin_sf5.csv", 1000)
#simd_scalarAgg_sf5 = get_data("fade_simd_test_scalarAgg_sf5.csv", 1000)
#simd = get_data("fade_simd.csv", 1000)

summary = con.execute("""
        select sf, n, num_threads, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd where is_scalar='False') as simd JOIN
        (select * from simd where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, n, num_threads, prune_label
        order by sf, n, num_threads, prune_label
        """).df()
print(summary)

summary = con.execute("""
        select sf, n, qid, num_threads, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd where is_scalar='False') as simd JOIN
        (select * from simd where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, n, qid, num_threads, prune_label
        order by sf, n, qid, num_threads, prune_label
        """).df()
print(summary)


# SIMD filter slowsdown exec
# TODO: test scalar Join only
summary = con.execute("""
        select 'Scalar Filter:' as label, sf,  n, prune_label, 
        avg( scalarFilter.eval_time_ms / simd.eval_time_ms ) as aSF,
        max( scalarFilter.eval_time_ms / simd.eval_time_ms ) as mxSF,
        min( scalarFilter.eval_time_ms / simd.eval_time_ms ) as minSF,
        
        avg( 1/(scalarFilter.eval_time_ms / simd.eval_time_ms) ) as IaSF,
        max( 1/(scalarFilter.eval_time_ms / simd.eval_time_ms) ) as ImxSF,
        min( 1/(scalarFilter.eval_time_ms / simd.eval_time_ms) ) as IminSF,
        from simd_sf5 as simd JOIN
        simd_scalarFilter_sf5 as scalarFilter USING (sf, qid, n, num_threads, prune_label, is_scalar) JOIN
        simd_scalarJoinFilter_sf5 as scalarJoinFilter USING (sf, qid, n, num_threads, prune_label, is_scalar) JOIN
        simd_scalarAgg_sf5 as scalarAgg USING (sf, qid, n, num_threads, prune_label, is_scalar) 
        where is_scalar='False'
        group by sf, n, prune_label
        order by sf, n, prune_label
        """).df()
print(summary)

summary = con.execute("""
        select 'Scalar Join:' as label, sf,  n, prune_label, 
        avg( scalarJoin.eval_time_ms / simd.eval_time_ms ) as aSJF,
        max( scalarJoin.eval_time_ms / simd.eval_time_ms ) as mxSJF,
        min( scalarJoin.eval_time_ms / simd.eval_time_ms ) as minSJF,
        
        avg( 1/(scalarJoin.eval_time_ms / simd.eval_time_ms) ) as IaSJF,
        max( 1/(scalarJoin.eval_time_ms / simd.eval_time_ms) ) as ImxSJF,
        min( 1/(scalarJoin.eval_time_ms / simd.eval_time_ms) ) as IminSJF,
        
        from simd_sf5 as simd JOIN
        simd_scalarJoin_sf5 as scalarJoin USING (sf, qid, n, num_threads, prune_label, is_scalar)
        where is_scalar='False'
        group by sf, n, prune_label
        order by sf, n, prune_label
        """).df()
print(summary)

summary = con.execute("""
        select 'Scalar Join,Filter:' as label, sf,  n, prune_label, 
        avg( scalarJoinFilter.eval_time_ms / simd.eval_time_ms ) as aSJF,
        max( scalarJoinFilter.eval_time_ms / simd.eval_time_ms ) as mxSJF,
        min( scalarJoinFilter.eval_time_ms / simd.eval_time_ms ) as minSJF,
        
        avg( 1/(scalarJoinFilter.eval_time_ms / simd.eval_time_ms) ) as IaSJF,
        max( 1/(scalarJoinFilter.eval_time_ms / simd.eval_time_ms) ) as ImxSJF,
        min( 1/(scalarJoinFilter.eval_time_ms / simd.eval_time_ms) ) as IminSJF,
        
        avg( scalarJoinFilter.eval_time_ms / scalarFilter.eval_time_ms ) as aFVJ,
        max( scalarJoinFilter.eval_time_ms / scalarFilter.eval_time_ms ) as mxFVJ,
        min( scalarJoinFilter.eval_time_ms / scalarFilter.eval_time_ms ) as minFVJ,
        
        avg( 1/(scalarJoinFilter.eval_time_ms / scalarFilter.eval_time_ms) ) as IaFVJ,
        max( 1/(scalarJoinFilter.eval_time_ms / scalarFilter.eval_time_ms) ) as ImxFVJ,
        min( 1/(scalarJoinFilter.eval_time_ms / scalarFilter.eval_time_ms) ) as IminFVJ,

        from simd_sf5 as simd JOIN
        simd_scalarFilter_sf5 as scalarFilter USING (sf, qid, n, num_threads, prune_label, is_scalar) JOIN
        simd_scalarJoinFilter_sf5 as scalarJoinFilter USING (sf, qid, n, num_threads, prune_label, is_scalar) JOIN
        simd_scalarAgg_sf5 as scalarAgg USING (sf, qid, n, num_threads, prune_label, is_scalar) 
        where is_scalar='False'
        group by sf, n, prune_label
        order by sf, n, prune_label
        """).df()
print(summary)

summary = con.execute("""
        select 'Agg SIMD:' as label, sf,  n, prune_label, 
        avg( scalarAgg.eval_time_ms / simd.eval_time_ms ) as aSAgg,
        max( scalarAgg.eval_time_ms / simd.eval_time_ms ) as mxSAgg,
        min( scalarAgg.eval_time_ms / simd.eval_time_ms ) as minSAgg,
        avg( 1/(scalarAgg.eval_time_ms / simd.eval_time_ms) ) as IaSAgg,
        max( 1/(scalarAgg.eval_time_ms / simd.eval_time_ms) ) as ImxSAgg,
        min( 1/(scalarAgg.eval_time_ms / simd.eval_time_ms) ) as IminSAgg
        from simd_sf5 as simd JOIN
        simd_scalarFilter_sf5 as scalarFilter USING (sf, qid, n, num_threads, prune_label, is_scalar) JOIN
        simd_scalarJoinFilter_sf5 as scalarJoinFilter USING (sf, qid, n, num_threads, prune_label, is_scalar) JOIN
        simd_scalarAgg_sf5 as scalarAgg USING (sf, qid, n, num_threads, prune_label, is_scalar) 
        where is_scalar='False'
        group by sf, n, prune_label
        order by sf, n, prune_label
        """).df()
print(summary)

summary = con.execute("""
        select sf, n, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd_sf5 where is_scalar='False') as simd JOIN
        (select * from simd_sf5 where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, n, prune_label
        order by sf, n, prune_label
        """).df()
#print(summary)

summary = con.execute("""
        select sf, n, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd_scalarFilter_sf5 where is_scalar='False') as simd JOIN
        (select * from simd_scalarFilter_sf5 where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, n, prune_label
        order by sf, n, prune_label
        """).df()
#print(summary)

summary = con.execute("""
        select sf, n, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd_scalarJoinFilter_sf5 where is_scalar='False') as simd JOIN
        (select * from simd_scalarJoinFilter_sf5 where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, n, prune_label
        order by sf, n, prune_label
        """).df()
#print(summary)

summary = con.execute("""
        select sf, qid, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd_sf5 where is_scalar='False') as simd JOIN
        (select * from simd_sf5 where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, qid, prune_label
        order by sf, qid, prune_label
        """).df()
#print(summary)

summary = con.execute("""
        select sf, qid, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd_scalarFilter_sf5 where is_scalar='False') as simd JOIN
        (select * from simd_scalarFilter_sf5 where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, qid, prune_label
        order by sf, qid, prune_label
        """).df()
#print(summary)

summary = con.execute("""
        select sf, qid, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd_scalarJoinFilter_sf5 where is_scalar='False') as simd JOIN
        (select * from simd_scalarJoinFilter_sf5 where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, qid, prune_label
        order by sf, qid, prune_label
        """).df()
#print(summary)

summary = con.execute("""
        select sf, qid, prune_label, 
        avg(scalar.eval_time_ms / simd.eval_time_ms) ,
        max(scalar.eval_time_ms / simd.eval_time_ms) ,
        min(scalar.eval_time_ms / simd.eval_time_ms) ,
        from (select * from simd_scalarAgg_sf5 where is_scalar='False') as simd JOIN
        (select * from simd_scalarAgg_sf5 where is_scalar='True') as scalar USING (sf, n, num_threads, prune_label, qid)
        group by sf, qid, prune_label
        order by sf, qid, prune_label
        """).df()
#print(summary)
