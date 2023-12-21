  select 
  0 as out_index,
    s_rid as supplier, l_rid as lineitem, l_rid2 as lineitem_2
  from
  (
  select
  *,
         supplier.rowid as s_rid from supplier, (
    select * from (
              select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
              from lineitem
              where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
              group by l_suppkey
    ) as qbase, (select *, rowid as l_rid from lineitem as l1 where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month)
    where l_suppkey=supplier_no
  ) as revenue_plus
  WHERE
      s_suppkey = supplier_no
      AND total_revenue = (
          SELECT  max(total_revenue)
          FROM (
              select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
              from lineitem
              where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
              group by l_suppkey
          ) as revenue0)
  ORDER BY
      s_suppkey
    )
    as Qplus
    ,  (
        select * from ( select max(total_revenue) as max_total_revenue from (
              select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
              from lineitem
              where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
              group by l_suppkey
            )), (
            select * from (
              select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
              from lineitem
              where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
              group by l_suppkey
            ) as base, (select *, rowid as l_rid2 from lineitem
              where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
            ) where l_suppkey=supplier_no 
          ) as rev_plus 
     ) where Qplus.total_revenue=max_total_revenue
