          ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
  select s_name, numwait,
         s_rid, l_rid, l_rid2, o_rid, n_rid
  from (
      SELECT s_name, count(*) AS numwait
      FROM (
          SELECT s_name, s_suppkey, o_orderkey
          FROM supplier, lineitem l1, orders, nation
          WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
              AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
              AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
      )
      GROUP BY s_name
      ORDER BY numwait DESC, s_name
      LIMIT 100
    ) as qbase join (
    select s_rid, l_rid, o_rid, n_rid, cb_sub1.l_rid2, s_name from (
            SELECT supplier.rowid as s_rid, l1.rowid as l_rid,
                   orders.rowid as o_rid, nation.rowid as n_rid,
                  s_name, l_orderkey, l_suppkey
            FROM supplier, lineitem l1, orders, nation
            WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
                AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
                AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
                AND NOT EXISTS (SELECT * FROM lineitem l3  WHERE   l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
                AND s_nationkey = n_nationkey
                AND n_name = 'SAUDI ARABIA'
      ) as in_plus, ( select rowid as l_rid2 from lineitem )  as cb_sub1 
      where 
         EXISTS ( select * from lineitem as l4
          where
          l4.l_orderkey=in_plus.l_orderkey and
          l4.l_suppkey <> in_plus.l_suppkey 
          and cb_sub1.l_rid2=l4.rowid
        )
  )  using (s_name)
