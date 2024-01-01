  SELECT out_index-1 as out_index, o_rid as orders, l_rid as lineitem
  FROM (
    SELECT o_orderpriority, count(*) AS order_count,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index
    FROM (
      SELECT o_orderpriority
      FROM orders
      WHERE o_orderdate >= CAST('1993-07-01' AS date)
          AND o_orderdate < CAST('1993-10-01' AS date)
          AND EXISTS (SELECT * FROM lineitem
                       WHERE l_commitdate < l_receiptdate
                         and l_orderkey=o_orderkey
                      )
    )
    GROUP BY o_orderpriority
  ) as groups join (
    SELECT orders.rowid as o_rid, o_orderpriority, o_orderkey
    FROM orders
    WHERE o_orderdate >= CAST('1993-07-01' AS date)
        AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS (SELECT * FROM lineitem
                    WHERE l_commitdate < l_receiptdate
                      and l_orderkey=o_orderkey
                    )
  ) as select_st USING (o_orderpriority) join (
    SELECT lineitem.rowid as l_rid, l_orderkey, l_commitdate, l_receiptdate
    FROM lineitem
    WHERE l_commitdate < l_receiptdate
  ) as exists_st on ( select_st.o_orderkey=exists_st.l_orderkey)
