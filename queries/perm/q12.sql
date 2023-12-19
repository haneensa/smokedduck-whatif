  SELECT Qbase.out_index-1 as out_index, orders, lineitem
  FROM (
      SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
      l_shipmode,
          sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
          sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
      FROM (
        SELECT l_shipmode, o_orderpriority
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
            AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
            AND l_receiptdate >= CAST('1994-01-01' AS date)
            AND l_receiptdate < CAST('1995-01-01' AS date)
      )
      GROUP BY l_shipmode
  ) as Qbase join (
    SELECT orders.rowid as orders, lineitem.rowid as lineitem, l_shipmode
    FROM orders, lineitem
    WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
        AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
        AND l_receiptdate >= CAST('1994-01-01' AS date)
        AND l_receiptdate < CAST('1995-01-01' AS date)
  ) as Qplus using (l_shipmode)
