  SELECT Qbase.out_index-1 as out_index, customer, orders, lineitem
  FROM (
    SELECT customer.rowid as customer, orders.rowid as orders, lineitem.rowid as lineitem,
        l_orderkey, o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
        AND l_shipdate > CAST('1995-03-15' AS date)
  ) as Qplus join (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
        l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
    FROM (
      SELECT l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
      FROM customer, orders, lineitem
      WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
          AND l_shipdate > CAST('1995-03-15' AS date)
    )
    GROUP BY l_orderkey, o_orderdate, o_shippriority
  ) as Qbase using (l_orderkey, o_orderdate, o_shippriority)
