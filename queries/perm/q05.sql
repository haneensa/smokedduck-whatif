  select Qbase.out_index-1 as out_index, customer, orders, lineitem, supplier,
         nation, region
  from (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM (
      SELECT n_name, l_extendedprice, l_discount
      FROM customer, orders, lineitem, supplier, nation, region
      WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
         AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
         AND r_name = 'ASIA' AND o_orderdate >= CAST('1994-01-01' AS date)
         AND o_orderdate < CAST('1995-01-01' AS date)
    )
    GROUP BY n_name
  ) as Qbase join (
    SELECT customer.rowid as customer, orders.rowid as orders,
           lineitem.rowid as lineitem, supplier.rowid as supplier,
           nation.rowid as nation, region.rowid as region,
           n_name
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
       AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
       AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
       AND r_name = 'ASIA' AND o_orderdate >= CAST('1994-01-01' AS date)
       AND o_orderdate < CAST('1995-01-01' AS date)
  ) as Qplus using (n_name)
