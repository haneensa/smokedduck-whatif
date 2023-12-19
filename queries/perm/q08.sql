  select Qbase.out_index-1 as out_index, part, supplier, lineitem, orders,
         customer, nation, nation_2, region
  from (
    SELECT part.rowid as part, supplier.rowid as supplier,
           lineitem.rowid as lineitem, orders.rowid as orders,
           customer.rowid as customer, n1.rowid as nation,
           n2.rowid as nation_2, region.rowid as region,
           extract(year FROM o_orderdate) AS o_year
    FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL'
  ) as Qplus join (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    o_year, sum(
              CASE WHEN nation = 'BRAZIL' THEN
                  volume
              ELSE
                  0
              END) / sum(volume) AS mkt_share
    FROM (
          SELECT 
          extract(year FROM o_orderdate) AS o_year, n2.n_name as nation, l_extendedprice * (1 - l_discount) AS volume
          FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
          WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
              AND l_orderkey = o_orderkey AND o_custkey = c_custkey
              AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
              AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
              AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
              AND CAST('1996-12-31' AS date)
              AND p_type = 'ECONOMY ANODIZED STEEL'
    )
    GROUP BY o_year
  ) as Qbase using (o_year)
