  select Qbase.out_index-1 as out_index, part,  supplier, lineitem,  partsupp,
         orders, nation
  from (
    SELECT part.rowid as part, supplier.rowid as supplier,
           lineitem.rowid as lineitem, partsupp.rowid as partsupp,
           orders.rowid as orders, nation.rowid as nation,
           n_name AS nation_name, extract(year FROM o_orderdate) AS o_year
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
  ) as Qplus join (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    nation_name, o_year, sum(amount) AS sum_profit
    FROM (
      SELECT n_name AS nation_name, extract(year FROM o_orderdate) AS o_year,
             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
      FROM part, supplier, lineitem, partsupp, orders, nation
      WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
          AND ps_partkey = l_partkey AND p_partkey = l_partkey
          AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
          AND p_name LIKE '%green%'
    )
    GROUP BY nation_name, o_year
  ) as Qbase using (nation_name, o_year)
