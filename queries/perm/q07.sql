  select Qbase.out_index-1 as out_index, supplier, lineitem, orders, customer, nation,  nation_2
  from (
    SELECT supplier.rowid as supplier, lineitem.rowid as lineitem, orders.rowid as orders,
        customer.rowid as customer, n1.rowid as nation_2, n2.rowid  as nation,
        n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year
    FROM supplier, lineitem, orders, customer, nation n1, nation n2
    WHERE s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE'
                AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY'
                AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
  ) as Qplus join (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    supp_nation, cust_nation, l_year, sum(volume) AS revenue
    FROM (
      SELECT
      n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year,
             l_extendedprice * (1 - l_discount) AS volume
      FROM supplier, lineitem, orders, customer, nation n1, nation n2
      WHERE s_suppkey = l_suppkey
          AND o_orderkey = l_orderkey AND c_custkey = o_custkey
          AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
          AND ((n1.n_name = 'FRANCE'
                  AND n2.n_name = 'GERMANY')
              OR (n1.n_name = 'GERMANY'
                  AND n2.n_name = 'FRANCE'))
          AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
          AND CAST('1996-12-31' AS date)
    )
    GROUP BY supp_nation, cust_nation, l_year
  ) as Qbase using (supp_nation, cust_nation, l_year)
