  SELECT Qbase.out_index-1 as out_index, customer, orders, lineitem,  nation
  FROM (
    SELECT customer.rowid as customer, orders.rowid as orders,
           lineitem.rowid as lineitem, nation.rowid as nation,
           c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, orders, lineitem, nation
    WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
        AND o_orderdate >= CAST('1993-10-01' AS date)
        AND o_orderdate < CAST('1994-01-01' AS date)
        AND l_returnflag = 'R' AND c_nationkey = n_nationkey
  ) as Qplus join (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue,
           c_acctbal, n_name, c_address, c_phone, c_comment
    FROM (
        SELECT l_extendedprice, l_discount, c_custkey, c_name, c_acctbal, n_name,
               c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND o_orderdate >= CAST('1993-10-01' AS date)
            AND o_orderdate < CAST('1994-01-01' AS date)
            AND l_returnflag = 'R' AND c_nationkey = n_nationkey
    )
    GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
  ) as Qbase using (c_custkey, c_name, c_acctbal, c_phone, c_name, c_address, c_comment)
