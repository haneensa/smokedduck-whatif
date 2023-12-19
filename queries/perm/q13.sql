  select groups2.out_index-1 as out_index,  customer, orders
  from (
    SELECT customer.rowid as customer, orders.rowid as orders,
           c_custkey, o_orderkey
    FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
  ) as joins1 left outer join (
    SELECT c_custkey, count(o_orderkey) as c_count
    FROM (
      SELECT c_custkey, o_orderkey
      FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
    )
    GROUP BY c_custkey
  ) as groups1 using (c_custkey) join (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    c_count, count(*) AS custdist
    FROM (
        SELECT c_custkey, count(o_orderkey) as c_count
        FROM (
          SELECT c_custkey, o_orderkey
          FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
        )
        GROUP BY c_custkey
    )
    GROUP BY c_count
  ) as groups2 using (c_count)
