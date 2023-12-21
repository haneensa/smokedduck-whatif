  select  out_index-1 as out_index,
  c_rid as customer, o_rid as orders, l_rid as lineitem, l_rid2 as lineitem_2
  from (
        SELECT  
      ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
        c_name,  c_custkey,  o_orderkey,  o_orderdate,  o_totalprice,  sum(l_quantity) as sum_l_quantity
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (
                SELECT l_orderkey
                FROM lineitem
                GROUP BY l_orderkey
                HAVING sum(l_quantity) > 300)
            AND c_custkey = o_custkey
            AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
  ) as q join  (
    select * from
    ( select *, customer.rowid as c_rid, 
      orders.rowid as o_rid, 
      lineitem.rowid as l_rid from customer, orders, lineitem
    ) as in_plus,( select sum_l_quantity_300,
                    l_orderkey, in_l.rowid as l_rid2 from  (select l_orderkey , 
                      sum(l_quantity) as sum_l_quantity_300
                    from lineitem group by l_orderkey
                    having sum(l_quantity) > 300) join lineitem as in_l using (l_orderkey) ) as cb
      WHERE in_plus.o_orderkey=cb.l_orderkey
          AND in_plus.c_custkey = in_plus.o_custkey
          AND in_plus.o_orderkey = in_plus.l_orderkey
  ) as qplus using (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
