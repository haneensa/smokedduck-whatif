  select out_index-1 as out_index, partsupp_rowid as partsupp, part_rowid as part, lineitem_rowid as lineitem
  from (
    SELECT supplier.rowid as supplier_rowid, nation.rowid as nation_rowid,
          ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
           s_name, s_address, s_suppkey
    FROM supplier, nation
    WHERE s_suppkey IN ( 
          SELECT ps_suppkey FROM partsupp
          WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
            AND ps_availqty > (
                          SELECT 0.5 * sum(l_quantity)
                          FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                              AND l_shipdate >= CAST('1994-01-01' AS date)
                              AND l_shipdate < CAST('1995-01-01' AS date)))
      AND s_nationkey = n_nationkey
      AND n_name = 'CANADA'
  ) as final, (
    SELECT partsupp.rowid as partsupp_rowid, ps_suppkey, ps_partkey
    FROM partsupp
    WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
      AND ps_availqty > (
                    SELECT 0.5 * sum(l_quantity)
                    FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                        AND l_shipdate >= CAST('1994-01-01' AS date)
                        AND l_shipdate < CAST('1995-01-01' AS date))
  ) as in1, (
    SELECT part.rowid as part_rowid, p_partkey
    FROM part
    WHERE p_name LIKE 'forest%'
  ) as in2, (
    SELECT lineitem.rowid as lineitem_rowid, l_partkey, l_suppkey, l_quantity
    FROM lineitem
    WHERE l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
  ) as in3_select
  where final.s_suppkey=in1.ps_suppkey
  and in1.ps_partkey=in2.p_partkey
  and in3_select.l_partkey=in1.ps_partkey
  and in3_select.l_suppkey=in1.ps_suppkey
