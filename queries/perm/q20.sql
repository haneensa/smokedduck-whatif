          ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
  select *
  from (
    SELECT *, supplier.rowid as s_rid, nation.rowid as n_rid FROM supplier, nation
  ) as qbase_plus,  partsupp as cb_partsupp, part as cb_part, lineitem as cb_lineitem
  WHERE qbase_plus.s_suppkey IN ( SELECT ps_suppkey FROM partsupp
                                  WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
                                  AND ps_availqty > (SELECT 0.5 * sum(l_quantity)
                                                      FROM lineitem 
                                                      WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                                                      AND l_shipdate >= CAST('1994-01-01' AS date)
                                                      AND l_shipdate < CAST('1995-01-01' AS date)
                                                      )
                                    )
        AND qbase_plus.s_nationkey = qbase_plus.n_nationkey  AND qbase_plus.n_name = 'CANADA'
        AND EXISTS (
          select *
          from ( select *, in_cb_part.rowid as p_rid, in_cb_lineitem.rowid as l_rid from ( 
            SELECT *, rowid as ps_rid  FROM partsupp
            ) as in1_qbase_plus, part as in_cb_part, lineitem as in_cb_lineitem
            WHERE in1_qbase_plus.ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
              AND in1_qbase_plus.ps_availqty > (
                            SELECT 0.5 * sum(l_quantity)
                            FROM lineitem WHERE l_partkey = in1_qbase_plus.ps_partkey AND l_suppkey = in1_qbase_plus.ps_suppkey
                                AND l_shipdate >= CAST('1994-01-01' AS date)
                                AND l_shipdate < CAST('1995-01-01' AS date)
                              )
              AND EXISTS ( select * from ( select * from (
                            SELECT 0.5 * sum(l_quantity) as sum_l_quantity
                            FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = in1_qbase_plus.ps_suppkey
                                AND l_shipdate >= CAST('1994-01-01' AS date)
                                AND l_shipdate < CAST('1995-01-01' AS date)
                              ), (
                            SELECT *, rowid as l_sub1_rid
                            FROM lineitem WHERE l_partkey = in1_qbase_plus.ps_partkey AND l_suppkey = in1_qbase_plus.ps_suppkey
                                AND l_shipdate >= CAST('1994-01-01' AS date)
                                AND l_shipdate < CAST('1995-01-01' AS date)
                            )
                          ) as in1_sub1_plus
                          where in1_sub1_plus.l_sub1_rid=in_cb_lineitem.rowid
                          and in1_qbase_plus.ps_availqty > in1_sub1_plus.sum_l_quantity
              ) AND EXISTS (
                select * from part as p
                WHERE  p.p_name LIKE 'forest%'
                and p.rowid=in_cb_part.rowid
                and in1_qbase_plus.ps_partkey=p.p_partkey
              )
          ) as in1_plus
          where in1_plus.ps_rid=cb_partsupp.rowid
          and in1_plus.p_rid=cb_part.rowid
          and in1_plus.l_rid=cb_lineitem.rowid
          and qbase_plus.s_suppkey=in1_plus.ps_suppkey
        )

