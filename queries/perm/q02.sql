SELECT *
FROM (
    SELECT  part.rowid as p_rid1, 
            supplier.rowid as s_rid1,
            partsupp.rowid as ps_rid1,
            nation.rowid as n_rid1,
            region.rowid as r_rid1,
            s_acctbal, s_name, n_name, p_partkey,  p_mfgr, s_address, s_phone, s_comment
    FROM part, supplier, partsupp, nation, region
    WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15
    AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT min(ps_supplycost)
        FROM partsupp,supplier, nation, region
        WHERE  p_partkey = ps_partkey AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
          )
) AS qplus,
(
    SELECT partsupp.rowid as ps_rid2, 
           supplier.rowid as s_rid2,
           nation.rowid as n_rid2, 
          region.rowid as r_rid2
    FROM partsupp, supplier, nation, region
    WHERE  r_name = 'EUROPE'
) AS cb
WHERE
    EXISTS (
      SELECT *
      FROM ( select t1.*, t2.*
        from
        (SELECT min(ps_supplycost)  AS min_ps_supplycost 
          FROM partsupp, supplier, nation, region
          WHERE qplus.p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE') as t1,
        (SELECT  partsupp.rowid as ps_rid3, 
           supplier.rowid as s_rid3,
           nation.rowid as n_rid3, 
          region.rowid as r_rid3
          FROM partsupp, supplier, nation, region
          WHERE qplus.p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE' ) as t2
          ) as Qsub_plus
      WHERE  
      Qsub_plus.ps_rid3=cb.ps_rid2
      AND Qsub_plus.s_rid3=cb.s_rid2
      AND Qsub_plus.n_rid3=cb.n_rid2
      AND Qsub_plus.r_rid3=cb.r_rid2
    )
