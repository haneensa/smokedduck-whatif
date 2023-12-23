SELECT out_index-1 as out_index, p_rid as part, s_rid as supplier, s_rid2 as supplier_2,
      ps_rid as partsupp, ps_rid2 as partsupp_2, n_rid as nation, n_rid2 as nation_2, r_rid as region, r_rid2 as region_2
  from (
    SELECT 
          partsupp.rowid as ps_rid2,
           supplier.rowid as s_rid2,
           nation.rowid as n_rid2,
          region.rowid as r_rid2,
          ps_partkey, ps_supplycost
    FROM partsupp, supplier, nation, region
    WHERE s_suppkey = ps_suppkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'EUROPE'
  ) as joins1 join (
    SELECT ps_partkey, min(ps_supplycost) as min_ps_supplycost
    FROM (
      SELECT ps_partkey, ps_supplycost
      FROM partsupp, supplier, nation, region
      WHERE s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
    )
    GROUP BY ps_partkey
  ) as group1 using ( ps_partkey ) join (
    SELECT
          ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
          part.rowid as p_rid,
          supplier.rowid as s_rid,
          partsupp.rowid as ps_rid,
          nation.rowid as n_rid,
          region.rowid as r_rid,
        ps_supplycost,s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
    FROM part, supplier, partsupp, nation, region
    WHERE p_partkey = partsupp.ps_partkey
        AND s_suppkey = partsupp.ps_suppkey AND p_size = 15
        AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey AND r_name = 'EUROPE'
      AND ps_supplycost = (
          SELECT min(ps_supplycost)
          FROM partsupp, supplier, nation, region
          WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE')
  ) as joins2 on (group1.min_ps_supplycost=joins2.ps_supplycost)
