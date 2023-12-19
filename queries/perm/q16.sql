  select groups.out_index-1 as out_index, ps_rid as partsupp, p_rid as part, s_rid as supplier
  from (
        SELECT
  ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
        p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt
        FROM (
            SELECT p_brand, p_type, p_size, ps_suppkey
            FROM partsupp, part
            WHERE p_partkey = ps_partkey
                AND p_brand <> 'Brand#45'
                AND p_type NOT LIKE 'MEDIUM POLISHED%'
                AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND ps_suppkey NOT IN (
                    SELECT s_suppkey FROM supplier  WHERE  s_comment LIKE '%Customer%Complaints%')
        )
        GROUP BY p_brand, p_type, p_size
    ) as groups join ( select ps_rid, p_rid, s_rid, p_brand, p_type, p_size from (
              SELECT partsupp.rowid as ps_rid, part.rowid as p_rid,
                    p_brand, p_type, p_size
              FROM partsupp, part
              WHERE p_partkey = ps_partkey
                  AND p_brand <> 'Brand#45'  AND p_type NOT LIKE 'MEDIUM POLISHED%'
                  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                  AND ps_suppkey NOT IN (
                      SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%')
          ) as Q,
          (SELECT supplier.rowid as s_rid FROM  supplier WHERE  s_comment LIKE '%Customer%Complaints%') as cb
  ) as Qplus using (p_brand, p_type, p_size)
