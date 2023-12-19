  select out_index-1 as out_index, lineitem, part from (
        SELECT
          ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
            sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM (
              SELECT
                  l_extendedprice, l_discount
              FROM
                  lineitem,
                  part
              WHERE (p_partkey = l_partkey
                  AND p_brand = 'Brand#12'
                  AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                  AND l_quantity >= 1
                  AND l_quantity <= 1 + 10
                  AND p_size BETWEEN 1 AND 5
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON')
                  OR (p_partkey = l_partkey
                      AND p_brand = 'Brand#23'
                      AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                      AND l_quantity >= 10
                      AND l_quantity <= 10 + 10
                      AND p_size BETWEEN 1 AND 10
                      AND l_shipmode IN ('AIR', 'AIR REG')
                      AND l_shipinstruct = 'DELIVER IN PERSON')
                  OR (p_partkey = l_partkey
                      AND p_brand = 'Brand#34'
                      AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                      AND l_quantity >= 20
                      AND l_quantity <= 20 + 10
                      AND p_size BETWEEN 1 AND 15
                      AND l_shipmode IN ('AIR', 'AIR REG')
                      AND l_shipinstruct = 'DELIVER IN PERSON')
          )
    ) as Qbase, (
      SELECT
          lineitem.rowid as lineitem,
          part.rowid as part
      FROM
          lineitem,
          part
      WHERE (p_partkey = l_partkey
          AND p_brand = 'Brand#12'
          AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
          AND l_quantity >= 1
          AND l_quantity <= 1 + 10
          AND p_size BETWEEN 1 AND 5
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON')
          OR (p_partkey = l_partkey
              AND p_brand = 'Brand#23'
              AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
              AND l_quantity >= 10
              AND l_quantity <= 10 + 10
              AND p_size BETWEEN 1 AND 10
              AND l_shipmode IN ('AIR', 'AIR REG')
              AND l_shipinstruct = 'DELIVER IN PERSON')
          OR (p_partkey = l_partkey
              AND p_brand = 'Brand#34'
              AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
              AND l_quantity >= 20
              AND l_quantity <= 20 + 10
              AND p_size BETWEEN 1 AND 15
              AND l_shipmode IN ('AIR', 'AIR REG')
              AND l_shipinstruct = 'DELIVER IN PERSON')
  ) as Qplus
