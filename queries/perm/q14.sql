  select Qbase.out_index-1as out_index,  lineitem, part
  from (
    SELECT lineitem.rowid as lineitem, part.rowid as part
    FROM  lineitem, part
    WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01'
        AND l_shipdate < CAST('1995-10-01' AS date)
  ) as Qplus, (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
        100.00 * sum(
            CASE WHEN p_type LIKE 'PROMO%' THEN
                l_extendedprice * (1 - l_discount)
            ELSE
                0
            END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
    FROM (
      SELECT p_type, l_extendedprice, l_discount
      FROM lineitem, part
      WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01'
          AND l_shipdate < CAST('1995-10-01' AS date)
    )
  ) as Qbase
