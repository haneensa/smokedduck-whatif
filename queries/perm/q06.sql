  select lineitem, out_index-1 as out_index 
  from (
    SELECT lineitem.rowid as lineitem
    FROM lineitem
    WHERE l_shipdate >= CAST('1994-01-01' AS date)
        AND l_shipdate < CAST('1995-01-01' AS date)
        AND l_discount BETWEEN 0.05 AND 0.07
        AND l_quantity < 24
  ) as Qplus, (
    SELECT  
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    sum(l_extendedprice * l_discount) AS revenue
    FROM (
      SELECT l_extendedprice, l_discount
      FROM lineitem
      WHERE l_shipdate >= CAST('1994-01-01' AS date)
          AND l_shipdate < CAST('1995-01-01' AS date)
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
    )
  ) as Qbase
