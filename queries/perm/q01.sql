select Qbase.out_index-1 as out_index, Qplus.lineitem
from (
  SELECT
      ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
      l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order
  FROM (
    SELECT
        lineitem.rowid as lineitem,
        l_returnflag,l_linestatus,l_quantity,l_extendedprice,l_discount,l_tax
    FROM lineitem
    WHERE l_shipdate <= CAST('1998-09-02' AS date)
  )
  GROUP BY l_returnflag, l_linestatus
) as Qbase join (
  SELECT
      lineitem.rowid as lineitem, l_returnflag, l_linestatus
  FROM lineitem
  WHERE l_shipdate <= CAST('1998-09-02' AS date)
) as Qplus using (l_returnflag, l_linestatus)
