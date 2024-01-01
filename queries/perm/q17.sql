  select 0 as out_index, lineitem.rowid as lineitem_2,
         lineitem_rowid as lineitem, part_rowid as part
  from  (
        SELECT
            sum(l_extendedprice) / 7.0 AS avg_yearly
        FROM (
            SELECT l_extendedprice, l_partkey
            FROM lineitem, part
            WHERE p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < (
                    SELECT
                        0.2 * avg(l_quantity)
                    FROM
                        lineitem
                    WHERE
                        l_partkey = p_partkey)
            )
    ) as groups, (
        SELECT lineitem.rowid as lineitem_rowid, 
        part.rowid as part_rowid, l_extendedprice, 
        l_partkey
        FROM lineitem, part
        WHERE p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container = 'MED BOX'
            AND l_quantity < (
                SELECT
                    0.2 * avg(l_quantity)
                FROM
                    lineitem
                WHERE
                    l_partkey = p_partkey)
  ) as joins, lineitem
  where lineitem.l_partkey=joins.l_partkey
