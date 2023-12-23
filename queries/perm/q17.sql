  SELECT
        0 as out_index,  
        Nl.rowid as lineitem, 
          Tplus.Prov_lineitem_rowid as lineitem_2,
          Tplus.Prov_part_rowid as part
FROM      lineitem as Nl, (select 
                                lineitem.rowid as Prov_lineitem_rowid, 
                                part.rowid as Prov_part_rowid, 
                                l_quantity,
                                p_partkey, l_partkey
          from lineitem, part
          where l_partkey=p_partkey and p_brand = 'Brand#23' and  p_container = 'MED BOX' and 
                l_quantity < (SELECT  0.2 * avg(l1.l_quantity) FROM lineitem as l1  WHERE  l1.l_partkey = p_partkey)
          ) as Tplus
WHERE     Tplus.l_quantity < (SELECT  0.2 * avg(l0.l_quantity) FROM lineitem as l0 WHERE l0.l_partkey = Tplus.p_partkey)
AND       EXISTS(
              SELECT  avg_yearly, l2_l_partkey
              FROM   ( select avg_yearly, l2_l_partkey from 
                       (SELECT  0.2 * avg(l3.l_quantity) as avg_yearly FROM lineitem as l3  WHERE l3.l_partkey = Tplus.p_partkey),
                       (SELECT  l2.l_partkey as l2_l_partkey
                        FROM    lineitem as l2
                        WHERE   l2.l_partkey = Tplus.p_partkey
                       )
                     ) AS Tsub_plus
              WHERE   Tsub_plus.l2_l_partkey = Nl.l_partkey
              AND Tplus.l_quantity < Tsub_plus.avg_yearly
              OR NOT (Tplus.l_quantity < (SELECT  0.2 * avg(l4.l_quantity) FROM lineitem as l4 WHERE l4.l_partkey = Tplus.p_partkey))
          )

