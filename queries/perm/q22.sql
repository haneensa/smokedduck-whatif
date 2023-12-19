  select Qbase.out_index-1 as out_index, c_rid1 as customer, c_rid3 as customer_2
  from (
      SELECT 
          ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
      cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
      FROM (
          SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode,  c_acctbal
          FROM customer
          WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal > (
                  SELECT avg(c_acctbal) FROM customer  WHERE c_acctbal > 0.00
                      AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'
                  ))
              AND NOT EXISTS (  SELECT *  FROM orders  WHERE o_custkey = c_custkey)
      ) AS custsale
      GROUP BY cntrycode
  ) as Qbase join (
    SELECT * 
    FROM  (
      SELECT *, substring(c_phone FROM 1 FOR 2) AS cntrycode,  c_acctbal,
      customer.rowid as c_rid1
      FROM customer
      WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal > (
                  SELECT avg(c_acctbal) FROM customer  WHERE c_acctbal > 0.00
                      AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17'
                  ))
              AND NOT EXISTS (  SELECT *  FROM orders  WHERE o_custkey = c_custkey)
    ) as in_plus,
      ( select c_rid3, avg_c_acctbal from
           ( SELECT avg(c_acctbal) as avg_c_acctbal FROM customer
            WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
          ), (select customer.rowid as c_rid3 from customer 
            WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
          )
        ) as Qsub_plus1
  ) as Qplus using (cntrycode)
