select out_index-1 as out_index, customer, customer_2 from (
    select groups.*, customer_rowid_1 as customer
    from (
      SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index
      FROM (
        SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
        FROM customer
        WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal >  (select avg(c_acctbal) from (
                SELECT c_acctbal FROM customer
                WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
              ))
              AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey=c_custkey)
      )
      GROUP BY cntrycode
    ) as groups join (
      SELECT customer.rowid as customer_rowid_1, substring(c_phone FROM 1 FOR 2) AS cntrycode, c_custkey
      FROM customer
      WHERE substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND c_acctbal >  (select avg(c_acctbal) from (
              SELECT c_acctbal FROM customer
              WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
            ))
            AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey=c_custkey)
    ) as joins1 using (cntrycode)
  ) as main, (select customer.rowid as customer_2
    FROM customer
      WHERE c_acctbal > 0.00 AND substring(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17')
  ) as subq
