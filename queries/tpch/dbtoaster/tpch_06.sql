SELECT
    sum(l.extendedprice * l.discount) AS revenue
FROM
    lineitem l
WHERE
    l.shipdate >= DATE('1994-01-01')
    AND l.shipdate < DATE('1995-01-01')
    AND l.discount BETWEEN 0.05
    AND 0.07
    AND l.quantity < 24;
