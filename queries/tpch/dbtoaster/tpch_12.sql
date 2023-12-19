SELECT
    l.shipmode,
    sum(
        CASE WHEN o.orderpriority = '1-URGENT'
            OR o.orderpriority = '2-HIGH' THEN
            1
        ELSE
            0
        END) AS high_line_count,
    sum(
        CASE WHEN o.orderpriority <> '1-URGENT'
            AND o.orderpriority <> '2-HIGH' THEN
            1
        ELSE
            0
        END) AS low_line_count
FROM
    orders o,
    lineitem l
WHERE
    o.orderkey = l.orderkey
    AND l.shipmode IN LIST ('MAIL', 'SHIP')
    AND l.commitdate < l.receiptdate
    AND l.shipdate < l.commitdate
    AND l.receiptdate >= DATE('1994-01-01')
    AND l.receiptdate < DATE('1995-01-01')
GROUP BY
    l.shipmode;
