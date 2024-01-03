SELECT
    o_orderpriority
FROM
    orders
WHERE
    EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
