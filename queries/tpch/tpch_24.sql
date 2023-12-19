SELECT
    p_partkey,
FROM
    part,
    partsupp,
WHERE
    p_partkey = ps_partkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
        WHERE
            p_partkey = ps_partkey
            )
LIMIT 100;
