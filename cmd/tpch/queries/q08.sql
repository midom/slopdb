SELECT
    o_year,
    sum(
        CASE WHEN nation = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) / sum(volume) AS mkt_share
FROM (
    SELECT
        SUBSTR(o_orderdate, 1, 4) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM
        part JOIN lineitem ON p_partkey = l_partkey JOIN supplier ON s_suppkey = l_suppkey JOIN orders ON l_orderkey = o_orderkey JOIN customer ON o_custkey = c_custkey JOIN nation n1 ON c_nationkey = n1.n_nationkey JOIN nation n2 ON s_nationkey = n2.n_nationkey JOIN region ON n1.n_regionkey = r_regionkey
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN '1995-01-01'
        AND '1996-12-31'
        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
