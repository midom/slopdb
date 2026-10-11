CREATE VIEW tpch_revenue AS     SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= '1996-01-01'
      AND l_shipdate < '1996-04-01'
    GROUP BY
        supplier_no
;
