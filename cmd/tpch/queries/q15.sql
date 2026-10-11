SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier JOIN tpch_revenue ON s_suppkey = supplier_no
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            max(total_revenue)
        FROM tpch_revenue)
ORDER BY
    s_suppkey;
