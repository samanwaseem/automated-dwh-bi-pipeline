SELECT
    p.pharmacy_name,
    m.generic_name,
    SUM(f.quantity_sold_packs)    AS packs_sold,
    SUM(f.net_sales_amount_usd)   AS net_sales_usd,
    SUM(f.gross_margin_usd)       AS gross_margin_usd
FROM fact_sales f
JOIN dim_pharmacy p ON f.pharmacy_key = p.pharmacy_key
JOIN dim_medicine m ON f.medicine_key = m.medicine_key
GROUP BY p.pharmacy_key, p.pharmacy_name, m.medicine_key, m.generic_name
ORDER BY net_sales_usd DESC
LIMIT 10;