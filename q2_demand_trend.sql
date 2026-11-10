SELECT
    d.year_month,
    SUM(f.forecast_demand_packs)   AS forecast_packs,
    SUM(f.consumed_quantity_packs) AS consumed_packs
FROM fact_demand f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year_month
ORDER BY d.year_month;