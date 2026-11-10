SELECT
    h.hospital_name,
    d.year_month,
    SUM(f.forecast_demand_packs)  AS forecast_packs,
    SUM(f.consumed_quantity_packs) AS consumed_packs,
    SUM(f.forecast_demand_packs) / NULLIF(SUM(f.consumed_quantity_packs), 0) AS forecast_to_actual_ratio
FROM fact_demand f
JOIN dim_hospital h ON f.hospital_key = h.hospital_key
JOIN dim_date d     ON f.date_key = d.date_key
GROUP BY h.hospital_name, d.year_month
ORDER BY h.hospital_name, d.year_month;