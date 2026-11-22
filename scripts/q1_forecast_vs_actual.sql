SELECT
    h.HospitalName,
    d.YearMonth,
    SUM(f.ForecastDemandPacks)    AS forecast_packs,
    SUM(f.ConsumedQuantityPacks)  AS consumed_packs,
    SUM(f.ForecastDemandPacks) / NULLIF(SUM(f.ConsumedQuantityPacks), 0) AS forecast_to_actual_ratio
FROM FactDemand f
JOIN DimHospital h ON f.HospitalKey = h.HospitalKey
JOIN DimDate d     ON f.DateKey = d.DateKey
GROUP BY h.HospitalName, d.YearMonth
ORDER BY h.HospitalName, d.YearMonth;
