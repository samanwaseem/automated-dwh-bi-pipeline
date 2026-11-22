SELECT
    d.YearMonth,
    SUM(f.ForecastDemandPacks)    AS forecast_packs,
    SUM(f.ConsumedQuantityPacks)  AS consumed_packs
FROM FactDemand f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.YearMonth
ORDER BY d.YearMonth;
