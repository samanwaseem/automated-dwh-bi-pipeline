-- group by InternalSKU / MedName, not MedKey: a medicine can have several SCD2 versions
SELECT
    p.PharmacyName,
    m.MedName,
    SUM(f.QuantitySoldPacks)   AS packs_sold,
    SUM(f.NetSalesAmountUSD)   AS net_sales_usd,
    SUM(f.GrossMarginUSD)      AS gross_margin_usd
FROM FactSales f
JOIN DimPharmacy p ON f.PharmacyKey = p.PharmacyKey
JOIN DimMedicine m ON f.MedKey = m.MedKey
GROUP BY p.PharmacyKey, p.PharmacyName, m.InternalSKU, m.MedName
ORDER BY net_sales_usd DESC
LIMIT 10;
