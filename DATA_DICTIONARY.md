# Data Dictionary

**Project:** Pharma Cold Chain DWH & BI  
**Architecture:** CSV files -> PostgreSQL 16 (staging) -> DuckDB (star schema in `warehouse.duckdb`)  
**Orchestration:** Apache Airflow (`pharma_sdi_pipeline`)  
**Refresh Batches:** `initial`, `refresh1`, `refresh2`

---

## 1. Oversampling & Incremental Refresh Strategy (`refresh1` & `refresh2`)

Two synthetic monthly refresh batches (`data/refresh1/` and `data/refresh2/`) were created to test incremental loading, data quality checks (`validate_staging`), and deduplicated star schema updates (`build_star_schema`):

- **shifted dates:** `SalesDate`, `SalesTimestampUTC`, `PharmacyDeliveryDate`, and `DemandMonth` were moved forward into the next months. `DemandMonth` was also cleaned up into one consistent format so it does not break during parsing.
- **unique keys:** `SalesTransactionID` and `HospitalDemandRecordID` were given new offset ids so new batches append cleanly without clashing with old rows, while still matching existing `MedicineID`, `PharmacyID`, and `HospitalID` records.
- **realistic numbers:** sales and demand quantities (`QuantitySoldPacks`, `ForecastDemandPacks`, `ConsumedQuantityPacks`, etc.) were varied slightly so the monthly trends look real, while keeping all the math consistent (`GrossSalesAmountUSD`, `NetSalesAmountUSD`, `GrossMarginUSD`, `ClosingStockPacks`).

---

## 2. Source & Staging Layer (CSV to PostgreSQL)

### 2.1 `01_master_data.csv` (product and cold-chain master, 34 columns)
*grain: one row per medicine sku.*

| Column | Type | Description |
| :--- | :--- | :--- |
| `MedicineID` | `VARCHAR` | main id for the medicine |
| `InternalSKU` | `VARCHAR` | internal sku code used by the company |
| `GTIN14` | `VARCHAR` | 14-digit global barcode number for the product |
| `GenericName` | `VARCHAR` | generic (non-brand) name of the drug |
| `FictionalBrandName` | `VARCHAR` | brand name the medicine is sold under |
| `DiseaseCategory` | `VARCHAR` | general disease group this medicine treats |
| `ICD10Category` | `VARCHAR` | standard icd-10 medical diagnosis code |
| `ATCCode` | `VARCHAR` | standard atc drug classification code |
| `TherapeuticClass` | `VARCHAR` | drug class based on how it works in the body |
| `DosageForm` | `VARCHAR` | physical form like tablet, vial, or injection |
| `Strength` | `VARCHAR` | dose strength or concentration |
| `RouteOfAdministration` | `VARCHAR` | how the drug is taken (oral, iv, subcutaneous, etc.) |
| `PackSize` | `INT` | how many units come inside one pack |
| `BaseUnitOfMeasure` | `VARCHAR` | base unit used to count stock (pack, vial, tablet) |
| `PrescriptionClass` | `VARCHAR` | whether it needs a prescription (rx) or is over the counter (otc) |
| `ControlledMedicineFlag` | `BOOLEAN` | true if the drug is a controlled substance |
| `ColdChainFlag` | `BOOLEAN` | true if the drug needs cold-chain temperature control |
| `StorageMinTempC` | `NUMERIC` | lowest safe storage temperature in celsius |
| `StorageMaxTempC` | `NUMERIC` | highest safe storage temperature in celsius |
| `MaxHumidityPct` | `NUMERIC` | maximum safe humidity percentage for storage |
| `ShelfLifeMonths` | `INT` | total shelf life in months from the manufacturing date |
| `MinimumRemainingShelfLifeDays` | `INT` | minimum days left before expiry required at delivery |
| `PrimarySupplierID` | `VARCHAR` | id of the main supplier |
| `PrimaryPlantID` | `VARCHAR` | id of the manufacturing plant |
| `ManufacturingLeadTimeDays` | `INT` | usual number of days it takes to manufacture a batch |
| `StandardBatchSizePacks` | `INT` | standard number of packs made in one production batch |
| `SafetyStockDays` | `INT` | extra buffer stock kept on hand, measured in days |
| `ReorderReviewCycleDays` | `INT` | how often inventory is checked for reordering, in days |
| `UnitManufacturingCostUSD` | `NUMERIC` | cost to make one pack (usd) |
| `WholesalePriceUSD` | `NUMERIC` | wholesale trade price per pack (usd) |
| `RecommendedRetailPriceUSD` | `NUMERIC` | recommended retail price / mrp per pack (usd) |
| `ProductStatus` | `VARCHAR` | current status of the product (active, discontinued, etc.) |
| `EffectiveFromDate` | `DATE` | date when this master record version took effect |
| `SourceSystem` | `VARCHAR` | name of the system this record came from |

---

### 2.2 `05_pharmacy_sales_data.csv` (pharmacy sales transactions, 45 columns)
*grain: one row per pharmacy sale line.*

| Column | Type | Description |
| :--- | :--- | :--- |
| `SalesTransactionID` | `VARCHAR` | unique id for each sale (primary key) |
| `SalesDate` | `DATE` | date the sale happened |
| `SalesTimestampUTC` | `TIMESTAMP` | exact utc date and time of the sale |
| `SalesChannel` | `VARCHAR` | where it was sold (in-store, e-commerce, hospital pharmacy) |
| `PharmacyID` | `VARCHAR` | unique id for the pharmacy |
| `PharmacyGLN` | `VARCHAR` | global location number for the pharmacy |
| `PharmacyName` | `VARCHAR` | name of the pharmacy |
| `PharmacyType` | `VARCHAR` | type of pharmacy (retail, chain, hospital) |
| `HospitalID` | `VARCHAR` | linked hospital id if it is a hospital pharmacy (can be null) |
| `PharmacyCity` | `VARCHAR` | city where the pharmacy is located |
| `PharmacyState` | `VARCHAR` | state or province of the pharmacy |
| `CountryCode` | `VARCHAR` | iso country code |
| `PharmacyOrderID` | `VARCHAR` | order id that restocked this medicine |
| `PharmacyDeliveryID` | `VARCHAR` | shipment id for the delivery to the pharmacy |
| `PharmacyDeliveryDate` | `DATE` | date the stock arrived at the pharmacy |
| `DeliveryQuantityPacks` | `INT` | number of packs delivered in that shipment |
| `DeliveryOTIFFlag` | `BOOLEAN` | true if the delivery arrived on time and in full |
| `WarehouseLotID` | `VARCHAR` | lot id from the warehouse |
| `SourceWarehouseID` | `VARCHAR` | id of the warehouse that sent the stock |
| `ProductionBatchID` | `VARCHAR` | manufacturing batch number |
| `MedicineID` | `VARCHAR` | links back to `01_master_data` |
| `GTIN14` | `VARCHAR` | 14-digit product barcode |
| `DiseaseCategory` | `VARCHAR` | disease group for the medicine sold |
| `GenericName` | `VARCHAR` | generic name of the medicine sold |
| `BatchExpiryDate` | `DATE` | expiry date of the batch that was sold |
| `DaysToExpiryAtSale` | `INT` | how many days were left before expiry when it was sold |
| `QuantitySoldPacks` | `INT` | number of packs sold (always 0 or more) |
| `UnitRetailPriceUSD` | `NUMERIC` | retail price charged per pack (usd) |
| `GrossSalesAmountUSD` | `NUMERIC` | total sales before discounts (`QuantitySoldPacks * UnitRetailPriceUSD`) |
| `DiscountAmountUSD` | `NUMERIC` | discount given on the sale (usd) |
| `NetSalesAmountUSD` | `NUMERIC` | final sales after discounts (`GrossSalesAmountUSD - DiscountAmountUSD`) |
| `SalesTaxUSD` | `NUMERIC` | sales tax charged (usd) |
| `CostOfGoodsSoldUSD` | `NUMERIC` | total cost of the packs sold (usd) |
| `GrossMarginUSD` | `NUMERIC` | profit made on the sale (`NetSalesAmountUSD - CostOfGoodsSoldUSD`) |
| `PromotionCode` | `VARCHAR` | promo code used, if any (can be null) |
| `PrescriptionRequiredFlag` | `BOOLEAN` | true if a prescription was needed for the sale |
| `PrescriptionID` | `VARCHAR` | prescription id if applicable (can be null) |
| `CustomerSegment` | `VARCHAR` | customer group (retail, chronic care, institutional) |
| `PayerType` | `VARCHAR` | who paid for it (out-of-pocket, insurance, government) |
| `PaymentMethod` | `VARCHAR` | how they paid (card, cash, digital) |
| `EcommerceDeliveryStatus` | `VARCHAR` | delivery status for online orders (can be null) |
| `ReturnFlag` | `BOOLEAN` | true if any packs were returned |
| `ReturnedQuantityPacks` | `INT` | number of packs returned |
| `SaleStatus` | `VARCHAR` | status of the sale (completed, returned, cancelled) |
| `SourceSystem` | `VARCHAR` | pos or e-commerce system the record came from |

---

### 2.3 `06_hospital_demand_data.csv` (hospital demand and forecast, 41 columns)
*grain: one row per hospital, medicine, and month.*

| Column | Type | Description |
| :--- | :--- | :--- |
| `HospitalDemandRecordID` | `VARCHAR` | unique id for each monthly hospital demand row (primary key) |
| `DemandMonth` | `DATE`/`VARCHAR` | month this demand and usage record covers |
| `HospitalID` | `VARCHAR` | unique id for the hospital |
| `HospitalGLN` | `VARCHAR` | global location number for the hospital |
| `HospitalName` | `VARCHAR` | name of the hospital |
| `HospitalType` | `VARCHAR` | type of hospital (tertiary, general, specialty) |
| `HospitalPharmacyID` | `VARCHAR` | id of the hospital's internal pharmacy |
| `City` | `VARCHAR` | city where the hospital is located |
| `State` | `VARCHAR` | state or province of the hospital |
| `CountryCode` | `VARCHAR` | iso country code |
| `LicensedBeds` | `INT` | total licensed beds in the hospital |
| `ICUBeds` | `INT` | total icu beds in the hospital |
| `AverageOccupancyRatePct` | `NUMERIC` | average percentage of beds occupied |
| `TraumaLevel` | `VARCHAR` | trauma center level of the hospital |
| `CatchmentPopulation` | `INT` | estimated population served by the hospital |
| `MedicineID` | `VARCHAR` | links back to `01_master_data` |
| `GTIN14` | `VARCHAR` | 14-digit product barcode |
| `GenericName` | `VARCHAR` | generic name of the medicine |
| `DiseaseCategory` | `VARCHAR` | disease category |
| `ICD10Category` | `VARCHAR` | icd-10 diagnosis category |
| `EstimatedDiseaseCases` | `INT` | estimated number of disease cases in the area/hospital |
| `DiseaseRelatedAdmissions` | `INT` | hospital admissions tied to that disease |
| `OutpatientEncounters` | `INT` | outpatient visits tied to that disease |
| `ForecastDemandPacks` | `INT` | expected monthly demand in packs |
| `RequestedQuantityPacks` | `INT` | how many packs the hospital asked for |
| `ApprovedQuantityPacks` | `INT` | how many packs were approved |
| `ReceivedQuantityPacks` | `INT` | how many packs actually arrived during the month |
| `EmergencyRequestQuantityPacks` | `INT` | extra urgent packs requested outside normal orders |
| `OpeningStockPacks` | `INT` | packs in stock at the start of the month |
| `DispensedQuantityPacks` | `INT` | packs handed out to wards or patients |
| `ConsumedQuantityPacks` | `INT` | packs actually used during the month |
| `WastageQuantityPacks` | `INT` | packs lost to expiry, damage, or spoilage |
| `ClosingStockPacks` | `INT` | packs left in stock at the end of the month |
| `UnfulfilledDemandPacks` | `INT` | demand that could not be met because of stockouts |
| `StockoutDays` | `INT` | number of days the hospital was out of stock |
| `ServiceLevelPct` | `NUMERIC` | percentage of demand met on time |
| `MedicationRequestStatus` | `VARCHAR` | status of the monthly stock request |
| `FormularyStatus` | `VARCHAR` | whether the drug is on the hospital's approved formulary list |
| `AverageLeadTimeDays` | `NUMERIC` | average days it took to get restocked |
| `DemandTrendIndex` | `NUMERIC` | index showing whether demand is trending up or down |
| `SourceSystem` | `VARCHAR` | hospital system the record came from |

---

## 3. Analytical Warehouse Layer (DuckDB Star Schema in `warehouse.duckdb`)

### 3.1 Dimension Tables

| Table | Surrogate Key | Design Pattern | Columns |
| :--- | :--- | :--- | :--- |
| **`DimDate`** | `DateKey` | shared calendar dimension (grows automatically as new refresh dates arrive) | `DateKey`, `FullDate`, `Year`, `Quarter`, `Month`, `MonthName`, `YearMonth`, `DayOfMonth`, `DayName`, `IsWeekend`, `Season` |
| **`DimMedicine`** | `MedKey` | scd type 2 (built to track cost, price, and status changes over time) | `MedKey`, `GTIN14`, `InternalSKU`, `MedName`, `TherapeuticClass`, `ICD10Category`, `DiseaseName`, `DosageFormStrength`, `Route`, `PackSize`, `PackSizeGroup`, `BaseUnitOfMeasure`, `PrescClass`, `ColdChainFlag`, `ShelfLifeMonths`, `ShelfLifeGroup`, `ProductStatus`, `StandardCost`, `ListTradePrice`, `MRP`, `ValidFrom`, `ValidTo`, `IsCurrent` |
| **`DimHospital`** | `HospitalKey` | hospital details dimension | `HospitalKey`, `HospitalID`, `HospitalGLN`, `HospitalName`, `HospitalType`, `LicensedBeds`, `ICUBeds`, `OccupancyPct` |
| **`DimPharmacy`** | `PharmacyKey` | pharmacy details dimension (links to `DimHospital` via `HospitalKey`) | `PharmacyKey`, `PharmacyID`, `PharmacyGLN`, `PharmacyName`, `PharmacyType`, `HospitalKey` |
| **`DimLocation`** | `LocationKey` | shared location dimension | `LocationKey`, `Country`, `State`, `City` |
| **`DimSalesProfile`** | `SalesProfileKey` | junk dimension grouping low-cardinality flags and channels | `SalesProfileKey`, `ChannelGroup`, `SalesChannel`, `CustomerSegment`, `DeliveryOTIFFlag` |

### 3.2 Fact Tables

| Table | Grain | Foreign Keys | Degenerate Dimension | Measures |
| :--- | :--- | :--- | :--- | :--- |
| **`FactSales`** | 1 row per pharmacy sale | `DateKey`, `MedKey`, `PharmacyKey`, `LocationKey`, `SalesProfileKey` | `SalesTransactionID` | `QuantitySoldPacks`, `UnitRetailPriceUSD`, `GrossSalesAmountUSD`, `DiscountAmountUSD`, `NetSalesAmountUSD`, `SalesTaxUSD`, `CostOfGoodsSoldUSD`, `GrossMarginUSD`, `DaysToExpiryAtSale` |
| **`FactDemand`** | 1 row per hospital, medicine, and month | `DateKey`, `MedKey`, `HospitalKey`, `LocationKey` | `DemandRecordID` (generated composite key `HospitalID\|GTIN14\|YYYYMM` to keep grain unique) | `EstimatedDiseaseCases`, `DiseaseRelatedAdmissions`, `OutpatientEncounters`, `ForecastDemandPacks`, `RequestedQuantityPacks`, `ApprovedQuantityPacks`, `ReceivedQuantityPacks`, `EmergencyRequestQuantityPacks`, `OpeningStockPacks`, `DispensedQuantityPacks`, `ConsumedQuantityPacks`, `WastageQuantityPacks`, `ClosingStockPacks`, `UnfulfilledDemandPacks`, `DemandTrendIndex` |

---

## 4. DE Observability & Query Layer

### 4.1 `load_metrics` (PostgreSQL)
tracks how each batch load performed across `initial`, `refresh1`, and `refresh2`.

| Column | Type | Description |
| :--- | :--- | :--- |
| `batch_id` | `VARCHAR` | which batch ran (`initial`, `refresh1`, `refresh2`) |
| `table_name` | `VARCHAR` | postgres staging table that was loaded |
| `source_file` | `VARCHAR` | csv file that was read |
| `mode` | `VARCHAR` | load mode used (`replace` or `append`) |
| `rows_loaded` | `INTEGER` | number of rows inserted in that run |
| `table_rows_after` | `INTEGER` | total rows in the staging table after the load finished |
| `seconds` | `NUMERIC` | how long the load took in seconds |
| `loaded_at` | `TIMESTAMP` | timestamp when the load completed |

### 4.2 `de_observability_metrics.xlsx`
pulls everything into one excel workbook for the de dashboards: batch ingestion (`load_metrics`), storage and staging (exact table row counts, postgres table sizes, and the total `warehouse.duckdb` file size of `75.51 MB`), and querying (median runtimes and error rates for `q1_forecast_vs_actual.sql`, `q2_demand_trend.sql`, and `q3_pharmacy_sales.sql`).
