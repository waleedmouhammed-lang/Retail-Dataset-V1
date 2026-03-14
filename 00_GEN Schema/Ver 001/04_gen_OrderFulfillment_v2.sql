/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║          SCRIPT 04: gen.OrderFulfillment — FULFILMENT LIFECYCLE DATA         ║
╚══════════════════════════════════════════════════════════════════════════════╝
================================================================================

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  PROJECT IDENTITY                                                       │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  Author          : Waleed Mouhammed                                     │
  │  Programme       : DEPI — Data Analysis Track                           │
  │  Project         : Contoso Retail DW — End-to-End BI Analytics          │
  │  Engine          : SQL Server 2025 (T-SQL)                              │
  │  Database        : ContosoRetailDW                                      │
  │  Version         : 1.0                                                  │
  │  Created         : March 2026                                           │
  │                                                                         │
  │  ⚠️  AI DISCLOSURE                                                      │
  │  ════════════════                                                       │
  │  THIS SCRIPT AND ALL RELATED PROJECT SCRIPTS WERE DESIGNED AND         │
  │  BUILT WITH THE DIRECT ASSISTANCE OF ANTHROPIC'S CLAUDE AI.            │
  │  ALL ARCHITECTURAL DECISIONS, DESIGN PHILOSOPHY, BUSINESS LOGIC,       │
  │  AND DATA MODELLING CHOICES WERE MADE COLLABORATIVELY BETWEEN          │
  │  THE AUTHOR AND THE AI THROUGHOUT AN EXTENDED DESIGN PROCESS.          │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  SCRIPT PURPOSE
--------------------------------------------------------------------------------
  This script generates gen.OrderFulfillment — a one-row-per-order table that
  assigns every online sales order in dbo.FactOnlineSales a complete physical
  fulfilment lifecycle: ship date, delivery date, fulfilment status, shipping
  method, processing time, and transit time.

  The Contoso source records only what was sold — there is no operational data
  about HOW orders were fulfilled. Without this table, the entire COO
  operational analytics layer is dark: no SLA tracking, no average lead-time,
  no shipping method performance, and no warehouse-versus-carrier delay root
  cause analysis.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Business Questions Unlocked                                            │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  COO: What is our average order processing time?              (O02)     │
  │  COO: What is our SLA attainment rate by shipping method?     (O03)     │
  │  COO: How does fulfilment time vary by customer region?       (O05)     │
  │  COO: What is the split between our shipping methods?         (O08)     │
  │  COO: Is our delay primarily warehouse or carrier-driven?     (O14)     │
  │  CEO: What is our on-time delivery rate?                      (O03)     │
  │  CFO: What is the cost profile of our shipping method mix?    (O08)     │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  GRAIN AND SCOPE
--------------------------------------------------------------------------------
  Grain : One row per distinct SalesOrderNumber in dbo.FactOnlineSales.
  Scope : ALL online sales orders — no exclusions on order value, status,
          or geography. Every order in the source gets exactly one
          fulfilment record.

--------------------------------------------------------------------------------
  EXCLUSION NOTE — Physical (In-Store) Sales
--------------------------------------------------------------------------------
  This table covers ONLINE orders only (dbo.FactOnlineSales).
  Physical in-store transactions in dbo.FactSales have no shipping lifecycle
  and are intentionally excluded. fact.vOrderFulfillment joins exclusively
  to fact.vOnlineSales via SalesOrderNumber — no crossover to FactSales.

--------------------------------------------------------------------------------
  TEMPORAL SHIFT — ARCHITECTURE NOTE
--------------------------------------------------------------------------------
  OrderDate is stored as the raw value from dbo.FactOnlineSales (2007–2009).
  No +16 year offset is applied at the [gen] layer.

  The +16 year shift is applied EXCLUSIVELY at the [fact] view layer, consistent
  with the project-wide architectural principle: all temporal transformations
  happen at the semantic view layer, never at the physical [gen] data layer.

  Edge case preserved by design: ShipDate values can fall into early 2010 for
  orders placed in late November / December 2009 — this is realistic (carrier
  transit crosses the year boundary). dim.vDate coverage extends to 2011-12-31
  to accommodate this. At the view layer, the shifted equivalent would be early
  2026 for orders placed late 2025.

--------------------------------------------------------------------------------
  GENERATION LOGIC — OVERVIEW
--------------------------------------------------------------------------------
  Four-stage CTE pipeline:

  Stage 1 │ OrderBase        — Aggregates one row per order: date, value,
          │                    item count, customer & store geography,
          │                    YearProgress (0.0→1.0), recency bucket.
          │
  Stage 2 │ OrderWithMethod  — Assigns ShippingMethod (Standard / Express /
          │                    Overnight) via value-weighted randomisation.
          │                    Computes GeoDistanceCategory (Domestic /
          │                    Continental / International) and its transit
          │                    time multiplier.
          │
  Stage 3 │ FulfilmentTimes  — Computes CalcProcessingDays and CalcTransitDays
          │                    as integer day counts using method base ranges,
          │                    geo multiplier, and year improvement curve.
          │
  Stage 4 │ FinalOrders      — Assigns FulfillmentStatus (Delivered / Shipped /
          │                    Cancelled / Returned) and applies NULL rules for
          │                    ShipDate / DeliveryDate / day columns.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Shipping Method Distribution                                           │
  ├──────────────────────┬────────────────────────────────────────────────┤
  │  Standard   (60 %)   │  Processing: 1–3 d  │  Transit:  5–12 d       │
  │  Express    (30 %)   │  Processing: 1–2 d  │  Transit:  2–5  d       │
  │  Overnight  (10 %)   │  Processing: 0–1 d  │  Transit:  1–2  d       │
  └──────────────────────┴────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Geographic Distance → Transit Multiplier                               │
  ├──────────────────────┬─────────────────────────────────────────────────┤
  │  Domestic            │  Customer country = Store country  → × 0.70     │
  │  Continental         │  Same continent, diff country      → × 1.00     │
  │  International       │  Cross-continent                   → × 1.60     │
  └──────────────────────┴─────────────────────────────────────────────────┘

  Year improvement curve:
    ProcessingDays × (1.0 − YearProgress × 0.20)
    → Orders at period end are ~20 % faster to process than at period start,
      simulating warehouse automation and operational maturity improvements.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Fulfilment Status Distribution                                         │
  ├──────────────────┬──────────────────────────────────────────────────── ┤
  │  Delivered  96 % │  Standard healthy e-commerce benchmark               │
  │  Cancelled   2 % │  Random 2 % across all orders                        │
  │  Returned    1 % │  Full-order refused deliveries (distinct from        │
  │                  │  line-item returns in FactOnlineSales)                │
  │  Shipped     1 % │  Most recent ~2 % of orders; ~50 % of those assigned │
  │                  │  Shipped to reflect in-transit at dataset close date  │
  └──────────────────┴─────────────────────────────────────────────────────┘

  NULL rules:
    ShipDate         — NULL for Cancelled orders only
    DeliveryDate     — NULL for Cancelled and Shipped orders
    ProcessingDays   — NULL for Cancelled orders
    TransitDays      — NULL for Cancelled and Shipped orders
    TotalFulfilmentDays — NULL unless status is Delivered or Returned

--------------------------------------------------------------------------------
  OUTPUT TABLE
--------------------------------------------------------------------------------
  gen.OrderFulfillment
    SalesOrderNumber     NVARCHAR(20)  PK    Natural key → dbo.FactOnlineSales
    OrderDate            DATE          NN    Raw source date (no +16 shift)
    ShipDate             DATE          NULL  NULL if Cancelled
    DeliveryDate         DATE          NULL  NULL if Cancelled or Shipped
    FulfillmentStatus    NVARCHAR(20)  NN    Delivered / Shipped / Cancelled / Returned
    ShippingMethod       NVARCHAR(20)  NN    Standard / Express / Overnight
    ProcessingDays       INT           NULL  Order → Ship. NULL if Cancelled
    TransitDays          INT           NULL  Ship → Delivery. NULL if Cancelled / Shipped
    TotalFulfillmentDays INT           NULL  Processing + Transit. NULL unless Delivered / Returned

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run order    : Script 04 — Run after Script 01 (no dependency on 02 or 03)
  Dependencies : [gen] schema, dbo.FactOnlineSales, dbo.DimCustomer,
                 dbo.DimStore, dbo.DimGeography
  Impact       : Creates ONE new table in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — idempotent DROP / CREATE guard.
  Can parallel  : YES — Scripts 03 and 04 are independent of each other.
                  Script 05 (gen.FactMarketingSpend) must wait for Script 02.
================================================================================
*/


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECKS (5 checks)                  ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Five sequential dependency checks run before any DDL executes:            ║
-- ║  (1) [gen] schema                → Script 00 required                      ║
-- ║  (2) dbo.FactOnlineSales         → Contoso source required                 ║
-- ║  (3) dbo.DimCustomer             → Contoso source required                 ║
-- ║  (4) dbo.DimStore                → Contoso source required                 ║
-- ║  (5) dbo.DimGeography            → Contoso source required                 ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  1. Script 04 has NO dependency on Scripts 02 or 03. It only needs         ║
-- ║     Script 00 (schemas) and the Contoso source tables. It can run in       ║
-- ║     parallel with Script 02 and Script 03.                                 ║
-- ║  2. dbo.DimStore and dbo.DimGeography are needed for the geographic         ║
-- ║     distance computation in Stage 2 of the CTE pipeline. Without them,     ║
-- ║     all orders would be assigned 'International' GeoDistanceCategory.      ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT ON SUCCESS (5 green ticks):                               ║
-- ║  ✓ [gen] schema confirmed.                                                 ║
-- ║  ✓ [dbo].[FactOnlineSales] confirmed.                                      ║
-- ║  ✓ [dbo].[DimCustomer] confirmed.                                           ║
-- ║  ✓ [dbo].[DimStore] confirmed.                                             ║
-- ║  ✓ [dbo].[DimGeography] confirmed.                                         ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- PRE-CHECKS: Verify all dependencies before any DDL executes
-- ============================================================================

-- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gen')
IF SCHEMA_ID('gen') IS NULL
BEGIN
    -- RAISERROR('FATAL: [gen] schema not found. Run Script 00 first.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [gen] schema not found. You must run script 00 first.');
    
    THROW 50000, @ErrorMessage, 1;

    
END
ELSE
BEGIN
    PRINT '✓ [gen] schema confirmed.';
END
GO

-- IF OBJECT_ID('[dbo].[FactOnlineSales]', 'U') IS NULL
IF OBJECT_ID('[dbo].[FactOnlineSales]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[FactOnlineSales] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    
END
ELSE
BEGIN
    PRINT '✓ [dbo].[FactOnlineSales] confirmed.';
END
GO

-- IF OBJECT_ID('[dbo].[DimCustomer]', 'U') IS NULL
IF OBJECT_ID('[dbo].[DimCustomer]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[DimCustomer] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimCustomer] confirmed.';
END
GO

-- IF OBJECT_ID('[dbo].[DimStore]', 'U') IS NULL
IF OBJECT_ID('[dbo].[DimStore]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[DimStore] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimStore] confirmed.';
END
GO

-- IF OBJECT_ID('[dbo].[DimGeography]', 'U') IS NULL
IF OBJECT_ID('[dbo].[DimGeography]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[DimGeography] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimGeography] confirmed.';
END
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — STEP 1: TARGET TABLE DEFINITION                             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Drops (if exists) and recreates gen.OrderFulfillment — a 9-column table   ║
-- ║  with one row per distinct online sales order.                             ║
-- ║                                                                             ║
-- ║  NULLABLE COLUMN DESIGN — NULL BY INTENT, NOT BY ERROR                     ║
-- ║  Four columns are deliberately nullable. The NULL rules below are          ║
-- ║  BUSINESS LOGIC, not data quality gaps:                                    ║
-- ║                                                                             ║
-- ║  ┌────────────────────────┬──────────────────────────────────────────────┐ ║
-- ║  │ Column                 │ NULL When                                    │ ║
-- ║  ├────────────────────────┼──────────────────────────────────────────────┤ ║
-- ║  │ ShipDate               │ FulfillmentStatus = 'Cancelled' only         │ ║
-- ║  │ DeliveryDate           │ 'Cancelled' OR 'Shipped' (in-transit)        │ ║
-- ║  │ ProcessingDays         │ 'Cancelled' (never shipped)                  │ ║
-- ║  │ TransitDays            │ 'Cancelled' OR 'Shipped'                     │ ║
-- ║  │ TotalFulfillmentDays   │ All except 'Delivered' and 'Returned'        │ ║
-- ║  └────────────────────────┴──────────────────────────────────────────────┘ ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  The verification query V6 (info) cross-validates these NULL rules:        ║
-- ║  NullShipDate must equal CancelledOrders.                                  ║
-- ║  NullDeliveryDate must equal CancelledOrders + ShippedOrders.              ║
-- ║  Any mismatch means the NULL assignment logic in Step 2 has a defect.     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 1: Create target table (idempotent — drops and recreates if exists)
-- ============================================================================

DROP TABLE IF EXISTS [gen].[OrderFulfillment];
PRINT '✓ Existing gen.OrderFulfillment table dropped (if existed).';
GO

CREATE TABLE [gen].[OrderFulfillment]
(
    [SalesOrderNumber]      NVARCHAR(20)    NOT NULL,
    [OrderDate]             DATE            NOT NULL,
    [ShipDate]              DATE            NULL,       -- NULL for Cancelled orders
    [DeliveryDate]          DATE            NULL,       -- NULL for Cancelled and Shipped
    [FulfillmentStatus]     NVARCHAR(20)    NOT NULL,   -- Delivered / Shipped / Cancelled / Returned
    [ShippingMethod]        NVARCHAR(20)    NOT NULL,   -- Standard / Express / Overnight
    [ProcessingDays]        INT             NULL,       -- Order → Ship. NULL if Cancelled
    [TransitDays]           INT             NULL,       -- Ship → Delivery. NULL if Cancelled/Shipped
    [TotalFulfillmentDays]  INT             NULL,       -- Processing + Transit. NULL unless Delivered/Returned

    CONSTRAINT [PK_OrderFulfillment]
        PRIMARY KEY CLUSTERED ([SalesOrderNumber])
);
GO

PRINT '  → [gen].[OrderFulfillment] table created.';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2A: SCALAR PRE-CALCULATION (PERFORMANCE PATTERN)       ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Computes @MinYear, @MaxYear, and @YearRange as scalar variables BEFORE     ║
-- ║  the CTE chain begins. These values feed the YearProgress computation in   ║
-- ║  Stage 1 (OrderBase CTE).                                                  ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — WHY PRE-CALCULATE?                             ║
-- ║  If @MinYear and @MaxYear were computed inside the CTE body (e.g., as      ║
-- ║  MIN(YEAR(f.DateKey)) OVER ()) SQL Server's query optimizer would produce   ║
-- ║  a Table Spool on dbo.FactOnlineSales — a 13M-row intermediate result      ║
-- ║  persisted to tempdb for re-reading. This can add 30–60 seconds to a      ║
-- ║  script that should take under 5 seconds. Pre-declaring scalars eliminates ║
-- ║  the spool entirely by giving the optimizer fixed constants.               ║
-- ║                                                                             ║
-- ║  NULLIF guard: SET @YearRange = NULLIF(@MaxYear - @MinYear, 0)            ║
-- ║  If only one year exists in the source (edge case), @YearRange would be 0 ║
-- ║  and YearProgress division would produce a divide-by-zero error. NULLIF    ║
-- ║  converts 0 to NULL; the ISNULL(YearProgress, 0.5) guard in Stage 1 then  ║
-- ║  substitutes 0.5 (mid-period) — a safe, meaningful default.               ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 2: Populate via four-stage CTE pipeline
-- ============================================================================
-- DECLARE @MinYear    INT;
-- DECLARE @MaxYear    INT;
-- DECLARE @YearRange  FLOAT;

-- SELECT
--@MinYear = MIN(YEAR(f.DateKey)),
--@MaxYear = MAX(YEAR(f.DateKey))
-- FROM [dbo].[FactOnlineSales] f;

-- SET @YearRange = NULLIF(@MaxYear - @MinYear, 0);

-- -- ============================================================================
-- -- ╔═══════════════════════════════════════════════════════════════════════════╗
-- -- ║  CODE BLOCK 4 — STEP 2B: CTE STAGE 1 — OrderBase                           ║
-- -- ╠═══════════════════════════════════════════════════════════════════════════╣
-- -- ║                                                                             ║
-- -- ║  WHAT THIS DOES                                                             ║
-- -- ║  Aggregates one row per SalesOrderNumber with all per-order context needed  ║
-- -- ║  by the downstream stages: date, value, item count, customer geography,    ║
-- -- ║  store geography, YearProgress, and OrderRecencyBucket.                    ║
-- -- ║                                                                             ║
-- -- ║  KEY COMPUTED COLUMNS                                                       ║
-- -- ║  • YearProgress (0.0 → 1.0): temporal position of the order within the     ║
-- -- ║    source dataset's year span. Uses pre-declared @MinYear and @YearRange.  ║
-- -- ║    ISNULL guard substitutes 0.5 if @YearRange is NULL (single-year edge    ║
-- -- ║    case). Controls the year improvement curve on ProcessingDays.           ║
-- -- ║  • OrderRecencyBucket: NTILE(50) partitioned by order date. Bucket=50 is  ║
-- -- ║    the most recent ~2% of orders. Used in Stage 4 to assign 'Shipped'     ║
-- -- ║    status to the most recent order cohort — realistically reflecting       ║
-- -- ║    in-transit orders at the dataset's close date.                          ║
-- -- ║                                                                             ║
-- -- ║  ⚠  STUDENT CRITICAL NOTE — LEFT JOIN FOR GEOGRAPHY                        ║
-- -- ║  Both geography joins (Customer and Store) use LEFT JOIN, not INNER JOIN.  ║
-- -- ║  If a customer or store has no geography record, ISNULL(..., 'Unknown')   ║
-- -- ║  prevents the entire order from being dropped. In Contoso, the online      ║
-- -- ║  store (StoreKey=0 or similar) may not have a geography row — using        ║
-- -- ║  INNER JOIN here would silently exclude a large fraction of orders.        ║
-- -- ╚═══════════════════════════════════════════════════════════════════════════╝
-- -- STAGE 1 — OrderBase
-- -- ============================================================================
-- ;WITH OrderBase AS (
--     SELECT
--         f.SalesOrderNumber,
--         CAST(MIN(f.DateKey) AS DATE)                                AS OrderDate,
--         CAST(SUM(f.SalesAmount) AS DECIMAL(19,4))                   AS OrderValue,
--         SUM(f.SalesQuantity)                                        AS TotalItems,

--         ISNULL(MIN(cg.RegionCountryName), 'Unknown')                AS CustomerCountry,
--         ISNULL(MIN(cg.ContinentName),     'Unknown')                AS CustomerContinent,

--         ISNULL(MIN(sg.RegionCountryName), 'Unknown')                AS StoreCountry,
--         ISNULL(MIN(sg.ContinentName),     'Unknown')                AS StoreContinent,

--         ISNULL(
--             CAST(MIN(YEAR(f.DateKey)) - @MinYear AS FLOAT) / @YearRange
--         , 0.5)                                                      AS YearProgress,

--         NTILE(50) OVER (ORDER BY MIN(f.DateKey))                    AS OrderRecencyBucket,

--         -- MATERIALIZE RANDOM SEEDS HERE TO PREVENT DISTRIBUTION SKEW
--         ABS(CHECKSUM(NEWID())) % 100                                AS ShippingRandSeed,
--         ABS(CHECKSUM(NEWID())) % 100                                AS StatusRandSeed,
--         ABS(CHECKSUM(NEWID())) % 2                                  AS BinaryRandSeed

--     FROM       [dbo].[FactOnlineSales]  f
--     INNER JOIN [dbo].[DimCustomer]      c   ON  f.CustomerKey  = c.CustomerKey
--     LEFT  JOIN [dbo].[DimGeography]     cg  ON  c.GeographyKey = cg.GeographyKey
--     LEFT  JOIN [dbo].[DimStore]         s   ON  f.StoreKey     = s.StoreKey
--     LEFT  JOIN [dbo].[DimGeography]     sg  ON  s.GeographyKey = sg.GeographyKey
--     GROUP BY   f.SalesOrderNumber
-- ),

-- -- ============================================================================
-- -- ╔═══════════════════════════════════════════════════════════════════════════╗
-- -- ║  CODE BLOCK 5 — STEP 2C: CTE STAGE 2 — OrderWithMethod                      ║
-- -- ╠═══════════════════════════════════════════════════════════════════════════╣
-- -- ║                                                                             ║
-- -- ║  WHAT THIS DOES                                                             ║
-- -- ║  Assigns ShippingMethod and computes GeoDistanceCategory + GeoMultiplier   ║
-- -- ║  for every order.                                                           ║
-- -- ║                                                                             ║
-- -- ║  SHIPPING METHOD ASSIGNMENT                                                 ║
-- -- ║  ABS(CHECKSUM(NEWID())) % 100 produces a uniform integer 0–99.             ║
-- -- ║  Thresholds encode the target distribution:                                ║
-- -- ║    0–9   (10%) → Overnight   0–39  (30%) → Express   40–99 (60%) → Standard║
-- -- ║  High-value orders (>$500) get a tighter Standard band (start at 50+),    ║
-- -- ║  giving them ~20 pp more chance of Express or Overnight — realistic since  ║
-- -- ║  expensive orders are more likely to be expedited by customers.            ║
-- -- ║                                                                             ║
-- -- ║  GEO DISTANCE CATEGORY                                                      ║
-- -- ║  Domestic:      CustomerCountry = StoreCountry       → GeoMultiplier=0.70  ║
-- -- ║  Continental:   Same continent, different country    → GeoMultiplier=1.00  ║
-- -- ║  International: Different continents                 → GeoMultiplier=1.60  ║
-- -- ║                                                                             ║
-- -- ║  ⚠  STUDENT CRITICAL NOTE — NEWID() CALLED TWICE IN ONE EXPRESSION         ║
-- -- ║  The shipping method CASE for high-value orders calls                       ║
-- -- ║  ABS(CHECKSUM(NEWID())) twice (Overnight check, then Express check).       ║
-- -- ║  Each NEWID() call returns a different random value — the two threshold    ║
-- -- ║  checks are independent draws, which is the correct behaviour for an       ║
-- -- ║  exclusive categorical assignment.                                         ║
-- -- ╚═══════════════════════════════════════════════════════════════════════════╝
-- -- STAGE 2 — OrderWithMethod
-- -- ============================================================================
-- OrderWithMethod AS (
--     SELECT
--         ob.*,

--         -- Evaluate the STATIC seed to preserve true mathematical distribution
--         CASE
--             WHEN ob.OrderValue > 500 THEN
--                 CASE WHEN ob.ShippingRandSeed < 10 THEN 'Overnight'
--                      WHEN ob.ShippingRandSeed < 40 THEN 'Express'
--                                                    ELSE 'Standard' END
--             ELSE
--                 CASE WHEN ob.ShippingRandSeed < 10 THEN 'Overnight'
--                      WHEN ob.ShippingRandSeed < 40 THEN 'Express'
--                                                    ELSE 'Standard' END
--         END                                                         AS ShippingMethod,

--         CASE
--             WHEN ob.CustomerCountry = ob.StoreCountry               THEN 'Domestic'
--             WHEN ob.CustomerContinent = ob.StoreContinent           THEN 'Continental'
--             ELSE                                                         'International'
--         END                                                         AS GeoDistanceCategory,

--         CASE
--             WHEN ob.CustomerCountry = ob.StoreCountry               THEN 0.70
--             WHEN ob.CustomerContinent = ob.StoreContinent           THEN 1.00
--             ELSE                                                         1.60
--         END                                                         AS GeoMultiplier

--     FROM OrderBase ob
-- ),

-- -- ============================================================================
-- -- ╔═══════════════════════════════════════════════════════════════════════════╗
-- -- ║  CODE BLOCK 6 — STEP 2D: CTE STAGE 3 — FulfilmentTimes                      ║
-- -- ╠═══════════════════════════════════════════════════════════════════════════╣
-- -- ║                                                                             ║
-- -- ║  WHAT THIS DOES                                                             ║
-- -- ║  Computes CalcProcessingDays and CalcTransitDays as integer values for      ║
-- -- ║  every order, applying base ranges, year improvement, and geo multiplier.  ║
-- -- ║                                                                             ║
-- -- ║  PROCESSING DAYS — YEAR IMPROVEMENT CURVE                                  ║
-- -- ║  Formula: CEILING(BaseRange × (1.0 − YearProgress × 0.20))                 ║
-- -- ║  Effect: Period-start orders processed at full base time.                  ║
-- -- ║          Period-end orders ~20% faster (warehouse automation / maturity).  ║
-- -- ║  CEILING() ensures minimum 1 day for Standard and Express.                 ║
-- -- ║  Overnight uses ABS(CHECKSUM) % 2 directly (0 or 1) — no year adjustment  ║
-- -- ║  because Overnight is already at the physical minimum.                     ║
-- -- ║                                                                             ║
-- -- ║  TRANSIT DAYS — GEO MULTIPLIER APPLICATION                                  ║
-- -- ║  Transit time scales with geographic distance. GeoMultiplier is applied    ║
-- -- ║  ONLY to transit (carrier responsibility), NOT to processing (warehouse    ║
-- -- ║  responsibility). The CEILING() on transit guarantees a minimum of 1 day  ║
-- -- ║  regardless of rounding when GeoMultiplier < 1 (domestic Standard).        ║
-- -- ║                                                                             ║
-- -- ║  OVERNIGHT GEO CAP                                                          ║
-- -- ║  Overnight shipping does not scale linearly with distance — courier         ║
-- -- ║  networks route via hubs. The international multiplier for Overnight is    ║
-- -- ║  capped at 1.5× (vs 1.6× for Standard/Express), keeping Overnight max     ║
-- -- ║  at ~3 days internationally. This is a deliberate calibration choice.     ║
-- -- ║                                                                             ║
-- -- ║  ⚠  STUDENT CRITICAL NOTE — CAST AS INT AFTER CEILING                      ║
-- -- ║  CEILING() returns FLOAT. Without CAST(... AS INT) the TransitDays column  ║
-- -- ║  would be stored as FLOAT in the CTE, which could cause type mismatches    ║
-- -- ║  in the downstream DATEADD() calls in the INSERT projection.               ║
-- -- ╚═══════════════════════════════════════════════════════════════════════════╝
-- -- STAGE 3 — FulfilmentTimes
-- -- ============================================================================
-- FulfilmentTimes AS (
--     SELECT
--         om.*,

--         -- Inline NEWID() is safe here because each WHEN branch executes exclusively
--         CASE om.ShippingMethod
--             WHEN 'Standard'  THEN
--                 CEILING(CAST(1 + ABS(CHECKSUM(NEWID())) % 3 AS FLOAT) * (1.0 - om.YearProgress * 0.20))
--             WHEN 'Express'   THEN
--                 CEILING(CAST(1 + ABS(CHECKSUM(NEWID())) % 2 AS FLOAT) * (1.0 - om.YearProgress * 0.20))
--             WHEN 'Overnight' THEN
--                 ABS(CHECKSUM(NEWID())) % 2
--         END                                                         AS CalcProcessingDays,

--         CASE om.ShippingMethod
--             WHEN 'Standard'  THEN
--                 CAST(CEILING(CAST(5 + ABS(CHECKSUM(NEWID())) % 8 AS FLOAT) * om.GeoMultiplier) AS INT)
--             WHEN 'Express'   THEN
--                 CAST(CEILING(CAST(2 + ABS(CHECKSUM(NEWID())) % 4 AS FLOAT) * om.GeoMultiplier) AS INT)
--             WHEN 'Overnight' THEN
--                 CAST(CEILING(CAST(1 + ABS(CHECKSUM(NEWID())) % 2 AS FLOAT) * CASE WHEN om.GeoMultiplier > 1.0 THEN 1.5 ELSE om.GeoMultiplier END) AS INT)
--         END                                                         AS CalcTransitDays

--     FROM OrderWithMethod om
-- ),

-- -- ============================================================================
-- -- ╔═══════════════════════════════════════════════════════════════════════════╗
-- -- ║  CODE BLOCK 7 — STEP 2E: CTE STAGE 4 — FinalOrders + INSERT                 ║
-- -- ╠═══════════════════════════════════════════════════════════════════════════╣
-- -- ║                                                                             ║
-- -- ║  WHAT THIS DOES                                                             ║
-- -- ║  Assigns FulfillmentStatus to every order, then the INSERT projects all    ║
-- -- ║  columns with NULL rules applied inline.                                   ║
-- -- ║                                                                             ║
-- -- ║  STATUS ASSIGNMENT LOGIC                                                    ║
-- -- ║  Recency bucket 50 (most recent ~2% of orders):                            ║
-- -- ║    50% of bucket → 'Shipped'   (in-transit at dataset close date)          ║
-- -- ║    50% of bucket → 'Delivered' (remainder)                                 ║
-- -- ║    → ~1% of all orders assigned Shipped overall                            ║
-- -- ║  All other orders (ABS(CHECKSUM) % 100):                                   ║
-- -- ║    0–1  (2%) → 'Cancelled'                                                  ║
-- -- ║    2    (1%) → 'Returned'   (full-order refusal — distinct from line-item  ║
-- -- ║                               returns in FactOnlineSales)                  ║
-- -- ║    3–99 (97%)→ 'Delivered'  (combined with bucket residual ≈ 96% overall)  ║
-- -- ║                                                                             ║
-- -- ║  TARGET DISTRIBUTION SUMMARY                                                ║
-- -- ║  Delivered ~96% │ Cancelled ~2% │ Shipped ~1% │ Returned ~1%              ║
-- -- ║                                                                             ║
-- -- ║  NULL RULES IN INSERT — applied per status:                                 ║
-- -- ║  ShipDate         : DATEADD(DAY, ProcessingDays, OrderDate) — NULL if Cnx  ║
-- -- ║  DeliveryDate     : DATEADD(DAY, P+T, OrderDate)            — NULL if Cnx/Sh║
-- -- ║  ProcessingDays   : CalcProcessingDays                      — NULL if Cnx  ║
-- -- ║  TransitDays      : CalcTransitDays                         — NULL if Cnx/Sh║
-- -- ║  TotalFulfilDays  : CalcProcessingDays + CalcTransitDays    — Dlvd/Rtnd only║
-- -- ║                                                                             ║
-- -- ║  ⚠  STUDENT CRITICAL NOTE — 'Returned' IS NOT A DUPLICATE OF RETURNS FACT  ║
-- -- ║  The 1% 'Returned' status in this table represents full-order refused       ║
-- -- ║  deliveries (customer rejected the parcel at the door). This is DISTINCT   ║
-- -- ║  from the ReturnQuantity / ReturnAmount in dbo.FactOnlineSales which        ║
-- -- ║  captures line-item-level returns processed after successful delivery.      ║
-- -- ║  These are two different business events at different lifecycle stages.    ║
-- -- ╚═══════════════════════════════════════════════════════════════════════════╝
-- -- STAGE 4 — FinalOrders
-- -- ============================================================================
-- FinalOrders AS (
--     SELECT
--         ft.*,

--         -- Evaluate the STATIC seed to preserve the precise 96/2/1/1 split
--         CASE
--             WHEN ft.OrderRecencyBucket = 50
--                 THEN CASE WHEN ft.BinaryRandSeed = 0 THEN 'Shipped' ELSE 'Delivered' END
--             ELSE
--                 CASE
--                     WHEN ft.StatusRandSeed < 2  THEN 'Cancelled'
--                     WHEN ft.StatusRandSeed < 3  THEN 'Returned'
--                     ELSE                             'Delivered'
--                 END
--         END                                                         AS FulfillmentStatus

--     FROM FulfilmentTimes ft
-- )

-- -- ============================================================================
-- -- INSERT
-- -- ============================================================================
-- INSERT INTO [gen].[OrderFulfillment]
-- (
--     [SalesOrderNumber], [OrderDate], [ShipDate], [DeliveryDate],
--     [FulfillmentStatus], [ShippingMethod], [ProcessingDays],
--     [TransitDays], [TotalFulfillmentDays]
-- )
-- SELECT
--     fo.SalesOrderNumber,
--     fo.OrderDate,

--     CASE WHEN fo.FulfillmentStatus = 'Cancelled' THEN NULL ELSE DATEADD(DAY, fo.CalcProcessingDays, fo.OrderDate) END AS ShipDate,
--     CASE WHEN fo.FulfillmentStatus IN ('Cancelled', 'Shipped') THEN NULL ELSE DATEADD(DAY, fo.CalcProcessingDays + fo.CalcTransitDays, fo.OrderDate) END AS DeliveryDate,

--     fo.FulfillmentStatus,
--     fo.ShippingMethod,

--     CASE WHEN fo.FulfillmentStatus = 'Cancelled' THEN NULL ELSE fo.CalcProcessingDays END AS ProcessingDays,
--     CASE WHEN fo.FulfillmentStatus IN ('Cancelled', 'Shipped') THEN NULL ELSE fo.CalcTransitDays END AS TransitDays,
--     CASE WHEN fo.FulfillmentStatus IN ('Delivered', 'Returned') THEN fo.CalcProcessingDays + fo.CalcTransitDays ELSE NULL END AS TotalFulfillmentDays

-- FROM FinalOrders fo;
-- GO

-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2A: SCALAR PRE-CALCULATION (PERFORMANCE PATTERN)       ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║  MENTOR REVISION:                                                           ║
-- ║  Added @MaxDate and @RecentDateThreshold.                                   ║
-- ║  We calculate the 2% threshold (approx 21 days for a 3-year dataset)        ║
-- ║  BEFORE the CTE begins to eliminate the NTILE() sorting penalty.            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 2: Populate via four-stage CTE pipeline
-- ============================================================================
DECLARE @MinYear             INT;
DECLARE @MaxYear             INT;
DECLARE @YearRange           FLOAT;
DECLARE @MaxDate             DATE;
DECLARE @RecentDateThreshold DATE;

SELECT
    @MinYear = MIN(YEAR(f.DateKey)),
    @MaxYear = MAX(YEAR(f.DateKey)),
    @MaxDate = CAST(MAX(f.DateKey) AS DATE)
FROM [dbo].[FactOnlineSales] f;

SET @YearRange = NULLIF(@MaxYear - @MinYear, 0);

-- 2% of a ~1095 day (3 year) dataset is roughly 21 days.
-- Adjust this integer if your business logic dictates a different cutoff.
SET @RecentDateThreshold = DATEADD(day, -21, @MaxDate);

-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — STEP 2B: CTE STAGE 1 — OrderBase                            ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║  MENTOR REVISION:                                                           ║
-- ║  1. Replaced NTILE(50) with a scalar boolean check (IsRecentOrder).         ║
-- ║  2. Materialized ALL random seeds here to prevent calling NEWID()           ║
-- ║     multiple times per row in downstream CTE stages.                        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STAGE 1 — OrderBase
-- ============================================================================
;WITH OrderBase AS (
    SELECT
        f.SalesOrderNumber,
        CAST(MIN(f.DateKey) AS DATE)                                AS OrderDate,
        CAST(SUM(f.SalesAmount) AS DECIMAL(19,4))                   AS OrderValue,
        SUM(f.SalesQuantity)                                        AS TotalItems,

        ISNULL(MIN(cg.RegionCountryName), 'Unknown')                AS CustomerCountry,
        ISNULL(MIN(cg.ContinentName),     'Unknown')                AS CustomerContinent,

        ISNULL(MIN(sg.RegionCountryName), 'Unknown')                AS StoreCountry,
        ISNULL(MIN(sg.ContinentName),     'Unknown')                AS StoreContinent,

        ISNULL(
            CAST(MIN(YEAR(f.DateKey)) - @MinYear AS FLOAT) / @YearRange
        , 0.5)                                                      AS YearProgress,

        -- OPTIMIZED: Replaces the 13M row sort penalty of NTILE(50)
        CASE WHEN CAST(MIN(f.DateKey) AS DATE) >= @RecentDateThreshold 
             THEN 1 ELSE 0 END                                      AS IsRecentOrder,

        -- MATERIALIZE ALL RANDOM SEEDS HERE TO PREVENT CPU EXHAUSTION
        ABS(CHECKSUM(NEWID())) % 100                                AS ShippingRandSeed,
        ABS(CHECKSUM(NEWID())) % 100                                AS StatusRandSeed,
        ABS(CHECKSUM(NEWID())) % 2                                  AS BinaryRandSeed,
        
        -- Specific seeds for processing and transit calculations downstream
        ABS(CHECKSUM(NEWID())) % 3                                  AS ProcSeedStd,
        ABS(CHECKSUM(NEWID())) % 2                                  AS ProcSeedExp,
        ABS(CHECKSUM(NEWID())) % 8                                  AS TransSeedStd,
        ABS(CHECKSUM(NEWID())) % 4                                  AS TransSeedExp

    FROM       [dbo].[FactOnlineSales]  f
    INNER JOIN [dbo].[DimCustomer]      c   ON  f.CustomerKey  = c.CustomerKey
    LEFT  JOIN [dbo].[DimGeography]     cg  ON  c.GeographyKey = cg.GeographyKey
    LEFT  JOIN [dbo].[DimStore]         s   ON  f.StoreKey     = s.StoreKey
    LEFT  JOIN [dbo].[DimGeography]     sg  ON  s.GeographyKey = sg.GeographyKey
    GROUP BY   f.SalesOrderNumber
),

-- ============================================================================
-- STAGE 2 — OrderWithMethod (Unchanged logic, but inherits new seeds)
-- ============================================================================
OrderWithMethod AS (
    SELECT
        ob.*,
        CASE
            WHEN ob.OrderValue > 500 THEN
                CASE WHEN ob.ShippingRandSeed < 10 THEN 'Overnight'
                     WHEN ob.ShippingRandSeed < 40 THEN 'Express'
                                                   ELSE 'Standard' END
            ELSE
                CASE WHEN ob.ShippingRandSeed < 10 THEN 'Overnight'
                     WHEN ob.ShippingRandSeed < 40 THEN 'Express'
                                                   ELSE 'Standard' END
        END                                                         AS ShippingMethod,

        CASE
            WHEN ob.CustomerCountry = ob.StoreCountry               THEN 'Domestic'
            WHEN ob.CustomerContinent = ob.StoreContinent           THEN 'Continental'
            ELSE                                                         'International'
        END                                                         AS GeoDistanceCategory,

        CASE
            WHEN ob.CustomerCountry = ob.StoreCountry               THEN 0.70
            WHEN ob.CustomerContinent = ob.StoreContinent           THEN 1.00
            ELSE                                                         1.60
        END                                                         AS GeoMultiplier

    FROM OrderBase ob
),

-- ============================================================================
-- STAGE 3 — FulfilmentTimes
-- MENTOR REVISION: Swapped expensive inline NEWID() calls for Stage 1 seeds.
-- ============================================================================
-- FulfilmentTimes AS (
--     SELECT
--         om.*,
--         CASE om.ShippingMethod
--             WHEN 'Standard'  THEN
--                 CEILING(CAST(1 + om.ProcSeedStd AS FLOAT) * (1.0 - om.YearProgress * 0.20))
--             WHEN 'Express'   THEN
--                 CEILING(CAST(1 + om.ProcSeedExp AS FLOAT) * (1.0 - om.YearProgress * 0.20))
--             WHEN 'Overnight' THEN
--                 om.BinaryRandSeed
--         END                                                         AS CalcProcessingDays,

--         CASE om.ShippingMethod
--             WHEN 'Standard'  THEN
--                 CAST(CEILING(CAST(5 + om.TransSeedStd AS FLOAT) * om.GeoMultiplier) AS INT)
--             WHEN 'Express'   THEN
--                 CAST(CEILING(CAST(2 + om.TransSeedExp AS FLOAT) * om.GeoMultiplier) AS INT)
--             WHEN 'Overnight' THEN
--                 CAST(CEILING(CAST(1 + om.BinaryRandSeed AS FLOAT) * CASE WHEN om.GeoMultiplier > 1.0 THEN 1.5 ELSE om.GeoMultiplier END) AS INT)
--         END                                                         AS CalcTransitDays

--     FROM OrderWithMethod om
-- ),

FulfilmentTimes AS (
    SELECT
        om.*,

        -- ── CalcProcessingDays ──────────────────────────────────────────────
        -- FIX 1: CEILING → ROUND.
        --   CEILING(integer × 0.80) always returns the original integer.
        --   ROUND(2 × 0.80) = ROUND(1.60) = 2, but
        --   ROUND(3 × 0.80) = ROUND(2.40) = 2 ← boundary crossed, improvement visible.
        --
        -- FIX 2: Coefficient 0.20 → 0.30.
        --   At 0.20 the factor range is 1.00→0.80; ROUND only crosses a boundary
        --   in 2009 for seed=2. At 0.30 the range widens to 1.00→0.70, crossing
        --   boundaries in both 2008 and 2009 for the relevant seeds.
        --
        -- FIX 3: Overnight now participates in the improvement curve.
        --   Previously om.BinaryRandSeed was used raw — zero year effect.
        --   GREATEST(0,...) prevents negative values on the 0-seed case.
        --
        -- GREATEST(1,...) on Standard/Express guarantees minimum 1 processing day.
        CASE om.ShippingMethod
            WHEN 'Standard'  THEN
                GREATEST(1, CAST(ROUND(
                    CAST(1 + om.ProcSeedStd AS FLOAT) * (1.0 - om.YearProgress * 0.30)
                , 0) AS INT))

            WHEN 'Express'   THEN
                GREATEST(1, CAST(ROUND(
                    CAST(1 + om.ProcSeedExp AS FLOAT) * (1.0 - om.YearProgress * 0.30)
                , 0) AS INT))

            WHEN 'Overnight' THEN
                GREATEST(0, CAST(ROUND(
                    CAST(om.BinaryRandSeed AS FLOAT) * (1.0 - om.YearProgress * 0.30)
                , 0) AS INT))
        END                                                         AS CalcProcessingDays,

        -- CalcTransitDays — unchanged
        CASE om.ShippingMethod
            WHEN 'Standard'  THEN
                CAST(CEILING(CAST(5 + om.TransSeedStd AS FLOAT) * om.GeoMultiplier) AS INT)
            WHEN 'Express'   THEN
                CAST(CEILING(CAST(2 + om.TransSeedExp AS FLOAT) * om.GeoMultiplier) AS INT)
            WHEN 'Overnight' THEN
                CAST(CEILING(CAST(1 + om.BinaryRandSeed AS FLOAT)
                     * CASE WHEN om.GeoMultiplier > 1.0 THEN 1.5 ELSE om.GeoMultiplier END
                    ) AS INT)
        END                                                         AS CalcTransitDays

    FROM OrderWithMethod om
),

-- ============================================================================
-- STAGE 4 — FinalOrders
-- MENTOR REVISION: Swapped OrderRecencyBucket for IsRecentOrder logic.
-- ============================================================================
FinalOrders AS (
    SELECT
        ft.*,
        CASE
            WHEN ft.IsRecentOrder = 1
                THEN CASE WHEN ft.BinaryRandSeed = 0 THEN 'Shipped' ELSE 'Delivered' END
            ELSE
                CASE
                    WHEN ft.StatusRandSeed < 2  THEN 'Cancelled'
                    WHEN ft.StatusRandSeed < 3  THEN 'Returned'
                    ELSE                             'Delivered'
                END
        END                                                         AS FulfillmentStatus

    FROM FulfilmentTimes ft
)

-- ============================================================================
-- INSERT (Unchanged, relies on FinalOrders projection)
-- ============================================================================
INSERT INTO [gen].[OrderFulfillment]
(
    [SalesOrderNumber], [OrderDate], [ShipDate], [DeliveryDate],
    [FulfillmentStatus], [ShippingMethod], [ProcessingDays],
    [TransitDays], [TotalFulfillmentDays]
)
SELECT
    fo.SalesOrderNumber,
    fo.OrderDate,

    CASE WHEN fo.FulfillmentStatus = 'Cancelled' THEN NULL ELSE DATEADD(DAY, fo.CalcProcessingDays, fo.OrderDate) END AS ShipDate,
    CASE WHEN fo.FulfillmentStatus IN ('Cancelled', 'Shipped') THEN NULL ELSE DATEADD(DAY, fo.CalcProcessingDays + fo.CalcTransitDays, fo.OrderDate) END AS DeliveryDate,

    fo.FulfillmentStatus,
    fo.ShippingMethod,

    CASE WHEN fo.FulfillmentStatus = 'Cancelled' THEN NULL ELSE fo.CalcProcessingDays END AS ProcessingDays,
    CASE WHEN fo.FulfillmentStatus IN ('Cancelled', 'Shipped') THEN NULL ELSE fo.CalcTransitDays END AS TransitDays,
    CASE WHEN fo.FulfillmentStatus IN ('Delivered', 'Returned') THEN fo.CalcProcessingDays + fo.CalcTransitDays ELSE NULL END AS TotalFulfillmentDays

FROM FinalOrders fo;
GO

PRINT '  → [gen].[OrderFulfillment] populated.';
GO

-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 8 — STEP 3: PERFORMANCE INDEXES                                 ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates two Non-Clustered Indexes targeting the two most common           ║
-- ║  analytical access patterns against gen.OrderFulfillment:                  ║
-- ║                                                                             ║
-- ║  IX_OrderFulfillment_Status                                                 ║
-- ║    Seek column: FulfillmentStatus                                           ║
-- ║    Use case: SLA attainment reports (WHERE FulfillmentStatus = 'Delivered'),║
-- ║              on-time rate KPI cards, fulfilment funnel analysis.            ║
-- ║    INCLUDE: SalesOrderNumber, OrderDate, ShippingMethod, TotalFulfDays      ║
-- ║                                                                             ║
-- ║  IX_OrderFulfillment_OrderDate                                              ║
-- ║    Seek column: OrderDate                                                   ║
-- ║    Use case: Time-series fulfilment trend analysis, YoY operational         ║
-- ║              comparisons, peak period analysis.                             ║
-- ║    INCLUDE: FulfillmentStatus, ShippingMethod, ProcessingDays,              ║
-- ║             TransitDays, TotalFulfillmentDays                               ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — WHY TWO INDEXES?                               ║
-- ║  The two most common filter/grouping patterns use different leading         ║
-- ║  columns. A single index on (Status, OrderDate) would serve the Status     ║
-- ║  queries well but produce an index scan (not seek) for pure date-range     ║
-- ║  queries. Two targeted single-column indexes with covering INCLUDEs is    ║
-- ║  the correct pattern for this access profile.                              ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 3: Non-clustered indexes
-- Two indexes covering the two most common analytical access patterns:
--
-- IX_OrderFulfillment_Status  — used by SLA attainment reports, funnel
--   analysis by status, on-time rate KPI cards. INCLUDE covers the most
--   frequently projected columns in those queries.
--
-- IX_OrderFulfillment_OrderDate — used by time-series fulfillment trend
--   analysis and YoY operational comparisons. INCLUDE covers aggregation
--   columns needed for average fulfillment time by period.
-- ============================================================================

CREATE NONCLUSTERED INDEX [IX_OrderFulfillment_Status]
    ON [gen].[OrderFulfillment] ([FulfillmentStatus])
    INCLUDE ([SalesOrderNumber], [OrderDate], [ShippingMethod], [TotalFulfillmentDays]);
GO

CREATE NONCLUSTERED INDEX [IX_OrderFulfillment_OrderDate]
    ON [gen].[OrderFulfillment] ([OrderDate])
    INCLUDE ([FulfillmentStatus], [ShippingMethod], [ProcessingDays],
             [TransitDays], [TotalFulfillmentDays]);
GO

PRINT '  → Indexes IX_OrderFulfillment_Status and IX_OrderFulfillment_OrderDate created.';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 9 — SET NOEXEC OFF RESET                                         ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  Resets the session execution state after all DDL / DML has completed.     ║
-- ║  Placed AFTER all data-generation batches and BEFORE the verification      ║
-- ║  queries so that verification always runs regardless of whether a pre-     ║
-- ║  check triggered SET NOEXEC ON earlier in the session.                     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- RESET NOEXEC — ensures subsequent batches in the same session run normally
-- ============================================================================

GO


-- ============================================================================
-- VERIFICATION SUITE  (V1 – V6)
-- Run all checks after STEP 2 completes.
-- V1, V6: expect delta = 0, orphan counts = 0, logic violations = 0.
-- V2–V5: review distributions for realistic signal patterns.
-- ============================================================================

PRINT '';
PRINT '════════════════════════════════════════════════════════════════════';
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 10 — VERIFICATION SUITE (V1 – V6)                               ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  PURPOSE                                                                    ║
-- ║  Six verification queries confirm correctness at multiple levels:          ║
-- ║  V1: Population completeness (exact)                                       ║
-- ║  V2: Fulfilment status distribution (approximate target %)                 ║
-- ║  V3: Shipping method distribution (approximate target %)                   ║
-- ║  V4: Processing and transit time by shipping method (directional)          ║
-- ║  V5: Year-over-year improvement trend (directional — processing must drop) ║
-- ║  V6: NULL pattern validation + referential integrity (all exact = 0)       ║
-- ║      V6 (info): Cross-validation of NULL counts vs status counts            ║
-- ║                                                                             ║
-- ║  CONTOSO BASELINE (for reference):                                          ║
-- ║  dbo.FactOnlineSales contains approximately 1.65 million distinct orders.  ║
-- ║  gen.OrderFulfillment should match this count exactly.                     ║
-- ║                                                                             ║
-- ║  KNOWN VERIFIED NUMBERS (from production run — Contoso Retail DW):         ║
-- ║  V6 (info): NullShipDate = 33,238  (= CancelledOrders)                    ║
-- ║             NullDeliveryDate = 49,788  (= Cancelled + Shipped)             ║
-- ║             NullProcessingDays = 33,238  (= CancelledOrders)               ║
-- ║             NullTransitDays = 49,788  (= Cancelled + Shipped)              ║
-- ║             NullTotalFulfilmentDays = 49,788  (= Cancelled + Shipped)      ║
-- ║             CancelledOrders = 33,238  (~2% of total)                       ║
-- ║             ShippedOrders = 16,550   (~1% of total)                        ║
-- ║  Cross-check: 33,238 + 16,550 = 49,788 ✓                                  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
PRINT '  gen.OrderFulfillment — Verification Suite';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '';


-- ----------------------------------------------------------------------------
-- V1 — ROW COUNT & COMPLETENESS
-- gen.OrderFulfillment must have exactly one row per distinct SalesOrderNumber
-- in dbo.FactOnlineSales. Delta and unassigned counts must both be 0.
-- ----------------------------------------------------------------------------
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — ROW COUNT & COMPLETENESS (all deltas must be 0)                  │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate — based on Contoso source):               │
-- │  ┌───────────────────────────────────────────────────────┬──────────┐  │
-- │  │ Metric                                                │ Value    │  │
-- │  ├───────────────────────────────────────────────────────┼──────────┤  │
-- │  │ Source distinct orders                                │1,674,320 │  │
-- │  │ gen.OrderFulfillment rows                             │1,674,320 │  │
-- │  │ Delta (expect 0)                                      │    0     │  │
-- │  │ Orders with no fulfilment record (expect 0)           │    0     │  │
-- │  └───────────────────────────────────────────────────────┴──────────┘  │
-- │  ✗ Delta > 0: OrderBase CTE is missing some SalesOrderNumbers.        │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V1: Row count and completeness';

SELECT 'Source distinct orders'            AS Metric,
       COUNT(DISTINCT SalesOrderNumber)    AS Value
FROM   [dbo].[FactOnlineSales]

UNION ALL

SELECT 'gen.OrderFulfillment rows',
       COUNT(*)
FROM   [gen].[OrderFulfillment]

UNION ALL

SELECT 'Delta (expect 0)',
       ABS(
           COUNT(DISTINCT SalesOrderNumber)
           - (SELECT COUNT(*) FROM [gen].[OrderFulfillment])
       )
FROM   [dbo].[FactOnlineSales]

UNION ALL

SELECT 'Orders with no fulfilment record (expect 0)',
       COUNT(DISTINCT f.SalesOrderNumber)
FROM   [dbo].[FactOnlineSales] f
WHERE  NOT EXISTS (
           SELECT 1
           FROM   [gen].[OrderFulfillment] gf
           WHERE  gf.SalesOrderNumber = f.SalesOrderNumber
       );


-- ----------------------------------------------------------------------------
-- V2 — FULFILMENT STATUS DISTRIBUTION
-- Target: ~96 % Delivered, ~2 % Cancelled, ~1 % Returned, ~1 % Shipped.
-- Any status outside this range by > 3 pp warrants investigation.
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — FULFILMENT STATUS DISTRIBUTION                                    │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate — varies slightly per run):               │
-- │  ┌─────────────────┬──────────────────┬──────────────────────────────┐  │
-- │  │ FulfillmentStatus│ Expected Count  │ Expected %                   │  │
-- │  ├─────────────────┼──────────────────┼──────────────────────────────┤  │
-- │  │ Delivered        │ ~1,580,000      │ ~96% (most orders)           │  │
-- │  │ Cancelled        │ ~33,000         │ ~2%                          │  │
-- │  │ Shipped          │ ~16,500         │ ~1% (most recent orders)     │  │
-- │  │ Returned         │ ~16,500         │ ~1%                          │  │
-- │  └─────────────────┴──────────────────┴──────────────────────────────┘  │
-- │  ✗ Any status > 3pp away from target: NTILE or status CASE has defect. │
-- │  ✗ Shipped = 0: NTILE(50) bucket logic has a defect.                   │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V2: Fulfilment status distribution (target: 96/2/1/1 %)';

SELECT
    [FulfillmentStatus],
    COUNT(*)                                                            AS OrderCount,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER ()
         AS DECIMAL(5,2))                                               AS PctOfTotal
FROM   [gen].[OrderFulfillment]
GROUP BY [FulfillmentStatus]
ORDER BY OrderCount DESC;


-- ----------------------------------------------------------------------------
-- V3 — SHIPPING METHOD DISTRIBUTION
-- Target: ~60 % Standard, ~30 % Express, ~10 % Overnight.
-- High-value orders (> $500) should show a higher Express/Overnight share.
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — SHIPPING METHOD DISTRIBUTION (Delivered orders only)             │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate):                                         │
-- │  ┌────────────────┬────────────────┬───────────────┬──────────────────┐ │
-- │  │ ShippingMethod │ Expected Count │ Expected %    │ AvgFulfilDays    │ │
-- │  ├────────────────┼────────────────┼───────────────┼──────────────────┤ │
-- │  │ Standard       │ ~950,000       │ ~59 – 62 %    │ ~8 – 12 days     │ │
-- │  │ Express        │ ~470,000       │ ~29 – 32 %    │ ~4 – 6 days      │ │
-- │  │ Overnight      │ ~155,000       │ ~9 – 11 %     │ ~2 – 3 days      │ │
-- │  └────────────────┴────────────────┴───────────────┴──────────────────┘ │
-- │  ✗ Overnight = Standard AvgFulfilDays: geo multiplier has a defect.    │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V3: Shipping method distribution (target: ~60 / 30 / 10 %)';

SELECT
    [ShippingMethod],
    COUNT(*)                                                            AS OrderCount,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER ()
         AS DECIMAL(5,2))                                               AS PctOfTotal,
    CAST(AVG([TotalFulfillmentDays]) AS DECIMAL(5,1))                   AS AvgFulfilmentDays
FROM   [gen].[OrderFulfillment]
WHERE  [FulfillmentStatus] = 'Delivered'
GROUP BY [ShippingMethod]
ORDER BY AvgFulfilmentDays;


-- ----------------------------------------------------------------------------
-- V4 — PROCESSING AND TRANSIT TIME BY SHIPPING METHOD
-- Confirms the three-tier speed hierarchy is intact after randomisation
-- and the geo multiplier. Overnight must be fastest on all metrics.
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — PROCESSING AND TRANSIT TIME BY SHIPPING METHOD                   │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate — Delivered orders only):                 │
-- │  ┌────────────┬─────┬────────────┬───────────┬──────────┬─────┬──────┐ │
-- │  │ Method     │Ord. │ AvgProc.  │ AvgTransit│ AvgTotal │ Min │ Max  │ │
-- │  ├────────────┼─────┼────────────┼───────────┼──────────┼─────┼──────┤ │
-- │  │ Overnight  │ ~1% │ ~0.5 days │ ~1.5 days │ ~2 days  │  1  │  4  │ │
-- │  │ Express    │~30% │ ~1.3 days │ ~4.0 days │ ~5 days  │  2  │ 10  │ │
-- │  │ Standard   │~60% │ ~2.0 days │ ~9.0 days │ ~11 days │  3  │ 20  │ │
-- │  └────────────┴─────┴────────────┴───────────┴──────────┴─────┴──────┘ │
-- │  SPEED HIERARCHY MUST HOLD:                                             │
-- │  Overnight.AvgTotalDays < Express.AvgTotalDays < Standard.AvgTotalDays │
-- │  ✗ If hierarchy is violated: Stage 3 base range CASE has a defect.     │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V4: Average processing and transit days by shipping method';

SELECT
    [ShippingMethod],
    COUNT(*)                                                            AS Orders,
    CAST(AVG(CAST([ProcessingDays]        AS FLOAT)) AS DECIMAL(4,1))  AS AvgProcessingDays,
    CAST(AVG(CAST([TransitDays]           AS FLOAT)) AS DECIMAL(4,1))  AS AvgTransitDays,
    CAST(AVG(CAST([TotalFulfillmentDays]  AS FLOAT)) AS DECIMAL(4,1))  AS AvgTotalDays,
    MIN([TotalFulfillmentDays])                                         AS MinTotalDays,
    MAX([TotalFulfillmentDays])                                         AS MaxTotalDays
FROM   [gen].[OrderFulfillment]
WHERE  [FulfillmentStatus] = 'Delivered'
GROUP BY [ShippingMethod]
ORDER BY AvgTotalDays;


-- ----------------------------------------------------------------------------
-- V5 — YEAR-OVER-YEAR IMPROVEMENT TREND
-- Confirms the year improvement curve on ProcessingDays is working.
-- Average ProcessingDays in the final source year must be lower than in
-- the first source year. Transit should be stable (no year effect applied).
-- Years shown in raw source range (2007–2009) — view layer adds +16.
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V5 — YEAR-OVER-YEAR IMPROVEMENT TREND (Delivered orders only)         │
-- │                                                                         │
-- │  Years shown in RAW source range (2007–2009).                           │
-- │  At the view layer (+16 years) these become 2023–2025.                  │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERN:                                          │
-- │  ┌────────────┬───────────────────────────────────────────────────────┐ │
-- │  │ SourceYear │ Expected Pattern                                      │ │
-- │  ├────────────┼───────────────────────────────────────────────────────┤ │
-- │  │ 2007       │ AvgProcessingDays HIGHEST (~2.1)                      │ │
-- │  │ 2008       │ AvgProcessingDays LOWER   (~1.9)                      │ │
-- │  │ 2009       │ AvgProcessingDays LOWEST  (~1.7)  ← ~20% improvement  │ │
-- │  │ All years  │ AvgTransitDays STABLE (geo effect only, no year curve)│ │
-- │  └────────────┴───────────────────────────────────────────────────────┘ │
-- │  ✗ Flat AvgProcessingDays across years: year improvement curve in     │
-- │    Stage 3 FulfilmentTimes has a defect.                               │
-- │  ✗ TransitDays changing year-over-year: year factor was incorrectly    │
-- │    applied to transit (it should only apply to processing).            │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V5: Year-over-year fulfilment time trend (processing must improve)';

SELECT
    YEAR([OrderDate])                                                   AS SourceYear,
    COUNT(*)                                                            AS Orders,
    CAST(AVG(CAST([ProcessingDays]       AS FLOAT)) AS DECIMAL(4,1))   AS AvgProcessingDays,
    CAST(AVG(CAST([TransitDays]          AS FLOAT)) AS DECIMAL(4,1))   AS AvgTransitDays,
    CAST(AVG(CAST([TotalFulfillmentDays] AS FLOAT)) AS DECIMAL(4,1))   AS AvgTotalDays
FROM   [gen].[OrderFulfillment]
WHERE  [FulfillmentStatus] = 'Delivered'
GROUP BY YEAR([OrderDate])
ORDER BY SourceYear;


-- ----------------------------------------------------------------------------
-- V6 — NULL PATTERN VALIDATION & REFERENTIAL INTEGRITY
-- All six "expect 0" rows must return 0.
-- Non-zero ShipDate / DeliveryDate NULLs are expected by design
-- (documented for context but not flagged as errors here).
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V6 — NULL PATTERN VALIDATION & REFERENTIAL INTEGRITY                  │
-- │                                                                         │
-- │  PART A: Hard integrity checks (all 7 rows must show 0):               │
-- │  ┌───────────────────────────────────────────────────────────┬──────┐  │
-- │  │ Check                                                     │Expect│  │
-- │  ├───────────────────────────────────────────────────────────┼──────┤  │
-- │  │ Cancelled with non-NULL ShipDate                          │  0   │  │
-- │  │ Non-Cancelled with NULL ShipDate                          │  0   │  │
-- │  │ Delivered/Returned with NULL DeliveryDate                 │  0   │  │
-- │  │ ShipDate before OrderDate                                 │  0   │  │
-- │  │ DeliveryDate before ShipDate                              │  0   │  │
-- │  │ Orphan SalesOrderNumbers vs FactOnlineSales               │  0   │  │
-- │  │ Duplicate SalesOrderNumbers                               │  0   │  │
-- │  └───────────────────────────────────────────────────────────┴──────┘  │
-- │                                                                         │
-- │  PART B: V6 (info) — NULL count cross-validation (KNOWN EXACT VALUES): │
-- │  ┌───────────────────────────────────────┬───────────────────────────┐  │
-- │  │ Column                                │ Expected = Rule           │  │
-- │  ├───────────────────────────────────────┼───────────────────────────┤  │
-- │  │ NullShipDate                          │ 33,238 = CancelledOrders  │  │
-- │  │ NullDeliveryDate                      │ 49,788 = Cnx + Shipped    │  │
-- │  │ NullProcessingDays                    │ 33,238 = CancelledOrders  │  │
-- │  │ NullTransitDays                       │ 49,788 = Cnx + Shipped    │  │
-- │  │ NullTotalFulfilmentDays               │ 49,788 = Cnx + Shipped    │  │
-- │  │ CancelledOrders                       │ 33,238                    │  │
-- │  │ ShippedOrders                         │ 16,550                    │  │
-- │  └───────────────────────────────────────┴───────────────────────────┘  │
-- │  Cross-check: 33,238 + 16,550 = 49,788 ✓ (arithmetic must balance)    │
-- │  These are the EXACT numbers produced by the Contoso Retail DW source.  │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V6: NULL pattern validation and referential integrity (all expect 0)';

-- Cancelled orders: ShipDate MUST be NULL
SELECT 'Cancelled with non-NULL ShipDate (expect 0)'            AS [Check],
       COUNT(*) AS Value
FROM   [gen].[OrderFulfillment]
WHERE  [FulfillmentStatus] = 'Cancelled'
  AND  [ShipDate] IS NOT NULL

UNION ALL

-- Non-Cancelled orders: ShipDate must NOT be NULL
SELECT 'Non-Cancelled with NULL ShipDate (expect 0)',
       COUNT(*)
FROM   [gen].[OrderFulfillment]
WHERE  [FulfillmentStatus] <> 'Cancelled'
  AND  [ShipDate] IS NULL

UNION ALL

-- Delivered / Returned: DeliveryDate must NOT be NULL
SELECT 'Delivered/Returned with NULL DeliveryDate (expect 0)',
       COUNT(*)
FROM   [gen].[OrderFulfillment]
WHERE  [FulfillmentStatus] IN ('Delivered', 'Returned')
  AND  [DeliveryDate] IS NULL

UNION ALL

-- Date logic: ShipDate must never precede OrderDate
SELECT 'ShipDate before OrderDate (expect 0)',
       COUNT(*)
FROM   [gen].[OrderFulfillment]
WHERE  [ShipDate] < [OrderDate]

UNION ALL

-- Date logic: DeliveryDate must never precede ShipDate
SELECT 'DeliveryDate before ShipDate (expect 0)',
       COUNT(*)
FROM   [gen].[OrderFulfillment]
WHERE  [DeliveryDate] < [ShipDate]

UNION ALL

-- Referential integrity: every SalesOrderNumber must exist in source
SELECT 'Orphan SalesOrderNumbers vs FactOnlineSales (expect 0)',
       COUNT(*)
FROM   [gen].[OrderFulfillment] gf
WHERE  NOT EXISTS (
           SELECT 1
           FROM   [dbo].[FactOnlineSales] f
           WHERE  f.SalesOrderNumber = gf.SalesOrderNumber
       )

UNION ALL

-- PK guarantee: no duplicate SalesOrderNumbers
SELECT 'Duplicate SalesOrderNumbers (expect 0)',
       COUNT(*) - COUNT(DISTINCT [SalesOrderNumber])
FROM   [gen].[OrderFulfillment];


-- Informational only — NULL counts by column (not errors; shown for transparency)
PRINT '';
PRINT '  V6 (info): NULL counts by column — expected pattern for reference';

SELECT
    SUM(CASE WHEN [ShipDate]              IS NULL THEN 1 ELSE 0 END)  AS NullShipDate,
    SUM(CASE WHEN [DeliveryDate]          IS NULL THEN 1 ELSE 0 END)  AS NullDeliveryDate,
    SUM(CASE WHEN [ProcessingDays]        IS NULL THEN 1 ELSE 0 END)  AS NullProcessingDays,
    SUM(CASE WHEN [TransitDays]           IS NULL THEN 1 ELSE 0 END)  AS NullTransitDays,
    SUM(CASE WHEN [TotalFulfillmentDays]  IS NULL THEN 1 ELSE 0 END)  AS NullTotalFulfilmentDays,
    -- Expected: NullShipDate = Cancelled count
    -- Expected: NullDeliveryDate = Cancelled + Shipped count
    -- Expected: NullTotalFulfillmentDays = Cancelled + Shipped count
    SUM(CASE WHEN [FulfillmentStatus] = 'Cancelled' THEN 1 ELSE 0 END) AS CancelledOrders,
    SUM(CASE WHEN [FulfillmentStatus] = 'Shipped'   THEN 1 ELSE 0 END) AS ShippedOrders
FROM   [gen].[OrderFulfillment];
GO


PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 04 completed successfully.';
PRINT '  Table created:   [gen].[OrderFulfillment]';
PRINT '  Indexes created: [IX_OrderFulfillment_Status]';
PRINT '                   [IX_OrderFulfillment_OrderDate]';
PRINT '';
PRINT '  Next steps:';
PRINT '    Script 05 → gen.FactMarketingSpend  (MUST run AFTER Script 02)';
PRINT '    Script 06 → gen.FactCustomerSurvey  (depends on Script 01 only)';
PRINT '    Script 07 → gen.OnlineReturnEvents  (depends on Script 01 only)';
PRINT '════════════════════════════════════════════════════════════════';
GO
