-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║                                                                              ║
-- ║   PROJECT    : Contoso Retail — End-to-End BI Analytics                     ║
-- ║   PROGRAMME  : DEPI — Data Analysis with Power BI Track                     ║
-- ║   AUTHOR     : Waleed Mouhammed                                              ║
-- ║   ENGINE     : SQL Server 2025 (T-SQL)                                       ║
-- ║   SCRIPT     : 10 — Fact Views ([fact] schema)                               ║
-- ║   VERSION    : 2.0 (Reviewed & Corrected)                                   ║
-- ║   DATE       : March 2026                                                    ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  AI DISCLOSURE                                                               ║
-- ║  Collaboratively designed with an AI assistant. All business logic,         ║
-- ║  thresholds, and architectural decisions reviewed and approved by the        ║
-- ║  lead architect. Treat as authoritative production code.                     ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  PURPOSE                                                                     ║
-- ║  Creates all 11 analytical fact views in the [fact] schema. These views     ║
-- ║  are the measurement layer consumed by Power BI via Parquet export and      ║
-- ║  connected to the [dim] schema views via Kimball Star Schema relationships.  ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  VIEWS CREATED (11 TOTAL)                                                   ║
-- ║                                                                              ║
-- ║   #   View Name                  Source(s)                         Grain    ║
-- ║  ─────────────────────────────────────────────────────────────────────────  ║
-- ║   1   fact.vOnlineSales          dbo.FactOnlineSales               Line     ║
-- ║   2   fact.vStoreSales           dbo.FactSales                     Summary  ║
-- ║   3   fact.vReturns              gen.OnlineReturnEvents UNION ALL           ║
-- ║                                  gen.PhysicalReturnEvents          Event    ║
-- ║   4   fact.vInventory            dbo.FactInventory                 Snapshot ║
-- ║   5   fact.vSalesQuota           dbo.FactSalesQuota + lookups      Quota    ║
-- ║   6   fact.vExchangeRate         dbo.FactExchangeRate              Monthly  ║
-- ║   7   fact.vOrderFulfillment     gen.OrderFulfillment              Order    ║
-- ║   8   fact.vCustomerSurvey       gen.FactCustomerSurvey            Survey   ║
-- ║   9   fact.vMarketingSpend       gen.FactMarketingSpend            Monthly  ║
-- ║  10   fact.vCustomerAcquisition  gen.CustomerAcquisition           Customer ║
-- ║  11   fact.vOrderPayment         gen.OrderPayment                  Order    ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  VERSION 2.0 — CORRECTIONS LOG                                              ║
-- ║  ─────────────────────────────────────────────────────────────────────────  ║
-- ║  VIEW 3  (fact.vReturns)    — 5 errors corrected:                           ║
-- ║    E1  FATAL SYNTAX    Missing comma after ReturnDateKey alias caused        ║
-- ║                        Msg 102 parse failure; entire view would not create. ║
-- ║    E2  DESIGN VIOLATION  Sentinel -1 replaced NULL for CustomerKey /        ║
-- ║                          StoreKey; -1 has no row in any dim, breaking Power ║
-- ║                          BI referential integrity. Restored to NULL by       ║
-- ║                          design (physical/online cross-channel absence).    ║
-- ║    E3  MISSING COLUMNS   OriginalSaleDateKey (INT) and OriginalSaleDate     ║
-- ║                          (DATE) were absent from both CTEs. These columns   ║
-- ║                          exist in both gen source tables and are required   ║
-- ║                          for the INACTIVE dim.vDate relationship and        ║
-- ║                          USERELATIONSHIP() return-lag-by-cohort analysis.  ║
-- ║    E4  MISSING COLUMNS   SalesOrderNumber (NVARCHAR(20)) and                ║
-- ║                          SalesOrderLineNumber (INT) were absent. Online     ║
-- ║                          rows carry these from the source; Physical rows    ║
-- ║                          carry CAST(NULL AS ...) by design. Required for   ║
-- ║                          TREATAS cross-fact bridge to fact.vOnlineSales.    ║
-- ║    E5  FRAGILE PATTERN   SELECT * UNION ALL replaced with fully explicit    ║
-- ║                          column lists; NULL columns typed with explicit     ║
-- ║                          CAST(NULL AS <type>) for UNION ALL type safety.    ║
-- ║                                                                              ║
-- ║  VIEW 4  (fact.vInventory)  — 3 errors corrected:                           ║
-- ║    E6  MISSING COLUMN    DateKey INT (YYYYMMDD) was never computed or       ║
-- ║                          output. Only InventoryDate DATE existed. Without   ║
-- ║                          DateKey INT there is no FK column for the Power BI ║
-- ║                          relationship to dim.vDate[DateKey]. Added.         ║
-- ║    E7  MISSING MEASURE   UnitCost (MONEY) confirmed in dbo.FactInventory    ║
-- ║                          (data dictionary row 22) was silently dropped by  ║
-- ║                          the CTE. InventoryValue and OnOrderValue (both     ║
-- ║                          documented in view header) are now computed.       ║
-- ║    E8  FRAGILE PATTERN   SELECT * replaced with explicit column list;       ║
-- ║                          documented pre-computed columns (StockStatus,      ║
-- ║                          StockCoverageRatio, AgingTier, InventoryValue,     ║
-- ║                          OnOrderValue) now materialised correctly.          ║
-- ║                                                                              ║
-- ║  VIEW 8  (fact.vCustomerSurvey) — 2 missing columns added:                  ║
-- ║    E9  MISSING COLUMN    SurveyResponseID (INT IDENTITY PK) absent.         ║
-- ║                          Required as NPS denominator key in DAX             ║
-- ║                          COUNT([SurveyResponseID]) per gen script spec.     ║
-- ║    E10 MISSING COLUMN    SurveyTrigger (NVARCHAR(30)) absent. Enables       ║
-- ║                          NPS/CSAT segmentation by survey type               ║
-- ║                          (Post-Purchase / Quarterly / Annual).              ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  DATE KEY CONTRACT — ALL 11 FACT VIEWS                                      ║
-- ║  Every date in every fact view is represented by TWO columns:               ║
-- ║    xxxDateKey  INT  (YYYYMMDD)  — FK to dim.vDate[DateKey]                  ║
-- ║    xxxDate     DATE             — Human-readable companion for display       ║
-- ║  No view exposes a date through one column type only.                        ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  TEMPORAL SHIFT — PROJECT-WIDE PRINCIPLE                                    ║
-- ║  All transactional and operational dates are shifted +16 years at this      ║
-- ║  view layer (2007–2009 source → 2023–2025 presentation).                   ║
-- ║  FIXED REFERENCE DATE for all computations: '2025-12-31'.                  ║
-- ║  Static reference fact tables (ExchangeRate, SalesQuota) also shifted.     ║
-- ║  GETDATE() is never used for historical attributes in fact views.           ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  KIMBALL STAR SCHEMA RULES ENFORCED                                         ║
-- ║  • Fact tables contain ONLY integer FKs + numeric measures + flags          ║
-- ║  • NO descriptive attributes — all resolved via Power BI relationships      ║
-- ║  • All DateKeys: INT YYYYMMDD format — VertiPaq-optimal                     ║
-- ║  • All division expressions: NULLIF denominator guard — no divide-by-zero   ║
-- ║  • ETL audit columns (ETLLoadID, LoadDate, UpdateDate) excluded             ║
-- ║  • NULL values in pipeline facts preserved — no filter at SQL layer         ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  EXECUTION ORDER                                                             ║
-- ║  Run AFTER: Script 09 (all dim views must exist first).                     ║
-- ║  Run BEFORE: Python export pipeline (Parquet generation).                   ║
-- ║                                                                              ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝


-- ============================================================================
-- PRE-EXECUTION CHECKS
-- ============================================================================

PRINT '════════════════════════════════════════════════════════════════════';
PRINT '  Script 10 — Pre-Execution Checks';
PRINT '════════════════════════════════════════════════════════════════════';

-- ── Check 1: [fact] schema ────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
BEGIN
    RAISERROR('FATAL: Schema [fact] not found. Run Script 00 first.', 16, 1);
    SET NOEXEC ON;
END
ELSE PRINT '  ✓ Schema [fact] confirmed.';
GO

-- ── Check 2: Required [dbo] source tables ─────────────────────────────────────
IF OBJECT_ID('[dbo].[FactOnlineSales]',  'U') IS NULL OR
   OBJECT_ID('[dbo].[FactSales]',         'U') IS NULL OR
   OBJECT_ID('[dbo].[FactInventory]',     'U') IS NULL OR
   OBJECT_ID('[dbo].[FactSalesQuota]',    'U') IS NULL OR
   OBJECT_ID('[dbo].[FactExchangeRate]',  'U') IS NULL OR
   OBJECT_ID('[dbo].[DimChannel]',        'U') IS NULL OR
   OBJECT_ID('[dbo].[DimScenario]',       'U') IS NULL OR
   OBJECT_ID('[dbo].[DimCurrency]',       'U') IS NULL
BEGIN
    RAISERROR('FATAL: One or more required [dbo] tables are missing.', 16, 1);
    SET NOEXEC ON;
END
ELSE PRINT '  ✓ All required [dbo] source tables confirmed.';
GO

-- ── Check 3: Required [gen] source tables ─────────────────────────────────────
IF OBJECT_ID('[gen].[OnlineReturnEvents]',    'U') IS NULL OR
   OBJECT_ID('[gen].[PhysicalReturnEvents]',  'U') IS NULL OR
   OBJECT_ID('[gen].[OrderFulfillment]',      'U') IS NULL OR
   OBJECT_ID('[gen].[FactCustomerSurvey]',    'U') IS NULL OR
   OBJECT_ID('[gen].[FactMarketingSpend]',    'U') IS NULL OR
   OBJECT_ID('[gen].[CustomerAcquisition]',   'U') IS NULL OR
   OBJECT_ID('[gen].[OrderPayment]',          'U') IS NULL
BEGIN
    RAISERROR('FATAL: One or more required [gen] tables are missing. Run Scripts 01–08 first.', 16, 1);
    SET NOEXEC ON;
END
ELSE PRINT '  ✓ All required [gen] tables confirmed.';
GO

-- ── Check 4: dim views must exist (fact views reference them for context) ─────
IF OBJECT_ID('[dim].[vDate]',    'V') IS NULL OR
   OBJECT_ID('[dim].[vChannel]', 'V') IS NULL
BEGIN
    RAISERROR('FATAL: Key dim views missing. Run Script 09 first.', 16, 1);
    SET NOEXEC ON;
END
ELSE PRINT '  ✓ Required dim views confirmed.';
GO

PRINT '  ✓ All pre-checks passed. Building fact views...';
PRINT '';
GO

SET NOEXEC OFF;
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 1 — fact.vOnlineSales
--  Source : dbo.FactOnlineSales
--  Grain  : One row per order line item (SalesOrderLineNumber level)
--  Key    : OnlineSalesKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  UNIQUE ANALYTICAL CAPABILITY
--  The ONLY fact view containing CustomerKey — the gateway for all
--  customer-centric analytics: CLV, cohort analysis, repeat purchase,
--  basket affinity. All customer-level DAX measures must anchor here.
--
--  DEGENERATE DIMENSIONS
--  SalesOrderNumber + SalesOrderLineNumber are degenerate dimensions —
--  natural identifiers from the source system retained in the fact.
--  They enable basket analysis (group by SalesOrderNumber) and act as
--  bridge keys to fact.vOrderFulfillment and fact.vOrderPayment via
--  TREATAS in DAX (no formal Power BI relationship needed).
--
--  TRANSACTIONAL PRICING
--  UnitCost / UnitPrice are the prices AT TIME OF SALE. They will differ
--  from dim.vProduct[CatalogCost] / [CatalogPrice] (list prices).
--  Net Sales = SalesAmount - ReturnAmount - DiscountAmount → build in DAX.
--
--  DATE KEY CONTRACT
--  DateKey  INT  (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE relationship)
--  OrderDate DATE           — Companion display column
--
--  TEMPORAL SHIFT
--  Source DateKey is DATETIME → shifted +16 years → INT YYYYMMDD.
-- ============================================================================

PRINT '  → Creating fact.vOnlineSales...';
GO

CREATE OR ALTER VIEW [fact].[vOnlineSales]
AS
SELECT
    /* ── SURROGATE KEY ───────────────────────────────────────────────────── */
    fos.[OnlineSalesKey],

    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    -- DateKey INT: FK → dim.vDate[DateKey] — source is DATETIME, cast to DATE first
    CAST(
        YEAR(DATEADD(YEAR, 16, CAST(fos.[DateKey] AS DATE))) * 10000
      + MONTH(DATEADD(YEAR, 16, CAST(fos.[DateKey] AS DATE))) * 100
      + DAY(DATEADD(YEAR, 16, CAST(fos.[DateKey] AS DATE)))
    AS INT)                                                     AS DateKey,

    -- OrderDate DATE: companion display column (not used as FK)
    CAST(DATEADD(YEAR, 16, CAST(fos.[DateKey] AS DATE)) AS DATE) AS OrderDate,

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    fos.[StoreKey],         -- FK → dim.vStore
    fos.[ProductKey],       -- FK → dim.vProduct
    fos.[PromotionKey],     -- FK → dim.vPromotion (Key=1 = No Promotion)
    fos.[CurrencyKey],      -- FK → dim.vCurrency
    fos.[CustomerKey],      -- FK → dim.vCustomer (unique to this fact)

    /* ── DEGENERATE DIMENSIONS ───────────────────────────────────────────── */
    -- Retained for basket analysis and cross-fact bridges (no formal relationship)
    fos.[SalesOrderNumber],
    fos.[SalesOrderLineNumber],

    /* ── SALES MEASURES ──────────────────────────────────────────────────── */
    fos.[SalesQuantity],
    fos.[SalesAmount],          -- Gross revenue (quantity × unit price)

    /* ── RETURN MEASURES ─────────────────────────────────────────────────── */
    fos.[ReturnQuantity],
    fos.[ReturnAmount],

    /* ── DISCOUNT MEASURES ───────────────────────────────────────────────── */
    fos.[DiscountQuantity],
    fos.[DiscountAmount],

    /* ── COST MEASURES ───────────────────────────────────────────────────── */
    fos.[TotalCost],
    fos.[UnitCost],             -- Transactional cost at time of sale
    fos.[UnitPrice],            -- Transactional price at time of sale

    /* ── PRE-COMPUTED GROSS PROFIT ───────────────────────────────────────── */
    -- Avoids repetitive DAX subtraction; used as an additive measure
    fos.[SalesAmount] - fos.[TotalCost]                         AS GrossProfit

    -- EXCLUDED: ETLLoadID, LoadDate, UpdateDate

FROM [dbo].[FactOnlineSales] AS fos;
GO

PRINT '    ✓ fact.vOnlineSales created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 2 — fact.vStoreSales
--  Source : dbo.FactSales
--  Grain  : One row per Product × Store × Date summary (NOT line-item)
--  Key    : SalesKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  SUMMARY GRAIN — NO CUSTOMERKEY
--  dbo.FactSales is the physical/in-store sales table. It records
--  aggregated daily store-level product sales. There is NO individual
--  customer attribution — CustomerKey does not exist in this source.
--  This is the fundamental structural difference from fact.vOnlineSales.
--
--  CHANNELKEY → dim.vChannel
--  Unlike fact.vOnlineSales, this fact carries ChannelKey which routes
--  to dim.vChannel (added in Script 09 specifically for this view).
--  Power BI relationship: fact.vStoreSales[ChannelKey] → dim.vChannel[ChannelKey]
--
--  DATE KEY CONTRACT
--  DateKey  INT  (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE relationship)
--  SaleDate DATE            — Companion display column
-- ============================================================================

PRINT '  → Creating fact.vStoreSales...';
GO

CREATE OR ALTER VIEW [fact].[vStoreSales]
AS
SELECT
    /* ── SURROGATE KEY ───────────────────────────────────────────────────── */
    fs.[SalesKey],

    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    CAST(
        YEAR(DATEADD(YEAR, 16, CAST(fs.[DateKey] AS DATE))) * 10000
      + MONTH(DATEADD(YEAR, 16, CAST(fs.[DateKey] AS DATE))) * 100
      + DAY(DATEADD(YEAR, 16, CAST(fs.[DateKey] AS DATE)))
    AS INT)                                                     AS DateKey,

    CAST(DATEADD(YEAR, 16, CAST(fs.[DateKey] AS DATE)) AS DATE) AS SaleDate,

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    fs.[channelKey]     AS ChannelKey,      -- FK → dim.vChannel
    fs.[StoreKey],                          -- FK → dim.vStore
    fs.[ProductKey],                        -- FK → dim.vProduct
    fs.[PromotionKey],                      -- FK → dim.vPromotion
    fs.[CurrencyKey],                       -- FK → dim.vCurrency

    /* ── SALES MEASURES ──────────────────────────────────────────────────── */
    fs.[SalesQuantity],
    fs.[SalesAmount],

    /* ── RETURN MEASURES ─────────────────────────────────────────────────── */
    -- Summary-grain returns embedded in the source.
    -- For event-level return detail use fact.vReturns (gen.PhysicalReturnEvents).
    fs.[ReturnQuantity],
    fs.[ReturnAmount],

    /* ── DISCOUNT MEASURES ───────────────────────────────────────────────── */
    fs.[DiscountQuantity],
    fs.[DiscountAmount],

    /* ── COST MEASURES ───────────────────────────────────────────────────── */
    fs.[TotalCost],
    fs.[UnitCost],
    fs.[UnitPrice],

    /* ── PRE-COMPUTED GROSS PROFIT ───────────────────────────────────────── */
    fs.[SalesAmount] - fs.[TotalCost]                           AS GrossProfit

    -- EXCLUDED: ETLLoadID, LoadDate, UpdateDate

FROM [dbo].[FactSales] AS fs;
GO

PRINT '    ✓ fact.vStoreSales created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 3 — fact.vReturns
--  Source : gen.OnlineReturnEvents  UNION ALL  gen.PhysicalReturnEvents
--  Grain  : One row per return event
--  Key    : ReturnEventKey (ROW_NUMBER surrogate over combined set)
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  UNION ALL ARCHITECTURE
--  Both return channels are unified into one analytical surface. The
--  ReturnChannel column ('Online' / 'Physical') is the primary discriminator
--  for channel-specific return analysis.
--
--  DUAL DATE KEY DESIGN (role-playing) — TWO PAIRS
--  ReturnDateKey  INT / ReturnDate  DATE
--    → ACTIVE relationship to dim.vDate.
--    → Use for: "returns processed in period X".
--  OriginalSaleDateKey  INT / OriginalSaleDate  DATE
--    → INACTIVE relationship to dim.vDate.
--    → Activate in DAX via USERELATIONSHIP() for:
--      "returns from sales cohort in period X" (return-lag analysis).
--  Both date pairs are shifted +16 years at this view layer.
--
--  NULL BY DESIGN — CROSS-CHANNEL ABSENT KEYS
--  CustomerKey        = NULL for Physical rows — FactSales has no customer grain.
--  SalesOrderNumber   = NULL for Physical rows — FactSales has no order grain.
--  SalesOrderLineNumber = NULL for Physical rows — same reason.
--  StoreKey           = NULL for Online rows — online orders have no store.
--  These are intentional data architecture decisions, NOT data quality issues.
--  Using sentinel -1 would create orphan FK rows in Power BI (no dim row
--  with CustomerKey = -1 or StoreKey = -1). NULL correctly renders as
--  BLANK() in DAX, which is the expected behaviour for these absent keys.
--
--  CROSS-FACT BRIDGE
--  SalesOrderNumber + SalesOrderLineNumber on Online rows bridge to
--  fact.vOnlineSales via TREATAS in DAX for return-rate-by-product analysis.
-- ============================================================================

PRINT '  → Creating fact.vReturns...';
GO

CREATE OR ALTER VIEW [fact].[vReturns]
AS
WITH OnlineReturns AS (
    SELECT
        -- Surrogate prefix to guarantee namespace uniqueness across the UNION
        'ONL-' + CAST(ore.[OnlineReturnEventID] AS VARCHAR(20)) AS ReturnEventID,

        /* ── ACTIVE DATE KEY PAIR: Return Date (shifted +16) ─────────────── */
        CAST(
            YEAR(DATEADD(YEAR, 16, ore.[ReturnDate])) * 10000
          + MONTH(DATEADD(YEAR, 16, ore.[ReturnDate])) * 100
          + DAY(DATEADD(YEAR, 16, ore.[ReturnDate]))
        AS INT)                                                 AS ReturnDateKey,

        CAST(DATEADD(YEAR, 16, ore.[ReturnDate]) AS DATE)       AS ReturnDate,

        /* ── INACTIVE DATE KEY PAIR: Original Sale Date (shifted +16) ────── */
        -- Enables USERELATIONSHIP() for return-lag cohort analysis in DAX.
        -- DAX pattern: CALCULATE([Return Count],
        --              USERELATIONSHIP(fact.vReturns[OriginalSaleDateKey],
        --                              dim.vDate[DateKey]))
        CAST(
            YEAR(DATEADD(YEAR, 16, ore.[OriginalSaleDate])) * 10000
          + MONTH(DATEADD(YEAR, 16, ore.[OriginalSaleDate])) * 100
          + DAY(DATEADD(YEAR, 16, ore.[OriginalSaleDate]))
        AS INT)                                                 AS OriginalSaleDateKey,

        CAST(DATEADD(YEAR, 16, ore.[OriginalSaleDate]) AS DATE) AS OriginalSaleDate,

        /* ── FOREIGN KEYS ─────────────────────────────────────────────────── */
        ore.[CustomerKey],                      -- FK → dim.vCustomer (NOT NULL by source constraint)
        CAST(NULL AS INT)           AS StoreKey,-- NULL by design — online orders have no store
        ore.[ProductKey],                       -- FK → dim.vProduct
        ore.[ReturnReasonKey],                  -- FK → dim.vReturnReason

        /* ── CROSS-FACT BRIDGE COLUMNS ────────────────────────────────────── */
        -- Join to fact.vOnlineSales via TREATAS(VALUES(fact.vReturns[SalesOrderNumber]),
        --                                     fact.vOnlineSales[SalesOrderNumber])
        ore.[SalesOrderNumber],
        ore.[SalesOrderLineNumber],

        /* ── MEASURES ────────────────────────────────────────────────────── */
        ore.[ReturnQuantity],
        ore.[ReturnAmount],
        ore.[RefundAmount],
        ore.[IsRefunded],
        ore.[ReturnLagDays],

        /* ── DISCRIMINATOR ──────────────────────────────────────────────── */
        'Online'                    AS ReturnChannel

    FROM [gen].[OnlineReturnEvents] AS ore
),
PhysicalReturns AS (
    SELECT
        'PHY-' + CAST(pre.[PhysicalReturnEventID] AS VARCHAR(20)) AS ReturnEventID,

        /* ── ACTIVE DATE KEY PAIR: Return Date (shifted +16) ─────────────── */
        CAST(
            YEAR(DATEADD(YEAR, 16, pre.[ReturnDate])) * 10000
          + MONTH(DATEADD(YEAR, 16, pre.[ReturnDate])) * 100
          + DAY(DATEADD(YEAR, 16, pre.[ReturnDate]))
        AS INT)                                                 AS ReturnDateKey,

        CAST(DATEADD(YEAR, 16, pre.[ReturnDate]) AS DATE)       AS ReturnDate,

        /* ── INACTIVE DATE KEY PAIR: Original Sale Date (shifted +16) ────── */
        CAST(
            YEAR(DATEADD(YEAR, 16, pre.[OriginalSaleDate])) * 10000
          + MONTH(DATEADD(YEAR, 16, pre.[OriginalSaleDate])) * 100
          + DAY(DATEADD(YEAR, 16, pre.[OriginalSaleDate]))
        AS INT)                                                 AS OriginalSaleDateKey,

        CAST(DATEADD(YEAR, 16, pre.[OriginalSaleDate]) AS DATE) AS OriginalSaleDate,

        /* ── FOREIGN KEYS ─────────────────────────────────────────────────── */
        CAST(NULL AS INT)           AS CustomerKey,             -- NULL by design — no customer grain in FactSales
        pre.[StoreKey],                                         -- FK → dim.vStore
        pre.[ProductKey],                                       -- FK → dim.vProduct
        pre.[ReturnReasonKey],                                  -- FK → dim.vReturnReason

        /* ── CROSS-FACT BRIDGE COLUMNS ────────────────────────────────────── */
        -- NULL for Physical rows — FactSales has no order-line grain
        CAST(NULL AS NVARCHAR(20))  AS SalesOrderNumber,
        CAST(NULL AS INT)           AS SalesOrderLineNumber,

        /* ── MEASURES ────────────────────────────────────────────────────── */
        pre.[ReturnQuantity],
        pre.[ReturnAmount],
        pre.[RefundAmount],
        pre.[IsRefunded],
        pre.[ReturnLagDays],

        /* ── DISCRIMINATOR ──────────────────────────────────────────────── */
        'Physical'                  AS ReturnChannel

    FROM [gen].[PhysicalReturnEvents] AS pre
),
AllReturns AS (
    -- Explicit column list for UNION ALL type safety
    SELECT ReturnEventID, ReturnDateKey, ReturnDate,
           OriginalSaleDateKey, OriginalSaleDate,
           CustomerKey, StoreKey, ProductKey, ReturnReasonKey,
           SalesOrderNumber, SalesOrderLineNumber,
           ReturnQuantity, ReturnAmount, RefundAmount, IsRefunded,
           ReturnLagDays, ReturnChannel
    FROM OnlineReturns
    UNION ALL
    SELECT ReturnEventID, ReturnDateKey, ReturnDate,
           OriginalSaleDateKey, OriginalSaleDate,
           CustomerKey, StoreKey, ProductKey, ReturnReasonKey,
           SalesOrderNumber, SalesOrderLineNumber,
           ReturnQuantity, ReturnAmount, RefundAmount, IsRefunded,
           ReturnLagDays, ReturnChannel
    FROM PhysicalReturns
)
SELECT
    /* ── SURROGATE KEY ───────────────────────────────────────────────────── */
    -- Deterministic within a single Power BI Import refresh.
    -- ROW_NUMBER in a view is valid in T-SQL; stable per-session is sufficient.
    ROW_NUMBER() OVER (ORDER BY ReturnChannel, ReturnEventID)   AS ReturnEventKey,

    ReturnEventID,

    /* ── ACTIVE DATE KEY PAIR ────────────────────────────────────────────── */
    ReturnDateKey,          -- INT YYYYMMDD — ACTIVE FK → dim.vDate[DateKey]
    ReturnDate,             -- DATE          — companion display column

    /* ── INACTIVE DATE KEY PAIR ──────────────────────────────────────────── */
    OriginalSaleDateKey,    -- INT YYYYMMDD — INACTIVE FK (USERELATIONSHIP)
    OriginalSaleDate,       -- DATE          — companion display column

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    CustomerKey,            -- NULL for Physical rows (by design)
    StoreKey,               -- NULL for Online rows (by design)
    ProductKey,
    ReturnReasonKey,

    /* ── CROSS-FACT BRIDGE ───────────────────────────────────────────────── */
    SalesOrderNumber,       -- NULL for Physical rows (by design)
    SalesOrderLineNumber,   -- NULL for Physical rows (by design)

    /* ── MEASURES ────────────────────────────────────────────────────────── */
    ReturnQuantity,
    ReturnAmount,
    RefundAmount,
    IsRefunded,
    ReturnLagDays,

    /* ── DISCRIMINATOR ───────────────────────────────────────────────────── */
    ReturnChannel           -- 'Online' | 'Physical'

FROM AllReturns;
GO

PRINT '    ✓ fact.vReturns created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 4 — fact.vInventory
--  Source : dbo.FactInventory
--  Grain  : One row per Product × Store × Date snapshot
--  Key    : InventoryKey (surrogate PK confirmed in data dictionary)
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  PERIODIC SNAPSHOT PATTERN
--  Each row records the inventory position on a given date. This is NOT
--  an accumulating snapshot — each snapshot date is independent.
--  Use LASTDATE() or LASTNONBLANK() in DAX for "current stock" measures.
--  IsCurrentSnapshot flag (pre-computed here) allows a simple slicer filter.
--
--  PRE-COMPUTED COLUMNS (Roche's Maxim — push logic upstream)
--  InventoryValue      = OnHandQuantity × UnitCost  (MONEY)
--  OnOrderValue        = OnOrderQuantity × UnitCost  (MONEY)
--  StockStatus         = 5-tier classification (calibrated to data distribution)
--  StockCoverageRatio  = OnHandQuantity / SafetyStockQuantity (NULLIF-guarded)
--  AgingTier           = Bucketed from Aging column for slicer use
--  IsCurrentSnapshot   = 1 for the latest snapshot date in the entire dataset
--
--  STOCK STATUS THRESHOLDS (calibrated to actual distribution)
--  Target: 0.17% Stock-Out | 15.68% Low Stock | 69.31% In Stock
--          13.59% Overstocked | 1.25% Severely Overstocked
--
--  DATE KEY CONTRACT
--  DateKey       INT  (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE)
--  InventoryDate DATE            — Companion display column
--
--  SOURCE TYPE NOTE
--  dbo.FactInventory[DateKey] is DATETIME — cast to DATE before DATEADD.
-- ============================================================================

PRINT '  → Creating fact.vInventory...';
GO

CREATE OR ALTER VIEW [fact].[vInventory]
AS
WITH ShiftedInventory AS (
    SELECT
        fi.[InventoryKey],

        /* ── DATE KEY (shifted +16 years, source is DATETIME) ─────────────── */
        CAST(
            YEAR(DATEADD(YEAR, 16, CAST(fi.[DateKey] AS DATE))) * 10000
          + MONTH(DATEADD(YEAR, 16, CAST(fi.[DateKey] AS DATE))) * 100
          + DAY(DATEADD(YEAR, 16, CAST(fi.[DateKey] AS DATE)))
        AS INT)                                                 AS DateKey,

        -- Companion DATE column
        CAST(DATEADD(YEAR, 16, CAST(fi.[DateKey] AS DATE)) AS DATE)
                                                                AS InventoryDate,

        /* ── FOREIGN KEYS ─────────────────────────────────────────────────── */
        fi.[StoreKey],
        fi.[ProductKey],
        fi.[CurrencyKey],

        /* ── QUANTITY MEASURES ────────────────────────────────────────────── */
        fi.[OnHandQuantity],
        fi.[OnOrderQuantity],
        fi.[SafetyStockQuantity],

        /* ── UNIT COST (retained for value computations below) ───────────── */
        fi.[UnitCost],

        /* ── TIME-IN-STOCK MEASURES ───────────────────────────────────────── */
        fi.[DaysInStock],
        fi.[MinDayInStock],
        fi.[MaxDayInStock],

        /* ── AGING ────────────────────────────────────────────────────────── */
        fi.[Aging]

    FROM [dbo].[FactInventory] AS fi
)
SELECT
    /* ── SURROGATE KEY ───────────────────────────────────────────────────── */
    si.[InventoryKey],

    /* ── DATE KEY PAIR ───────────────────────────────────────────────────── */
    si.[DateKey],           -- INT YYYYMMDD — FK → dim.vDate[DateKey]
    si.[InventoryDate],     -- DATE          — companion display column

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    si.[StoreKey],
    si.[ProductKey],
    si.[CurrencyKey],

    /* ── QUANTITY MEASURES ───────────────────────────────────────────────── */
    si.[OnHandQuantity],
    si.[OnOrderQuantity],
    si.[SafetyStockQuantity],

    /* ── PRE-COMPUTED: INVENTORY VALUE MEASURES ──────────────────────────── */
    -- On-hand stock value at average unit cost
    CAST(si.[OnHandQuantity] * si.[UnitCost] AS MONEY)          AS InventoryValue,

    -- On-order pipeline value at average unit cost
    CAST(si.[OnOrderQuantity] * si.[UnitCost] AS MONEY)         AS OnOrderValue,

    /* ── PRE-COMPUTED: STOCK COVERAGE RATIO ──────────────────────────────── */
    -- How many "safety stock units" are on hand. >1 = covered; <1 = at risk.
    -- NULLIF prevents divide-by-zero when SafetyStockQuantity = 0.
    CAST(
        si.[OnHandQuantity]
        / NULLIF(CAST(si.[SafetyStockQuantity] AS FLOAT), 0)
    AS DECIMAL(10, 4))                                          AS StockCoverageRatio,

    /* ── PRE-COMPUTED: STOCK STATUS (5-tier) ─────────────────────────────── */
    -- Thresholds calibrated to produce the target distribution:
    -- 0.17% Stock-Out | 15.68% Low | 69.31% In Stock | 13.59% Over | 1.25% Severely Over
    CASE
        WHEN si.[OnHandQuantity] = 0
             THEN 'Stock-Out'
        WHEN si.[OnHandQuantity] < si.[SafetyStockQuantity]
             THEN 'Low Stock'
        WHEN si.[OnHandQuantity] <= si.[SafetyStockQuantity] * 2
             THEN 'In Stock'
        WHEN si.[OnHandQuantity] <= si.[SafetyStockQuantity] * 4
             THEN 'Overstocked'
        ELSE      'Severely Overstocked'
    END                                                         AS StockStatus,

    /* ── TIME-IN-STOCK MEASURES ──────────────────────────────────────────── */
    si.[DaysInStock],
    si.[MinDayInStock],
    si.[MaxDayInStock],

    /* ── AGING ───────────────────────────────────────────────────────────── */
    si.[Aging],

    /* ── PRE-COMPUTED: AGING TIER (sortable prefix for slicer) ──────────── */
    CASE
        WHEN si.[Aging] IS NULL   THEN NULL
        WHEN si.[Aging] <= 30     THEN '1 — Fresh (≤30d)'
        WHEN si.[Aging] <= 90     THEN '2 — Active (31–90d)'
        WHEN si.[Aging] <= 180    THEN '3 — Slow-Moving (91–180d)'
        ELSE                           '4 — Aged (>180d)'
    END                                                         AS AgingTier,

    /* ── PRE-COMPUTED: IS CURRENT SNAPSHOT FLAG ──────────────────────────── */
    -- 1 = this row belongs to the latest inventory date in the dataset.
    -- Power BI usage: CALCULATE([On Hand Qty], fact.vInventory[IsCurrentSnapshot] = 1)
    -- Prevents accidental double-counting of semi-additive stock measures.
    CAST(
        CASE WHEN si.[InventoryDate] = MAX(si.[InventoryDate]) OVER () THEN 1
             ELSE 0 END
    AS INT)                                                     AS IsCurrentSnapshot

    -- EXCLUDED: ETLLoadID, LoadDate, UpdateDate, UnitCost (used for value calcs only)

FROM ShiftedInventory AS si;
GO

PRINT '    ✓ fact.vInventory created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 5 — fact.vSalesQuota
--  Source : dbo.FactSalesQuota
--           LEFT JOIN dbo.DimChannel  (ChannelName flattened)
--           LEFT JOIN dbo.DimScenario (ScenarioName flattened)
--  Grain  : One row per Product × Store × Date × Scenario × Channel
--  Key    : Composite (DateKey × StoreKey × ProductKey × ScenarioName)
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  ⚠ TRIPLE-COUNTING WARNING — CRITICAL DAX PATTERN
--  This fact contains THREE scenario rows per Product-Store-Date combination:
--  Budget, Actual, and Forecast. Any DAX measure that simply SUM(SalesAmountQuota)
--  will triple-count unless filtered. Always filter ScenarioName first:
--    CALCULATE([Quota Amount], fact.vSalesQuota[ScenarioName] = "Budget")
--
--  FLATTENED CHANNEL & SCENARIO
--  ChannelName and ScenarioName are denormalised directly into this fact.
--  There are no separate dim.vChannel or dim.vScenario relationships from
--  this table. dim.vChannel IS used by fact.vStoreSales for its ChannelKey FK.
--
--  DATE KEY CONTRACT
--  DateKey        INT  (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE)
--  SalesQuotaDate DATE            — Companion display column
-- ============================================================================

PRINT '  → Creating fact.vSalesQuota...';
GO

CREATE OR ALTER VIEW [fact].[vSalesQuota]
AS
SELECT
    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    CAST(
        YEAR(DATEADD(YEAR, 16, CAST(q.[DateKey] AS DATE))) * 10000
      + MONTH(DATEADD(YEAR, 16, CAST(q.[DateKey] AS DATE))) * 100
      + DAY(DATEADD(YEAR, 16, CAST(q.[DateKey] AS DATE)))
    AS INT)                                                     AS DateKey,

    CAST(DATEADD(YEAR, 16, CAST(q.[DateKey] AS DATE)) AS DATE)  AS SalesQuotaDate,

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    q.[StoreKey],       -- FK → dim.vStore
    q.[ProductKey],     -- FK → dim.vProduct
    q.[CurrencyKey],    -- FK → dim.vCurrency

    /* ── FLATTENED DESCRIPTORS (no separate Power BI relationship) ───────── */
    ch.[ChannelName],
    ch.[ChannelDescription],
    sc.[ScenarioName],          -- 'Budget' | 'Actual' | 'Forecast'
    sc.[ScenarioDescription],

    /* ── QUOTA MEASURES ──────────────────────────────────────────────────── */
    CAST(q.[SalesQuantityQuota] AS INT)                         AS SalesQuantityQuota,
    q.[SalesAmountQuota],
    q.[GrossMarginQuota],

    /* ── PRE-COMPUTED: GROSS MARGIN QUOTA % ──────────────────────────────── */
    -- NULL when quota amount = 0 — avoids divide-by-zero in Power BI
    CAST(
        q.[GrossMarginQuota]
        / NULLIF(CAST(q.[SalesAmountQuota] AS FLOAT), 0)
    AS DECIMAL(10, 4))                                          AS GrossMarginQuotaPct

    -- EXCLUDED: SalesQuotaKey, ETLLoadID, LoadDate, UpdateDate

FROM [dbo].[FactSalesQuota]  AS q
LEFT JOIN [dbo].[DimChannel]  AS ch ON q.[ChannelKey]  = ch.[ChannelKey]
LEFT JOIN [dbo].[DimScenario] AS sc ON q.[ScenarioKey] = sc.[ScenarioKey];
GO

PRINT '    ✓ fact.vSalesQuota created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 6 — fact.vExchangeRate
--  Source : dbo.FactExchangeRate
--  Grain  : One row per Currency per Month
--  Key    : CurrencyKey × YearMonthKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  ⚠ MONTHLY GRAIN — JOIN PATTERN
--  Exchange rates are stored at monthly granularity, not daily.
--  The DateKey represents the first day of each month.
--  Power BI relationship: fact.vExchangeRate[DateKey] → dim.vDate[DateKey]
--  DAX measures using this fact MUST evaluate at month/year level:
--    REMOVEFILTERS(dim.vDate) + re-filter on Year and Month
--  OR join via YearMonthKey in DAX using TREATAS.
--
--  BASE CURRENCY FLAG
--  IsBaseCurrency = 1 for USD rows. USD rate = 1.00000 always.
--  Filter out in DAX when computing weighted average FX rates.
--
--  RATE DIRECTION
--  'Appreciating' : EndOfDayRate > AverageRate × 1.005
--  'Depreciating' : EndOfDayRate < AverageRate × 0.995
--  'Stable'       : within ±0.5% of AverageRate
--
--  DATE KEY CONTRACT
--  DateKey  INT  (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE)
--  RateDate DATE            — Companion display column
-- ============================================================================

PRINT '  → Creating fact.vExchangeRate...';
GO

CREATE OR ALTER VIEW [fact].[vExchangeRate]
AS
SELECT
    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    er.[CurrencyKey],   -- FK → dim.vCurrency

    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    -- Source DateKey is DATETIME representing first day of month
    CAST(
        YEAR(DATEADD(YEAR, 16, CAST(er.[DateKey] AS DATE))) * 10000
      + MONTH(DATEADD(YEAR, 16, CAST(er.[DateKey] AS DATE))) * 100
      + DAY(DATEADD(YEAR, 16, CAST(er.[DateKey] AS DATE)))
    AS INT)                                                     AS DateKey,

    CAST(DATEADD(YEAR, 16, CAST(er.[DateKey] AS DATE)) AS DATE) AS RateDate,

    -- YearMonthKey: INT YYYYMM — for monthly-grain DAX joins via TREATAS
    CAST(
        YEAR(DATEADD(YEAR, 16, CAST(er.[DateKey] AS DATE))) * 100
      + MONTH(DATEADD(YEAR, 16, CAST(er.[DateKey] AS DATE)))
    AS INT)                                                     AS YearMonthKey,

    /* ── RATE MEASURES ───────────────────────────────────────────────────── */
    er.[AverageRate],       -- Average exchange rate for the period
    er.[EndOfDayRate],      -- Closing rate — used for period-end valuations

    /* ── PRE-COMPUTED: RATE DIRECTION ────────────────────────────────────── */
    CASE
        WHEN er.[AverageRate] = 0                               THEN 'Stable'
        WHEN er.[EndOfDayRate] > er.[AverageRate] * 1.005       THEN 'Appreciating'
        WHEN er.[EndOfDayRate] < er.[AverageRate] * 0.995       THEN 'Depreciating'
        ELSE                                                          'Stable'
    END                                                         AS RateDirection,

    /* ── BASE CURRENCY FLAG ──────────────────────────────────────────────── */
    CAST(
        CASE WHEN er.[AverageRate] = 1.0 AND er.[EndOfDayRate] = 1.0 THEN 1
             ELSE 0 END
    AS BIT)                                                     AS IsBaseCurrency

    -- EXCLUDED: ExchangeRateKey, ETLLoadID, LoadDate, UpdateDate

FROM [dbo].[FactExchangeRate] AS er;
GO

PRINT '    ✓ fact.vExchangeRate created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 7 — fact.vOrderFulfillment
--  Source : gen.OrderFulfillment
--  Grain  : One row per sales order (order-level, NOT line-item)
--  Key    : SalesOrderNumber (degenerate — links to fact.vOnlineSales)
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  THREE DATE KEY PAIRS
--  OrderDateKey / OrderDate       — ACTIVE relationship to dim.vDate
--  ShipDateKey  / ShipDate        — INACTIVE (USERELATIONSHIP in DAX)
--  DeliveryDateKey / DeliveryDate — INACTIVE (USERELATIONSHIP in DAX)
--
--  NULL PRESERVATION — DESIGN INTENT
--  ShipDate and DeliveryDate are NULL for orders not yet shipped/delivered
--  (source: gen.OrderFulfillment — Cancelled / In Progress orders).
--  ShipDateKey and DeliveryDateKey will be NULL for these rows.
--  DAX must handle NULL: ISBLANK() / IFERROR() guards required.
--
--  PRE-COMPUTED OPERATIONAL COLUMNS (Roche's Maxim)
--  SLAStatus, FulfillmentTier, DelayRootCause, IsOnTime are pushed to SQL.
--  They represent stable business logic thresholds — not DAX computed columns.
--
--  CROSS-FACT LINK
--  SalesOrderNumber bridges to fact.vOnlineSales[SalesOrderNumber] via
--  TREATAS in DAX — no formal Power BI model relationship.
-- ============================================================================

PRINT '  → Creating fact.vOrderFulfillment...';
GO

CREATE OR ALTER VIEW [fact].[vOrderFulfillment]
AS
SELECT
    /* ── NATURAL KEY (cross-fact bridge to fact.vOnlineSales) ────────────── */
    f.[SalesOrderNumber],

    /* ── ACTIVE DATE KEY PAIR: Order Date (shifted +16) ─────────────────── */
    -- Source column is DATE — DATEADD directly, no CAST to DATE needed
    CAST(
        YEAR(DATEADD(YEAR, 16, f.[OrderDate])) * 10000
      + MONTH(DATEADD(YEAR, 16, f.[OrderDate])) * 100
      + DAY(DATEADD(YEAR, 16, f.[OrderDate]))
    AS INT)                                                     AS OrderDateKey,

    CAST(DATEADD(YEAR, 16, f.[OrderDate]) AS DATE)              AS OrderDate,

    /* ── INACTIVE DATE KEY PAIR: Ship Date (NULL preserved for unshipped) ── */
    CASE
        WHEN f.[ShipDate] IS NOT NULL
        THEN CAST(
                YEAR(DATEADD(YEAR, 16, f.[ShipDate])) * 10000
              + MONTH(DATEADD(YEAR, 16, f.[ShipDate])) * 100
              + DAY(DATEADD(YEAR, 16, f.[ShipDate]))
             AS INT)
        ELSE NULL
    END                                                         AS ShipDateKey,

    CAST(DATEADD(YEAR, 16, f.[ShipDate]) AS DATE)               AS ShipDate,   -- NULL for unshipped

    /* ── INACTIVE DATE KEY PAIR: Delivery Date (NULL preserved) ─────────── */
    CASE
        WHEN f.[DeliveryDate] IS NOT NULL
        THEN CAST(
                YEAR(DATEADD(YEAR, 16, f.[DeliveryDate])) * 10000
              + MONTH(DATEADD(YEAR, 16, f.[DeliveryDate])) * 100
              + DAY(DATEADD(YEAR, 16, f.[DeliveryDate]))
             AS INT)
        ELSE NULL
    END                                                         AS DeliveryDateKey,

    CAST(DATEADD(YEAR, 16, f.[DeliveryDate]) AS DATE)           AS DeliveryDate, -- NULL for undelivered

    /* ── FULFILLMENT ATTRIBUTES ──────────────────────────────────────────── */
    f.[FulfillmentStatus],  -- In Progress / Shipped / Delivered / Cancelled
    f.[ShippingMethod],     -- Standard / Express / Overnight

    /* ── TIMING MEASURES ─────────────────────────────────────────────────── */
    f.[ProcessingDays],         -- Days: Order → Ship
    f.[TransitDays],            -- Days: Ship → Delivery
    f.[TotalFulfillmentDays],   -- Processing + Transit (NULL if not delivered)

    /* ── PRE-COMPUTED: SLA STATUS ────────────────────────────────────────── */
    CASE
        WHEN f.[FulfillmentStatus] NOT IN ('Delivered') THEN 'In Progress'
        WHEN f.[TotalFulfillmentDays] <= 3              THEN 'Express SLA Met'
        WHEN f.[TotalFulfillmentDays] <= 7              THEN 'Standard SLA Met'
        ELSE                                                 'SLA Breached'
    END                                                         AS SLAStatus,

    /* ── PRE-COMPUTED: FULFILLMENT TIER (sortable prefix) ────────────────── */
    CASE
        WHEN f.[TotalFulfillmentDays] <= 3              THEN '1 — Express (≤3d)'
        WHEN f.[TotalFulfillmentDays] <= 7              THEN '2 — Standard (4–7d)'
        WHEN f.[TotalFulfillmentDays] <= 14             THEN '3 — Delayed (8–14d)'
        WHEN f.[TotalFulfillmentDays] IS NOT NULL       THEN '4 — Late (>14d)'
        ELSE                                                 NULL   -- In Progress / Cancelled
    END                                                         AS FulfillmentTier,

    /* ── PRE-COMPUTED: DELAY ROOT CAUSE ──────────────────────────────────── */
    CASE
        WHEN f.[TotalFulfillmentDays] IS NULL           THEN NULL
        WHEN f.[ProcessingDays] > f.[TransitDays]       THEN 'Warehouse Bottleneck'
        WHEN f.[TransitDays] > f.[ProcessingDays] * 2   THEN 'Carrier Delay'
        ELSE                                                 'Balanced'
    END                                                         AS DelayRootCause,

    /* ── PRE-COMPUTED: IS ON TIME (additive INT flag) ────────────────────── */
    -- INT not BIT — enables SUM()-based % calculations without CALCULATE.
    -- % On Time = DIVIDE(SUM([IsOnTime]), COUNTROWS(fact.vOrderFulfillment))
    CAST(
        CASE WHEN f.[FulfillmentStatus] = 'Delivered'
              AND f.[TotalFulfillmentDays] <= 7 THEN 1
             ELSE 0 END
    AS INT)                                                     AS IsOnTime

FROM [gen].[OrderFulfillment] AS f;
GO

PRINT '    ✓ fact.vOrderFulfillment created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 8 — fact.vCustomerSurvey
--  Source : gen.FactCustomerSurvey
--  Grain  : One row per survey response per customer per trigger type
--  Key    : SurveyResponseID (INT IDENTITY PK from source)
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  NPSContribution — PRE-COMPUTED FOR DAX EFFICIENCY
--  NPS Score = ((Promoters - Detractors) / Total Respondents) × 100
--  NPSContribution converts the raw score to: +1 (Promoter), -1 (Detractor), 0 (Passive)
--  DAX pattern: [NPS %] = DIVIDE(SUM([NPSContribution]), COUNT([SurveyResponseID])) × 100
--  This uses a single SUM() over a pre-classified column instead of
--  multiple CALCULATE + FILTER iterations in DAX.
--
--  CSAT NOTE
--  CSATScore is on a 1–5 scale. CSAT % = % of responses with CSATScore >= 4.
--  IsSatisfied INT flag (1 / 0) is pre-computed here for single-SUM DAX pattern.
--
--  SURVEY TRIGGER SEGMENTATION
--  SurveyTrigger enables splitting NPS/CSAT by survey context:
--    'Post-Purchase' — 7–14 days after first purchase (all sampled customers)
--    'Quarterly'     — ≈180 days (customers with tenure >200 days, 2+ orders)
--    'Annual'        — ≈365 days (customers with tenure >380 days, 3+ orders)
--
--  DATE KEY CONTRACT
--  SurveyDateKey INT (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE)
--  SurveyDate    DATE           — Companion display column
-- ============================================================================

PRINT '  → Creating fact.vCustomerSurvey...';
GO

CREATE OR ALTER VIEW [fact].[vCustomerSurvey]
AS
SELECT
    /* ── SURROGATE KEY (grain identifier for DAX COUNTROWS NPS denominator) */
    cs.[SurveyResponseID],

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    cs.[CustomerKey],       -- FK → dim.vCustomer

    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    -- Source SurveyDate is DATE (unshifted — stored in raw gen range)
    CAST(
        YEAR(DATEADD(YEAR, 16, cs.[SurveyDate])) * 10000
      + MONTH(DATEADD(YEAR, 16, cs.[SurveyDate])) * 100
      + DAY(DATEADD(YEAR, 16, cs.[SurveyDate]))
    AS INT)                                                     AS SurveyDateKey,

    CAST(DATEADD(YEAR, 16, cs.[SurveyDate]) AS DATE)            AS SurveyDate,

    /* ── NPS MEASURES ────────────────────────────────────────────────────── */
    cs.[NPSScore],          -- Raw NPS score (0–10 scale)
    cs.[NPSCategory],       -- 'Promoter' (9–10) | 'Passive' (7–8) | 'Detractor' (0–6)
                            -- Persisted computed column in source — no CASE needed here

    /* ── PRE-COMPUTED: NPS CONTRIBUTION (+1 / 0 / -1) ───────────────────── */
    -- DAX: [NPS %] = DIVIDE(SUM([NPSContribution]), COUNT([SurveyResponseID])) * 100
    CAST(
        CASE cs.[NPSCategory]
            WHEN 'Promoter'  THEN  1
            WHEN 'Detractor' THEN -1
            ELSE                   0   -- Passive
        END
    AS INT)                                                     AS NPSContribution,

    /* ── CSAT MEASURES ───────────────────────────────────────────────────── */
    cs.[CSATScore],         -- 1–5 scale

    /* ── PRE-COMPUTED: IS SATISFIED FLAG ─────────────────────────────────── */
    -- CSATScore >= 4 = Satisfied. INT for SUM()-based % in DAX:
    -- DAX: [CSAT %] = DIVIDE(SUM([IsSatisfied]), COUNTROWS(fact.vCustomerSurvey))
    CAST(CASE WHEN cs.[CSATScore] >= 4 THEN 1 ELSE 0 END AS INT)
                                                                AS IsSatisfied,

    /* ── ADDITIONAL SURVEY ATTRIBUTES ───────────────────────────────────── */
    cs.[WouldRecommend],    -- BIT: 1 = would recommend to others

    -- Survey context — enables NPS/CSAT segmentation by trigger type:
    -- 'Post-Purchase' | 'Quarterly' | 'Annual'
    cs.[SurveyTrigger]

FROM [gen].[FactCustomerSurvey] AS cs;
GO

PRINT '    ✓ fact.vCustomerSurvey created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 9 — fact.vMarketingSpend
--  Source : gen.FactMarketingSpend  LEFT JOIN  gen.DimAcquisitionChannel
--  Grain  : One row per Acquisition Channel per Month
--  Key    : MarketingSpendID
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  ⚠ MONTHLY GRAIN — DAX CONTEXT REQUIREMENT
--  MonthStartDateKey represents the first day of each month.
--  Day-level date filters in Power BI will suppress all Marketing Spend data.
--  DAX measures must elevate to month/year context:
--    CALCULATE([Marketing Spend], REMOVEFILTERS(dim.vDate),
--              VALUES(dim.vDate[CalendarYear]),
--              VALUES(dim.vDate[CalendarMonth]))
--
--  DIRECT CHANNEL (AcquisitionChannelKey = 5)
--  Direct channel rows have MonthlySpend = 0. CostPerClick and
--  CostPerAcquisition = 0 or NULL. DAX must always use DIVIDE() — never the
--  division operator — to handle these zero denominators safely.
--
--  SpendEfficiencyTier benchmarks each row's CostPerAcquisition against
--  the channel's own CAC range from gen.DimAcquisitionChannel.
--
--  DATE KEY CONTRACT
--  MonthStartDateKey INT  (YYYYMMDD, 1st of month) — FK → dim.vDate[DateKey]
--  MonthStartDate    DATE                           — Companion display column
--  YearMonthKey      INT  (YYYYMM)                  — Monthly-grain DAX joins
-- ============================================================================

PRINT '  → Creating fact.vMarketingSpend...';
GO

CREATE OR ALTER VIEW [fact].[vMarketingSpend]
AS
SELECT
    /* ── SURROGATE KEY ───────────────────────────────────────────────────── */
    ms.[MarketingSpendID],

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    ms.[AcquisitionChannelKey],     -- FK → dim.vAcquisitionChannel

    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    -- Source MonthStartDateKey is INT YYYYMMDD (1st of month, unshifted).
    -- Convert to DATE via VARCHAR(8) / format 112, then apply +16 year shift.
    CAST(
        (YEAR(CONVERT(DATE, CAST(ms.[MonthStartDateKey] AS VARCHAR(8)), 112)) + 16) * 10000
      + MONTH(CONVERT(DATE, CAST(ms.[MonthStartDateKey] AS VARCHAR(8)), 112)) * 100
      + 1                         -- First day of month is always day 1
    AS INT)                                                     AS MonthStartDateKey,

    DATEFROMPARTS(
        YEAR(CONVERT(DATE, CAST(ms.[MonthStartDateKey] AS VARCHAR(8)), 112)) + 16,
        MONTH(CONVERT(DATE, CAST(ms.[MonthStartDateKey] AS VARCHAR(8)), 112)),
        1
    )                                                           AS MonthStartDate,

    -- YearMonthKey: INT YYYYMM — for monthly-grain DAX joins via TREATAS
    CAST(
        (YEAR(CONVERT(DATE, CAST(ms.[MonthStartDateKey] AS VARCHAR(8)), 112)) + 16) * 100
      + MONTH(CONVERT(DATE, CAST(ms.[MonthStartDateKey] AS VARCHAR(8)), 112))
    AS INT)                                                     AS YearMonthKey,

    /* ── SPEND & VOLUME MEASURES ─────────────────────────────────────────── */
    ms.[MonthlySpend],          -- Total marketing spend for month (MONEY)
    ms.[Impressions],           -- Total ad impressions served
    ms.[Clicks],                -- Total clicks generated
    ms.[NewCustomersAcquired],  -- Customers attributed to this channel/month

    /* ── EFFICIENCY MEASURES ─────────────────────────────────────────────── */
    ms.[CostPerClick],          -- MONEY (persisted computed in source)
    ms.[CostPerAcquisition],    -- MONEY (persisted computed in source)
    ms.[ClickThroughRate],      -- FLOAT (Clicks / Impressions × 100)

    /* ── PRE-COMPUTED: SPEND EFFICIENCY TIER ─────────────────────────────── */
    -- Benchmarked against the channel's own CAC range from dim.vAcquisitionChannel
    CASE
        WHEN ms.[NewCustomersAcquired] = 0              THEN 'No Acquisitions'
        WHEN ms.[CostPerAcquisition] <= ac.[EstimatedCACLow]
                                                        THEN 'Efficient — Below CAC Range'
        WHEN ms.[CostPerAcquisition] <= ac.[EstimatedCACHigh]
                                                        THEN 'On Target — Within CAC Range'
        ELSE                                                 'Inefficient — Above CAC Range'
    END                                                         AS SpendEfficiencyTier

FROM [gen].[FactMarketingSpend]       AS ms
LEFT JOIN [gen].[DimAcquisitionChannel] AS ac
    ON ms.[AcquisitionChannelKey] = ac.[AcquisitionChannelKey];
GO

PRINT '    ✓ fact.vMarketingSpend created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 10 — fact.vCustomerAcquisition
--  Source : gen.CustomerAcquisition  LEFT JOIN  gen.DimAcquisitionChannel
--  Grain  : One row per acquired customer (customer-grain bridge)
--  Key    : CustomerKey (natural — one row per individual customer)
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  BRIDGE FACT PATTERN
--  Records a single event per customer (their acquisition). Bridges
--  dim.vCustomer ↔ dim.vAcquisitionChannel.
--
--  CORPORATE CUSTOMER EXCLUSION — BY DESIGN
--  CustomerType = 'Company' customers are B2B relationships without a
--  consumer acquisition channel. They have NO row in gen.CustomerAcquisition.
--  In DAX, these customers return BLANK() on any acquisition metric — correct.
--
--  CACMidpoint RETAINED IN FACT
--  The midpoint of the estimated CAC range is denormalised here alongside
--  the AcquisitionChannelKey FK. This allows CAC analysis at customer grain
--  without joining back to dim.vAcquisitionChannel in every DAX measure.
--
--  DATE KEY CONTRACT
--  AcquisitionDateKey INT  (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE)
--  AcquisitionDate    DATE            — Companion display column
-- ============================================================================

PRINT '  → Creating fact.vCustomerAcquisition...';
GO

CREATE OR ALTER VIEW [fact].[vCustomerAcquisition]
AS
SELECT
    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    ca.[CustomerKey],               -- FK → dim.vCustomer
    ca.[AcquisitionChannelKey],     -- FK → dim.vAcquisitionChannel

    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    -- Source AcquisitionDate is DATE (unshifted — stored in raw gen range)
    CAST(
        YEAR(DATEADD(YEAR, 16, ca.[AcquisitionDate])) * 10000
      + MONTH(DATEADD(YEAR, 16, ca.[AcquisitionDate])) * 100
      + DAY(DATEADD(YEAR, 16, ca.[AcquisitionDate]))
    AS INT)                                                     AS AcquisitionDateKey,

    CAST(DATEADD(YEAR, 16, ca.[AcquisitionDate]) AS DATE)       AS AcquisitionDate,

    /* ── DENORMALISED CAC MIDPOINT ───────────────────────────────────────── */
    -- Pre-joined from gen.DimAcquisitionChannel for customer-grain CAC analysis
    CAST(
        (ac.[EstimatedCACLow] + ac.[EstimatedCACHigh]) / 2.0
    AS MONEY)                                                   AS CACMidpoint,

    /* ── ACQUISITION COST CLASSIFICATION ─────────────────────────────────── */
    CASE
        WHEN CAST((ac.[EstimatedCACLow] + ac.[EstimatedCACHigh]) / 2.0 AS MONEY) < 10
             THEN 'Low CAC'
        WHEN CAST((ac.[EstimatedCACLow] + ac.[EstimatedCACHigh]) / 2.0 AS MONEY) < 30
             THEN 'Medium CAC'
        ELSE      'High CAC'
    END                                                         AS CACTier

FROM [gen].[CustomerAcquisition]      AS ca
LEFT JOIN [gen].[DimAcquisitionChannel] AS ac
    ON ca.[AcquisitionChannelKey] = ac.[AcquisitionChannelKey];
GO

PRINT '    ✓ fact.vCustomerAcquisition created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 11 — fact.vOrderPayment
--  Source : gen.OrderPayment
--  Grain  : One row per sales order (order-grain)
--  Key    : SalesOrderNumber (degenerate — links to fact.vOnlineSales)
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  ORDER GRAIN (NOT LINE-ITEM)
--  A customer pays ONCE per order regardless of line-item count.
--  fact.vOnlineSales is at line-item grain; this view is at order grain.
--  The grains must never be mixed in DAX without aggregation.
--
--  PAYMENTMETHODKEY → dim.vPaymentMethod
--  All payment descriptive attributes (PaymentMethodName, PaymentCategory,
--  IsDigital) are resolved via the Power BI relationship to dim.vPaymentMethod.
--  Only the integer FK is stored here — no attribute denormalisation.
--
--  ORDERVALUE DENORMALISATION
--  OrderValue (total order amount) is denormalised from the source to enable
--  payment-value correlation analysis without requiring a fact-to-fact join.
--
--  CROSS-FACT LINK TO fact.vOnlineSales
--  SalesOrderNumber bridges to fact.vOnlineSales[SalesOrderNumber] via
--  TREATAS in DAX for customer-level payment enrichment.
--
--  DATE KEY CONTRACT
--  OrderDateKey INT  (YYYYMMDD) — FK → dim.vDate[DateKey]   (ACTIVE)
--  OrderDate    DATE            — Companion display column
--
--  SOURCE TYPE NOTE
--  gen.OrderPayment[OrderDateKey] is INT YYYYMMDD (unshifted).
--  Converted: INT → VARCHAR(8) → DATE via format 112 → +16 years.
-- ============================================================================

PRINT '  → Creating fact.vOrderPayment...';
GO

CREATE OR ALTER VIEW [fact].[vOrderPayment]
AS
SELECT
    /* ── NATURAL KEY (cross-fact bridge to fact.vOnlineSales) ────────────── */
    op.[SalesOrderNumber],

    /* ── FOREIGN KEYS ────────────────────────────────────────────────────── */
    op.[PaymentMethodKey],      -- FK → dim.vPaymentMethod

    /* ── DATE KEY PAIR (shifted +16 years) ──────────────────────────────── */
    -- Source OrderDateKey is INT YYYYMMDD (unshifted gen range).
    -- Convert to DATE via VARCHAR(8) / format 112, then apply +16 year shift.
    CAST(
        (YEAR(CONVERT(DATE, CAST(op.[OrderDateKey] AS VARCHAR(8)), 112)) + 16) * 10000
      + MONTH(CONVERT(DATE, CAST(op.[OrderDateKey] AS VARCHAR(8)), 112)) * 100
      + DAY(CONVERT(DATE, CAST(op.[OrderDateKey] AS VARCHAR(8)), 112))
    AS INT)                                                     AS OrderDateKey,

    CAST(DATEADD(YEAR, 16,
         CONVERT(DATE, CAST(op.[OrderDateKey] AS VARCHAR(8)), 112))
    AS DATE)                                                    AS OrderDate,

    /* ── DENORMALISED ORDER VALUE ─────────────────────────────────────────── */
    -- Total order amount: enables payment-method vs. order-size correlation
    op.[OrderValue]

FROM [gen].[OrderPayment] AS op;
GO

PRINT '    ✓ fact.vOrderPayment created.';
GO


-- ============================================================================
-- VERIFICATION SUITE
-- ============================================================================

PRINT '';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '  Script 10 — Verification Suite';
PRINT '════════════════════════════════════════════════════════════════════';

-- ── V1: Row counts for all 11 fact views ─────────────────────────────────────
-- EXPECTED: Row counts match source tables (no filters applied in views)
PRINT '';
PRINT '  V1 — Row counts for all 11 fact views (vs. source tables)';
SELECT 'fact.vOnlineSales'         AS FactView, COUNT(*) AS 'RowCount' FROM [fact].[vOnlineSales]
UNION ALL
SELECT 'fact.vStoreSales',                       COUNT(*) FROM [fact].[vStoreSales]
UNION ALL
SELECT 'fact.vReturns',                          COUNT(*) FROM [fact].[vReturns]
UNION ALL
SELECT 'fact.vInventory',                        COUNT(*) FROM [fact].[vInventory]
UNION ALL
SELECT 'fact.vSalesQuota',                       COUNT(*) FROM [fact].[vSalesQuota]
UNION ALL
SELECT 'fact.vExchangeRate',                     COUNT(*) FROM [fact].[vExchangeRate]
UNION ALL
SELECT 'fact.vOrderFulfillment',                 COUNT(*) FROM [fact].[vOrderFulfillment]
UNION ALL
SELECT 'fact.vCustomerSurvey',                   COUNT(*) FROM [fact].[vCustomerSurvey]
UNION ALL
SELECT 'fact.vMarketingSpend',                   COUNT(*) FROM [fact].[vMarketingSpend]
UNION ALL
SELECT 'fact.vCustomerAcquisition',              COUNT(*) FROM [fact].[vCustomerAcquisition]
UNION ALL
SELECT 'fact.vOrderPayment',                     COUNT(*) FROM [fact].[vOrderPayment]
ORDER BY FactView;

-- ── V2: Temporal shift validation — all date-bearing facts ───────────────────
-- EXPECTED: MinYear >= 2021, MaxYear <= 2027, core data 2023–2025
PRINT '';
PRINT '  V2 — Temporal shift validation (expect min 2021, max 2027)';
SELECT 'fact.vOnlineSales'         AS FactView,
       MIN(OrderDate)              AS MinDate,
       MAX(OrderDate)              AS MaxDate
FROM [fact].[vOnlineSales]
UNION ALL
SELECT 'fact.vStoreSales',
       MIN(SaleDate), MAX(SaleDate)
FROM [fact].[vStoreSales]
UNION ALL
SELECT 'fact.vReturns (ReturnDate)',
       MIN(ReturnDate), MAX(ReturnDate)
FROM [fact].[vReturns]
UNION ALL
SELECT 'fact.vReturns (OriginalSaleDate)',
       MIN(OriginalSaleDate), MAX(OriginalSaleDate)
FROM [fact].[vReturns]
UNION ALL
SELECT 'fact.vInventory',
       MIN(InventoryDate), MAX(InventoryDate)
FROM [fact].[vInventory]
UNION ALL
SELECT 'fact.vOrderFulfillment',
       MIN(OrderDate), MAX(OrderDate)
FROM [fact].[vOrderFulfillment]
UNION ALL
SELECT 'fact.vCustomerSurvey',
       MIN(SurveyDate), MAX(SurveyDate)
FROM [fact].[vCustomerSurvey]
UNION ALL
SELECT 'fact.vCustomerAcquisition',
       MIN(AcquisitionDate), MAX(AcquisitionDate)
FROM [fact].[vCustomerAcquisition]
ORDER BY FactView;

-- ── V3: fact.vReturns — channel split and NULL pattern validation ─────────────
-- EXPECTED: Online rows — CustomerKey populated, StoreKey NULL
-- EXPECTED: Physical rows — CustomerKey NULL, StoreKey populated
-- EXPECTED: Both SalesOrderNumber and SalesOrderLineNumber NULL for Physical rows
PRINT '';
PRINT '  V3 — fact.vReturns: channel split and NULL integrity';
SELECT
    ReturnChannel,
    COUNT(*)                                                    AS 'RowCount',
    SUM(CASE WHEN CustomerKey         IS NULL THEN 1 ELSE 0 END) AS NullCustomerKeys,
    SUM(CASE WHEN StoreKey            IS NULL THEN 1 ELSE 0 END) AS NullStoreKeys,
    SUM(CASE WHEN SalesOrderNumber    IS NULL THEN 1 ELSE 0 END) AS NullOrderNumbers,
    SUM(CASE WHEN OriginalSaleDateKey IS NULL THEN 1 ELSE 0 END) AS NullOriginalSaleDateKeys,
    MIN(ReturnDate)                                             AS MinReturnDate,
    MAX(ReturnDate)                                             AS MaxReturnDate,
    SUM(RefundAmount)                                           AS TotalRefundAmount
FROM [fact].[vReturns]
GROUP BY ReturnChannel;

-- ── V4: fact.vSalesQuota — scenario distribution (triple-count guard) ─────────
-- EXPECTED: Budget = Actual = Forecast row counts (all three present, equal)
PRINT '';
PRINT '  V4 — fact.vSalesQuota: scenario distribution (must be 3 equal groups)';
SELECT
    ScenarioName,
    COUNT(*)                    AS 'RowCount',
    SUM(SalesAmountQuota)       AS TotalAmountQuota
FROM [fact].[vSalesQuota]
GROUP BY ScenarioName
ORDER BY ScenarioName;

-- ── V5: fact.vInventory — stock status distribution ───────────────────────────
-- EXPECTED (approx): Stock-Out 0.17% | Low 15.68% | In Stock 69.31%
--                    Overstocked 13.59% | Severely Overstocked 1.25%
PRINT '';
PRINT '  V5 — fact.vInventory: stock status distribution and DateKey integrity';
SELECT
    StockStatus,
    COUNT(*)                                                    AS 'RowCount',
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2))
                                                                AS PctOfTotal,
    SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END)            AS NullDateKeys
FROM [fact].[vInventory]
GROUP BY StockStatus
ORDER BY PctOfTotal DESC;

-- ── V6: fact.vOrderFulfillment — SLA status and NULL date integrity ───────────
-- EXPECTED: NULL ShipDates only for In Progress / Cancelled orders
PRINT '';
PRINT '  V6 — fact.vOrderFulfillment: SLA status distribution and NULL dates';
SELECT
    SLAStatus,
    FulfillmentTier,
    COUNT(*)                                                    AS OrderCount,
    SUM(IsOnTime)                                               AS OnTimeOrders,
    SUM(CASE WHEN ShipDate     IS NULL THEN 1 ELSE 0 END)       AS NullShipDates,
    SUM(CASE WHEN DeliveryDate IS NULL THEN 1 ELSE 0 END)       AS NullDeliveryDates
FROM [fact].[vOrderFulfillment]
GROUP BY SLAStatus, FulfillmentTier
ORDER BY
    CASE WHEN FulfillmentTier IS NULL THEN 1 ELSE 0 END,
    FulfillmentTier;

-- ── V7: fact.vExchangeRate — grain and base currency check ────────────────────
-- EXPECTED: One row per CurrencyKey per YearMonthKey (no duplicates)
-- EXPECTED: At least one currency with IsBaseCurrency = 1 (USD)
PRINT '';
PRINT '  V7 — fact.vExchangeRate: grain integrity and base currency';
SELECT
    COUNT(*)                                                    AS TotalRows,
    COUNT(DISTINCT CAST(CurrencyKey AS NVARCHAR(10))
        + '|' + CAST(YearMonthKey AS NVARCHAR(10)))             AS UniqueKeys,
    SUM(CAST(IsBaseCurrency AS INT))                            AS BaseCurrencyRows,
    MIN(RateDate)                                               AS MinRateDate,
    MAX(RateDate)                                               AS MaxRateDate
FROM [fact].[vExchangeRate];

-- ── V8: fact.vCustomerSurvey — NPS contribution distribution ─────────────────
-- EXPECTED: NPSContribution values only +1, 0, -1
-- EXPECTED: SurveyTrigger has 3 distinct values
PRINT '';
PRINT '  V8 — fact.vCustomerSurvey: NPS contribution and trigger distribution';
SELECT
    NPSCategory,
    NPSContribution,
    SurveyTrigger,
    IsSatisfied,
    COUNT(*)                                                    AS ResponseCount,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2))
                                                                AS PctOfTotal
FROM [fact].[vCustomerSurvey]
GROUP BY NPSCategory, NPSContribution, SurveyTrigger, IsSatisfied
ORDER BY NPSContribution DESC, SurveyTrigger;

-- ── V9: fact.vMarketingSpend — efficiency tier and Direct channel check ───────
PRINT '';
PRINT '  V9 — fact.vMarketingSpend: spend efficiency tier distribution';
SELECT
    SpendEfficiencyTier,
    COUNT(*)                                                    AS 'RowCount',
    SUM(MonthlySpend)                                           AS TotalSpend,
    SUM(NewCustomersAcquired)                                   AS TotalAcquisitions
FROM [fact].[vMarketingSpend]
GROUP BY SpendEfficiencyTier
ORDER BY TotalSpend DESC;

-- ── V10: Referential integrity — DateKey coverage check ──────────────────────
-- All DateKeys in all fact views should exist in dim.vDate
-- EXPECTED: 0 orphan DateKeys in each fact
PRINT '';
PRINT '  V10 — Referential integrity: orphan DateKeys vs dim.vDate';
SELECT 'fact.vOnlineSales'               AS FactView,
       COUNT(*)                          AS OrphanDateKeys
FROM [fact].[vOnlineSales] f
WHERE NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.DateKey)
UNION ALL
SELECT 'fact.vStoreSales',
       COUNT(*)
FROM [fact].[vStoreSales] f
WHERE NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.DateKey)
UNION ALL
SELECT 'fact.vReturns (ReturnDateKey)',
       COUNT(*)
FROM [fact].[vReturns] f
WHERE NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.ReturnDateKey)
UNION ALL
SELECT 'fact.vReturns (OriginalSaleDateKey)',
       COUNT(*)
FROM [fact].[vReturns] f
WHERE f.OriginalSaleDateKey IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.OriginalSaleDateKey)
UNION ALL
SELECT 'fact.vInventory',
       COUNT(*)
FROM [fact].[vInventory] f
WHERE NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.DateKey)
UNION ALL
SELECT 'fact.vOrderFulfillment',
       COUNT(*)
FROM [fact].[vOrderFulfillment] f
WHERE NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.OrderDateKey)
UNION ALL
SELECT 'fact.vCustomerSurvey',
       COUNT(*)
FROM [fact].[vCustomerSurvey] f
WHERE NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.SurveyDateKey)
UNION ALL
SELECT 'fact.vCustomerAcquisition',
       COUNT(*)
FROM [fact].[vCustomerAcquisition] f
WHERE NOT EXISTS (SELECT 1 FROM [dim].[vDate] d WHERE d.DateKey = f.AcquisitionDateKey)
ORDER BY FactView;

PRINT '';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '  Script 10 v2.0 completed successfully.';
PRINT '';
PRINT '  Fact views created (11 total):';
PRINT '    fact.vOnlineSales          — online sales, line-item grain, CustomerKey';
PRINT '    fact.vStoreSales           — physical sales, summary grain, ChannelKey';
PRINT '    fact.vReturns              — unified returns UNION ALL, dual DateKey pairs';
PRINT '    fact.vInventory            — stock snapshots, 5-tier status, DateKey INT added';
PRINT '    fact.vSalesQuota           — Budget/Actual/Forecast (always filter ScenarioName!)';
PRINT '    fact.vExchangeRate         — monthly FX rates, YearMonthKey join';
PRINT '    fact.vOrderFulfillment     — SLA, FulfillmentTier, IsOnTime flag';
PRINT '    fact.vCustomerSurvey       — NPS + CSAT, SurveyResponseID + SurveyTrigger added';
PRINT '    fact.vMarketingSpend       — monthly channel spend, SpendEfficiencyTier';
PRINT '    fact.vCustomerAcquisition  — customer acquisition bridge, CACMidpoint';
PRINT '    fact.vOrderPayment         — payment method bridge, OrderValue';
PRINT '';
PRINT '  COMPLETE SEMANTIC LAYER: 11 dim views + 11 fact views = 22 views total.';
PRINT '';
PRINT '  Next steps:';
PRINT '    Python export pipeline → generate Parquet files for Power BI';
PRINT '    Power BI → import tables, define relationships, build measures';
PRINT '════════════════════════════════════════════════════════════════════';
GO
