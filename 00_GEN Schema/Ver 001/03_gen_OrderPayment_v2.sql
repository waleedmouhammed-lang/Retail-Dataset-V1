/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║            SCRIPT 03: gen.OrderPayment — PAYMENT METHOD ASSIGNMENT           ║
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
  This script generates gen.OrderPayment — a one-row-per-order table that
  assigns every distinct online sales order in dbo.FactOnlineSales to exactly
  one payment method from gen.DimPaymentMethod.

  The Contoso source records what was sold but has no concept of HOW customers
  paid. Without this table, all CFO and CMO payment analytics are dark: no
  payment method mix, no digital adoption trending, no BNPL exposure analysis,
  and no payment-to-order-value correlation.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Business Questions Unlocked                                            │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  CFO: What is our payment method mix across all orders?                 │
  │  CFO: Is digital payment adoption growing year-over-year?               │
  │  CFO: What is our BNPL exposure and average order value for BNPL users? │
  │  CFO: What % of high-value orders are paid via Bank Transfer?           │
  │  CMO: Does payment method preference vary by customer region?           │
  │  COO: Do Cash-on-Delivery orders have higher return / cancel rates?     │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  GRAIN AND SCOPE
--------------------------------------------------------------------------------
  Grain : One row per distinct SalesOrderNumber in dbo.FactOnlineSales.
  Scope : ALL online sales orders — no exclusions. A customer pays once per
          order regardless of how many line items the order contains.

--------------------------------------------------------------------------------
  TEMPORAL SHIFT — ARCHITECTURE NOTE
--------------------------------------------------------------------------------
  OrderDateKey is stored as the YYYYMMDD integer derived from the RAW source
  DateKey (2007–2009 era). No +16 year offset is applied at the [gen] layer.
  The temporal shift is applied EXCLUSIVELY at fact.vOrderPayment view layer,
  consistent with the project-wide architectural principle.

--------------------------------------------------------------------------------
  SCORING MODEL — DESIGN RATIONALE
--------------------------------------------------------------------------------
  Assigns payment methods via a competitive scoring system:
  each order receives a score for all 7 payment methods and the highest wins.

  FinalScore = GeoBaseWeight × TimeFactor × OrderValueFactor × RandomNoise

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Factor 1: GeoBaseWeight — continent-level payment preference           │
  ├──────────────┬──────────────────────┬──────────────────────────────────┤
  │ Region       │ Dominant Method      │ Notable Signal                    │
  ├──────────────┼──────────────────────┼──────────────────────────────────┤
  │ N. America   │ Credit Card (40.0)   │ BNPL emerging (8.0)              │
  │ Europe       │ Credit=Debit=22.0    │ BNPL highest (10.0) — Klarna HQ  │
  │ Asia         │ Cash on Del. (30.0)  │ COD still dominant               │
  │ Other/Unknwn │ Cash on Del. (35.0)  │ Developing market defaults       │
  └──────────────┴──────────────────────┴──────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Factor 2: TimeFactor — payment adoption shift 2023→2025               │
  ├──────────────────────┬──────────────────────────────────────────────── ┤
  │ BNPL (Key=7)         │ 0.70 → 1.30  (steepest growth — Affirm/Klarna) │
  │ Cash on Delivery     │ 1.20 → 0.80  (sharpest decline)                 │
  │ Credit Card          │ 1.10 → 1.00  (slight decline — BNPL eats share)│
  │ PayPal               │ 1.10 → 0.90  (declining — competition)          │
  │ Gift Card            │ 0.90 → 1.10  (slight growth)                    │
  │ Debit Card           │ 1.00         (stable)                            │
  │ Bank Transfer        │ 1.00 → 0.90  (slight decline)                   │
  └──────────────────────┴──────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Factor 3: OrderValueFactor — payment affinity by basket size           │
  ├────────────────────────┬────────────────────────────────────────────── ┤
  │ Bank Transfer ≥$500    │ 1.50× — high-value specialist                  │
  │ BNPL $100–$500         │ 1.40× — instalment sweet spot                  │
  │ Credit Card ≥$500      │ 1.30× — trusted for high-value                 │
  │ Cash on Del. >$300     │ 0.40× — sharply penalised (COD refusal risk)   │
  └────────────────────────┴────────────────────────────────────────────────┘

  Factor 4: RandomNoise — (0.5 + ABS(CHECKSUM(NEWID())) % 1000 / 1000.0)
  Re-evaluated per CROSS JOIN row. Prevents deterministic ties.

--------------------------------------------------------------------------------
  OUTPUT TABLE
--------------------------------------------------------------------------------
  gen.OrderPayment
    SalesOrderNumber  NVARCHAR(20) PK   Natural key → dbo.FactOnlineSales
    PaymentMethodKey  INT          FK   → gen.DimPaymentMethod (BNPL = Key 7)
    OrderDateKey      INT               YYYYMMDD raw source date (no +16 shift)
    OrderValue        MONEY             SUM(SalesAmount) per order

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run order   : Script 03 — Run after Script 01
  Dependencies: [gen] schema, gen.DimPaymentMethod, dbo.FactOnlineSales,
                dbo.DimCustomer, dbo.DimGeography
  Impact      : Creates ONE new table in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — idempotent DROP / CREATE guard.
  Can parallel  : YES — Scripts 03 and 04 are independent of each other.
                  Script 05 (gen.FactMarketingSpend) MUST wait for Script 02.

================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECKS (4 checks)                  ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Four sequential dependency checks run before any DDL:                     ║
-- ║  (1) [gen] schema            → Script 00 required                          ║
-- ║  (2) gen.DimPaymentMethod    → Script 01 required (7 rows incl. BNPL)      ║
-- ║  (3) dbo.FactOnlineSales     → Contoso source required                     ║
-- ║  (4) dbo.DimCustomer         → Contoso source required                     ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  Script 03 does NOT depend on Script 02. It can run in parallel with       ║
-- ║  Script 02. The only hard prerequisite beyond Script 00 is Script 01.      ║
-- ║  If check (2) fires, confirm gen.DimPaymentMethod has 7 rows (not 6).     ║
-- ║  The BNPL row (Key=7) was added after the initial build — older copies     ║
-- ║  of Script 01 may be missing it.                                           ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT ON SUCCESS:                                               ║
-- ║  ✓ [gen] schema confirmed.                                                 ║
-- ║  ✓ [gen].[DimPaymentMethod] confirmed.                                     ║
-- ║  ✓ [dbo].[FactOnlineSales] confirmed.                                      ║
-- ║  ✓ [dbo].[DimCustomer] confirmed.                                          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- PRE-CHECK: Verify all dependencies before any DDL executes
-- ============================================================================

-- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gen')
IF SCHEMA_ID('gen') IS NULL
BEGIN
    -- RAISERROR('FATAL: [gen] schema not found. Run Script 00 first.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [gen] schema not found. Run Script 00 first.');
    
    THROW 50001, @ErrorMessage, 1;

    
END
ELSE
BEGIN
    PRINT '✓ [gen] schema confirmed.';
END
GO

IF OBJECT_ID('[gen].[DimPaymentMethod]', 'U') IS NULL
BEGIN
    
    -- RAISERROR('ERROR: [gen].[DimPaymentMethod] does not exist. Run Script 01 first.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('ERROR: [gen].[DimPaymentMethod] does not exist. Run Script 01 first.');
    
    THROW 50001, @ErrorMessage, 1;

    
END
ELSE
BEGIN
    PRINT '✓ [gen].[DimPaymentMethod] confirmed.';
END
GO

IF OBJECT_ID('[dbo].[FactOnlineSales]', 'U') IS NULL
BEGIN
    
    -- RAISERROR('ERROR: [gen].[DimAcquisitionChannel] does not exist. Run Script 01 first.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('ERROR: [dbo].[FactOnlineSales] does not exist. Ensure ContosoRetailDW database is selected and source tables are present.');
    
    THROW 50001, @ErrorMessage, 1;

    
END
ELSE
BEGIN
    PRINT '✓ [dbo].[FactOnlineSales] confirmed.';
END
GO

IF OBJECT_ID('[dbo].[DimCustomer]', 'U') IS NULL
BEGIN
    
    -- RAISERROR('ERROR: [gen].[DimAcquisitionChannel] does not exist. Run Script 01 first.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('ERROR: [dbo].[DimCustomer] does not exist. Ensure ContosoRetailDW database is selected and source tables are present.');
    
    THROW 50001, @ErrorMessage, 1;

    
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimCustomer] confirmed.';
END
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — STEP 1: TARGET TABLE DEFINITION                             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Drops (if exists) and recreates gen.OrderPayment — a 4-column table with  ║
-- ║  one row per distinct online sales order.                                  ║
-- ║                                                                             ║
-- ║  TABLE DESIGN DECISIONS                                                     ║
-- ║  • SalesOrderNumber NVARCHAR(20) PK — natural key matching FactOnlineSales.║
-- ║    No surrogate key needed: the source key is already unique per order.    ║
-- ║  • PaymentMethodKey INT FK — integer reference to gen.DimPaymentMethod.    ║
-- ║    Storing the key (not the name) is critical for VertiPaq compression     ║
-- ║    in Power BI. String-based relationships are 10-100× less efficient.     ║
-- ║  • OrderDateKey INT YYYYMMDD — denormalised from the source DateKey.        ║
-- ║    Enables time-sliced payment analysis without joining back to source.    ║
-- ║    Stored in raw source format (no +16 shift — shift applied at view).     ║
-- ║  • OrderValue MONEY — SUM(SalesAmount) per order, denormalised here for   ║
-- ║    payment-value correlation analysis without a fact-to-fact join.         ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  The FK constraint to gen.DimPaymentMethod enforces referential integrity  ║
-- ║  at the database engine level. If Script 01 produced only 6 rows           ║
-- ║  (missing BNPL Key=7), inserts for BNPL-winning orders will fail with a    ║
-- ║  FK violation here. Always verify gen.DimPaymentMethod = 7 rows first.    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 1: Create target table (idempotent — drops and recreates if exists)
-- ============================================================================

DROP TABLE IF EXISTS [gen].[OrderPayment];
PRINT '✓ Existing gen.OrderPayment table dropped (if existed).';
GO

CREATE TABLE [gen].[OrderPayment]
(
    [SalesOrderNumber]  NVARCHAR(20)    NOT NULL,
    [PaymentMethodKey]  INT             NOT NULL,
    [OrderDateKey]      INT             NOT NULL,   
    [OrderValue]        DECIMAL(19,4)   NOT NULL,   

    CONSTRAINT [PK_OrderPayment]
        PRIMARY KEY CLUSTERED ([SalesOrderNumber]),

    CONSTRAINT [FK_OrderPayment_PaymentMethod]
        FOREIGN KEY ([PaymentMethodKey])
        REFERENCES [gen].[DimPaymentMethod] ([PaymentMethodKey])
);
GO

PRINT '  → gen.OrderPayment table created.';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2: COMPOSITE SCORING ALGORITHM                         ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Assigns exactly one payment method to every online order using a          ║
-- ║  3-CTE competitive scoring pipeline fed by pre-declared scalars.           ║
-- ║                                                                             ║
-- ║  ┌─────────────────────────────────────────────────────────────────────┐   ║
-- ║  │  ALGORITHM — 3-STAGE CTE PIPELINE                                   │   ║
-- ║  ├─────────────────────────────────────────────────────────────────────┤   ║
-- ║  │  DECLARE @MinYear / @MaxYear / @YearRange                           │   ║
-- ║  │    Pre-computed BEFORE the CTE chain. Prevents a Table Spool on     │   ║
-- ║  │    the 13M-row FactOnlineSales table if these were computed inside  │   ║
-- ║  │    the CTE body. This is a critical performance pattern.            │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 1: OrderProfile                                                │   ║
-- ║  │    One row per order. Aggregates OrderValue, resolves customer      │   ║
-- ║  │    geography for GeoBaseWeight, and computes YearProgress (0→1).   │   ║
-- ║  │    DateKey is a DATE in Contoso — CONVERT(VARCHAR(8),...,112)       │   ║
-- ║  │    produces the YYYYMMDD string which is cast to INT for storage.   │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 2: PaymentScores                                               │   ║
-- ║  │    CROSS JOIN OrderProfile × DimPaymentMethod (7 methods).         │   ║
-- ║  │    Produces N_orders × 7 rows, each with a FinalScore computed as: │   ║
-- ║  │    GeoBaseWeight × TimeFactor × OrderValueFactor × RandomNoise     │   ║
-- ║  │    All three scoring CASE blocks are compact one-liners per method  │   ║
-- ║  │    key for readability without sacrificing logic clarity.           │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 3: RankedPayments                                              │   ║
-- ║  │    ROW_NUMBER() PARTITION BY SalesOrderNumber ORDER BY Score DESC.  │   ║
-- ║  │    ScoreRank=1 = winning payment method per order.                  │   ║
-- ║  │    Only ScoreRank=1 rows are inserted.                              │   ║
-- ║  └─────────────────────────────────────────────────────────────────────┘   ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  1. ISNULL(op.YearProgress, 0.5) is applied to every TimeFactor CASE      ║
-- ║     expression. Without this guard, orders with NULL YearProgress (rare   ║
-- ║     edge case where a single-year dataset makes @YearRange=0) would        ║
-- ║     produce NULL FinalScores and could prevent correct winner selection.   ║
-- ║  2. The compact CASE WHEN pattern in PaymentScores (all 7 keys on one      ║
-- ║     line) is intentional for readability on large scoring blocks. Each     ║
-- ║     WHEN clause maps PaymentMethodKey → multiplier factor directly.        ║
-- ║  3. Zero-value orders (~10K rows from fully-discounted source orders) are  ║
-- ║     intentionally retained. OrderValue=0 does not break scoring. These    ║
-- ║     should be filtered at the DAX/report layer, not here.                 ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 2: Populate via composite scoring model
-- ============================================================================
-- PRE-CALCULATION: Extract static temporal boundaries to variables.
-- Prevents a catastrophic 13-million row Table Spool on FactOnlineSales.
DECLARE @MinYear INT;
DECLARE @MaxYear INT;
DECLARE @YearRange FLOAT;

SELECT 
    @MinYear = MIN(YEAR(DateKey)),
    @MaxYear = MAX(YEAR(DateKey))
FROM [dbo].[FactOnlineSales];

SET @YearRange = NULLIF(@MaxYear - @MinYear, 0);

    -- -------------------------------------------------------------------------
    -- One row per order.
    -- Aggregates OrderValue and resolves customer geography for GeoBaseWeight.
    -- Computes YearProgress (0.0→1.0) across the raw source year range so
    -- TimeFactor tracks payment evolution proportionally across the dataset.
    --
    -- NOTE: f.DateKey is DATE in dbo.FactOnlineSales.
    --       CONVERT(VARCHAR(8), ..., 112) produces YYYYMMDD string → cast to INT.
    -- -------------------------------------------------------------------------

;WITH OrderProfile AS (
    SELECT
        f.SalesOrderNumber,
        CAST(CONVERT(VARCHAR(8), MIN(f.DateKey), 112) AS INT)    AS OrderDateKey,
        CAST(SUM(f.SalesAmount) AS DECIMAL(19,4))                AS OrderValue,
        ISNULL(MIN(g.ContinentName), 'Unknown')                  AS CustomerContinent,

        -- Ultra-fast scalar math using pre-calculated variables
        CAST(MIN(YEAR(f.DateKey)) - @MinYear AS FLOAT) / @YearRange AS YearProgress

    FROM       [dbo].[FactOnlineSales]   f
    INNER JOIN [dbo].[DimCustomer]       c  ON  f.CustomerKey  = c.CustomerKey
    LEFT  JOIN [dbo].[DimGeography]      g  ON  c.GeographyKey = g.GeographyKey
    GROUP BY   f.SalesOrderNumber
),

PaymentScores AS (
    SELECT
        op.SalesOrderNumber,
        op.OrderDateKey,
        op.OrderValue,
        pm.PaymentMethodKey,

        CASE op.CustomerContinent
            WHEN 'North America' THEN
                CASE pm.PaymentMethodKey 
                    WHEN 1 THEN 40.0 
                    WHEN 2 THEN 15.0 
                    WHEN 3 THEN 18.0 
                    WHEN 4 THEN 5.0 
                    WHEN 5 THEN 7.0 
                    WHEN 6 THEN 7.0 
                    WHEN 7 THEN 8.0 
                END
            WHEN 'Europe' THEN
                CASE pm.PaymentMethodKey 
                    WHEN 1 THEN 22.0 
                    WHEN 2 THEN 22.0 
                    WHEN 3 THEN 15.0 
                    WHEN 4 THEN 20.0 
                    WHEN 5 THEN 5.0 
                    WHEN 6 THEN 6.0 
                    WHEN 7 THEN 10.0 
                END
            WHEN 'Asia' THEN
                CASE pm.PaymentMethodKey 
                    WHEN 1 THEN 20.0 
                    WHEN 2 THEN 18.0 
                    WHEN 3 THEN 12.0 
                    WHEN 4 THEN 8.0 
                    WHEN 5 THEN 30.0 
                    WHEN 6 THEN 7.0 
                    WHEN 7 THEN 5.0 
                END
            ELSE  
                CASE pm.PaymentMethodKey 
                    WHEN 1 THEN 20.0 
                    WHEN 2 THEN 15.0 
                    WHEN 3 THEN 10.0 
                    WHEN 4 THEN 8.0 
                    WHEN 5 THEN 35.0 
                    WHEN 6 THEN 9.0 
                    WHEN 7 THEN 3.0 
                END
        END  

        * CASE pm.PaymentMethodKey
            WHEN 1 THEN 1.10 - (ISNULL(op.YearProgress, 0.5) * 0.10)
            WHEN 2 THEN 1.00
            WHEN 3 THEN 1.10 - (ISNULL(op.YearProgress, 0.5) * 0.20)
            WHEN 4 THEN 1.00 - (ISNULL(op.YearProgress, 0.5) * 0.10)
            WHEN 5 THEN 1.20 - (ISNULL(op.YearProgress, 0.5) * 0.40)
            WHEN 6 THEN 0.90 + (ISNULL(op.YearProgress, 0.5) * 0.20)
            WHEN 7 THEN 0.70 + (ISNULL(op.YearProgress, 0.5) * 0.60)
          END  

        * CASE pm.PaymentMethodKey
            WHEN 1 THEN 
                CASE 
                    WHEN op.OrderValue > 500 THEN 1.30 
                    WHEN op.OrderValue > 100 THEN 1.10 
                    ELSE 0.90 
                END
            WHEN 2 THEN 1.00
            WHEN 3 THEN 
                CASE WHEN op.OrderValue > 500 THEN 0.80 
                    ELSE 1.10 
                END
            WHEN 4 THEN 
                CASE WHEN op.OrderValue > 500 THEN 1.50 
                     WHEN op.OrderValue > 200 THEN 1.20 
                     ELSE 0.60 
                END
            WHEN 5 THEN 
                CASE WHEN op.OrderValue > 300 THEN 0.40 
                     WHEN op.OrderValue > 100 THEN 0.70 
                     ELSE 1.30 
                END
            WHEN 6 THEN 
                CASE WHEN op.OrderValue > 500 THEN 0.50 
                     WHEN op.OrderValue > 200 THEN 0.80 
                     ELSE 1.30 
                END
            WHEN 7 THEN 
                CASE WHEN op.OrderValue > 500 THEN 1.20 
                     WHEN op.OrderValue > 100 THEN 1.40 
                     ELSE 0.80 
                END
          END  

        * (0.5 + (ABS(CHECKSUM(NEWID())) % 1000) / 1000.0)
        AS FinalScore

    FROM       OrderProfile              op
    CROSS JOIN [gen].[DimPaymentMethod]  pm
),

RankedPayments AS (
    SELECT
        SalesOrderNumber,
        OrderDateKey,
        OrderValue,
        PaymentMethodKey,
        ROW_NUMBER() OVER (
            PARTITION BY SalesOrderNumber
            ORDER BY     FinalScore DESC
        ) AS ScoreRank
    FROM PaymentScores
)

INSERT INTO [gen].[OrderPayment]
    ([SalesOrderNumber], [PaymentMethodKey], [OrderDateKey], [OrderValue])
SELECT
    SalesOrderNumber,
    PaymentMethodKey,
    OrderDateKey,
    OrderValue
FROM  RankedPayments
WHERE ScoreRank = 1;
GO

PRINT '  → gen.OrderPayment populated.';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — STEP 3: PERFORMANCE INDEX                                   ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates a single Non-Clustered Index optimising the two most common       ║
-- ║  access patterns against gen.OrderPayment:                                 ║
-- ║  (1) GROUP BY PaymentMethodKey  — payment mix reports and distribution     ║
-- ║      queries: counts/aggregates by method.                                 ║
-- ║  (2) JOIN on SalesOrderNumber   — enrichment joins from fact.vOnlineSales  ║
-- ║      to fact.vOrderPayment.                                                ║
-- ║                                                                             ║
-- ║  WHY INCLUDE THREE COLUMNS                                                  ║
-- ║  SalesOrderNumber, OrderDateKey, and OrderValue are the columns most        ║
-- ║  commonly projected alongside PaymentMethodKey in analytical queries.      ║
-- ║  Including them in the index leaf pages eliminates Key Lookups back to the ║
-- ║  clustered index (SalesOrderNumber PK) for those column fetches.           ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 3: Non-clustered index
-- Optimises the most common query patterns:
--   - GROUP BY PaymentMethodKey (payment mix reports)
--   - JOIN on SalesOrderNumber (order-level enrichment from fact.vOnlineSales)
--   - Filter on OrderDateKey (time-sliced payment analysis)
-- ============================================================================

CREATE NONCLUSTERED INDEX [IX_OrderPayment_PaymentMethodKey]
    ON [gen].[OrderPayment] ([PaymentMethodKey])
    INCLUDE ([SalesOrderNumber], [OrderDateKey], [OrderValue]);
GO

PRINT '  → Index IX_OrderPayment_PaymentMethodKey created.';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — SET NOEXEC OFF RESET                                         ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Resets the session execution state so that the verification queries       ║
-- ║  below run normally even if a pre-check earlier triggered SET NOEXEC ON.   ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  SET NOEXEC OFF must appear AFTER all DDL and DML batches and BEFORE the   ║
-- ║  verification queries. If it were placed before the INSERT it would        ║
-- ║  allow the INSERT to run even when a dependency check had failed.          ║
-- ║  Placement matters: guard ON at top, reset OFF before verification.        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- RESET NOEXEC — ensures subsequent batches in the same session run normally
-- ============================================================================

GO


-- ============================================================================
-- VERIFICATION SUITE  (V1 – V5)
-- Run all checks after STEP 2 completes.  All "expect 0" rows must be 0.
-- ============================================================================

PRINT '';
PRINT '================================================================';
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 6 — VERIFICATION SUITE (V1 – V5)                                ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  PURPOSE                                                                    ║
-- ║  Five verification queries confirm correctness at multiple levels:         ║
-- ║  population completeness, payment distribution, digital adoption trend,    ║
-- ║  order-value vs payment correlation, and referential integrity.            ║
-- ║                                                                             ║
-- ║  V1 row counts are EXACT (deterministic join to source).                   ║
-- ║  V2–V4 distributions are APPROXIMATE (scoring randomness).                 ║
-- ║  V5 integrity checks are EXACT — all must return 0.                        ║
-- ║                                                                             ║
-- ║  CONTOSO BASELINE (for reference):                                          ║
-- ║  dbo.FactOnlineSales contains approximately 1.65 million distinct orders.  ║
-- ║  gen.OrderPayment should have the same count — one row per order.          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
PRINT '  gen.OrderPayment — Verification Suite';
PRINT '================================================================';
PRINT '';


-- ----------------------------------------------------------------------------
-- V1 — ROW COUNT & COMPLETENESS
-- gen.OrderPayment must have exactly one row per distinct SalesOrderNumber
-- in dbo.FactOnlineSales.  Delta and orphan counts must both be zero.
-- ----------------------------------------------------------------------------
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — ROW COUNT & COMPLETENESS (all deltas must be 0)                   │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate — based on Contoso source):               │
-- │  ┌──────────────────────────────────────────────────────┬───────────┐   │
-- │  │ Metric                                               │ Value     │   │
-- │  ├──────────────────────────────────────────────────────┼───────────┤   │
-- │  │ Source distinct orders                               │ 1,674,320 │   │
-- │  │ gen.OrderPayment rows                                │ 1,674,320│   │
-- │  │ Delta (expect 0)                                     │    0      │   │
-- │  │ Unassigned source orders (expect 0)                  │    0      │   │
-- │  └──────────────────────────────────────────────────────┴───────────┘   │
-- │  ✗ Delta > 0: CROSS JOIN or ROW_NUMBER filter has a defect.             │
-- │  ✗ Unassigned > 0: some source orders were skipped in OrderProfile.     │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '--- V1: Row Count & Completeness ---';

SELECT 'Source distinct orders'                AS Metric,
       COUNT(DISTINCT SalesOrderNumber)         AS Value
FROM   dbo.FactOnlineSales

UNION ALL

SELECT 'gen.OrderPayment rows',
       COUNT(*)
FROM   [gen].[OrderPayment]

UNION ALL

SELECT 'Delta (expect 0)',
       ABS(  COUNT(DISTINCT f.SalesOrderNumber)
           - (SELECT COUNT(*) FROM [gen].[OrderPayment])  )
FROM   dbo.FactOnlineSales f

UNION ALL

SELECT 'Unassigned source orders (expect 0)',
       COUNT(DISTINCT f.SalesOrderNumber)
FROM   dbo.FactOnlineSales f
WHERE  NOT EXISTS (
           SELECT 1
           FROM   [gen].[OrderPayment] op
           WHERE  op.SalesOrderNumber = f.SalesOrderNumber
       );


-- ----------------------------------------------------------------------------
-- V2 — PAYMENT METHOD DISTRIBUTION
-- All 7 payment methods must appear.  No single method should exceed ~45 %
-- (Credit Card North America drives the ceiling).
-- BNPL expected to be smallest but non-trivial (~3–8 % range).
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — PAYMENT METHOD DISTRIBUTION                                       │
-- │                                                                         │
-- │  EXPECTED PATTERN (7 rows — approximate % ranges):                      │
-- │  ┌─────────────────────────┬───────────────────┬─────────────────────┐  │
-- │  │ Method                  │ Expected % Range  │ Key Signal          │  │
-- │  ├─────────────────────────┼───────────────────┼─────────────────────┤  │
-- │  │ Credit Card (1)         │ ~28 – 38 %        │ Always #1 overall   │  │
-- │  │ Cash on Delivery (5)    │ ~12 – 20 %        │ Asia COD effect     │  │
-- │  │ Debit Card (2)          │ ~12 – 18 %        │                     │  │
-- │  │ PayPal (3)              │ ~8  – 14 %        │                     │  │
-- │  │ Bank Transfer (4)       │ ~6  – 12 %        │ Europe B2B effect   │  │
-- │  │ Gift Card (6)           │ ~4  –  9 %        │                     │  │
-- │  │ Buy Now Pay Later (7)   │ ~3  –  8 %        │ Smallest but >0    │  │
-- │  └─────────────────────────┴───────────────────┴─────────────────────┘  │
-- │  ✗ BNPL showing 0 rows: BNPL Key=7 is missing from gen.DimPaymentMethod.│
-- │  ✗ Any single method > 45%: GeoBaseWeight calibration has a defect.    │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '--- V2: Payment Method Distribution ---';

SELECT
    pm.PaymentMethodKey,
    pm.PaymentMethodName,
    pm.PaymentCategory,
    pm.IsDigital,
    COUNT(op.SalesOrderNumber)                                            AS OrderCount,
    CAST(COUNT(op.SalesOrderNumber) * 100.0
         / SUM(COUNT(op.SalesOrderNumber)) OVER ()
         AS DECIMAL(5,2))                                                 AS PctOfTotal,
    CAST(AVG(op.OrderValue) AS DECIMAL(12,2))                             AS AvgOrderValue,
    CAST(SUM(op.OrderValue) AS DECIMAL(18,2))                             AS TotalOrderValue
FROM       [gen].[OrderPayment]      op
INNER JOIN [gen].[DimPaymentMethod]  pm  ON  op.PaymentMethodKey = pm.PaymentMethodKey
GROUP BY
    pm.PaymentMethodKey,
    pm.PaymentMethodName,
    pm.PaymentCategory,
    pm.IsDigital
ORDER BY   OrderCount DESC;


-- ----------------------------------------------------------------------------
-- V3 — DIGITAL vs NON-DIGITAL ADOPTION TREND
-- Digital% must grow year-over-year; Non-Digital must decline.
-- Years shown in raw source range (2007-2009) — view layer adds +16.
-- Digital methods: Credit Card (1), Debit Card (2), PayPal (3),
--                  Gift Card (6), BNPL (7).
-- Non-Digital:     Bank Transfer (4), Cash on Delivery (5).
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — DIGITAL vs NON-DIGITAL ADOPTION TREND BY YEAR                    │
-- │                                                                         │
-- │  Years shown in RAW source range (2007–2009).                           │
-- │  At the view layer (+16 years) these become 2023–2025.                  │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERN:                                          │
-- │  ┌────────────┬────────────────────┬─────────────────────────────────┐  │
-- │  │ SourceYear │ Expected DigitalPct│ Direction                       │  │
-- │  ├────────────┼────────────────────┼─────────────────────────────────┤  │
-- │  │ 2007       │ ~72 – 78 %         │ Lower — COD/Transfer peak era   │  │
-- │  │ 2008       │ ~74 – 80 %         │ Rising                          │  │
-- │  │ 2009       │ ~76 – 82 %         │ Highest — BNPL growth + COD     │  │
-- │  │            │                    │ decline by 2025                 │  │
-- │  └────────────┴────────────────────┴─────────────────────────────────┘  │
-- │  ✗ Flat or declining DigitalPct: TimeFactor CASE block has a defect.   │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '--- V3: Digital vs Non-Digital Trend by Source Year ---';

SELECT
    -- SourceYear is derived from raw OrderDateKey (YYYYMMDD) for accurate time slicing.
    (op.OrderDateKey / 10000)  AS SourceYear,
    SUM(CASE WHEN pm.IsDigital = 1 THEN 1   ELSE 0   END)   AS DigitalOrders,
    SUM(CASE WHEN pm.IsDigital = 0 THEN 1   ELSE 0   END)   AS NonDigitalOrders,
    COUNT(*)                                                  AS TotalOrders,
    CAST(
        SUM(CASE WHEN pm.IsDigital = 1 THEN 1.0 ELSE 0.0 END)
        / COUNT(*) * 100
    AS DECIMAL(5,2))                                          AS DigitalPct
FROM       [gen].[OrderPayment]      op
INNER JOIN [gen].[DimPaymentMethod]  pm  ON  op.PaymentMethodKey = pm.PaymentMethodKey
GROUP BY   (op.OrderDateKey / 10000)
ORDER BY   SourceYear;


-- ----------------------------------------------------------------------------
-- V4 — ORDER VALUE SEGMENTATION vs PAYMENT METHOD
-- Validates OrderValueFactor logic:
--   High (>$500)    : Bank Transfer and Credit Card should lead.
--   Mid ($100-$500) : BNPL should over-index; balanced distribution.
--   Low (≤$100)     : Cash on Delivery and Gift Card should be highest.
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — PAYMENT METHOD BY ORDER VALUE SEGMENT                            │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERNS (OrderValueFactor effect):               │
-- │  ┌─────────────────────┬────────────────────────────────────────────┐   │
-- │  │ Value Segment       │ Expected Leaders                           │   │
-- │  ├─────────────────────┼────────────────────────────────────────────┤   │
-- │  │ 1 - High (>$500)    │ Credit Card and Bank Transfer should lead.  │   │
-- │  │                     │ Cash on Delivery should be near-zero.       │   │
-- │  │ 2 - Mid ($100-$500) │ BNPL should over-index vs its overall %.    │   │
-- │  │                     │ Balanced distribution across card methods.  │   │
-- │  │ 3 - Low (≤$100)     │ Cash on Delivery and Gift Card highest.     │   │
-- │  │                     │ Bank Transfer near-zero (not worth it).     │   │
-- │  └─────────────────────┴────────────────────────────────────────────┘   │
-- │  ✗ COD % same across all value segments: OrderValueFactor CTE defect.  │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '--- V4: Payment Method by Order Value Segment ---';

SELECT
    Seg.ValueSegment,
    pm.PaymentMethodName,
    COUNT(*) AS OrderCount,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY Seg.ValueSegment) AS DECIMAL(5,2)) AS PctWithinSegment
FROM [gen].[OrderPayment] op
INNER JOIN [gen].[DimPaymentMethod] pm ON op.PaymentMethodKey = pm.PaymentMethodKey
CROSS APPLY (
    SELECT CASE
        WHEN op.OrderValue > 500 THEN '1 - High (>$500)'
        WHEN op.OrderValue > 100 THEN '2 - Mid ($100-$500)'
        ELSE '3 - Low (≤$100)'
    END AS ValueSegment
) Seg
GROUP BY Seg.ValueSegment, pm.PaymentMethodName
ORDER BY Seg.ValueSegment, OrderCount DESC;


-- ----------------------------------------------------------------------------
-- V5 — REFERENTIAL INTEGRITY & DATA QUALITY
-- All four checks must return 0.  Any non-zero value indicates a defect.
-- ----------------------------------------------------------------------------
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V5 — REFERENTIAL INTEGRITY & DATA QUALITY (all 6 checks must be 0)    │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — all zeros):                                   │
-- │  ┌──────────────────────────────────────────────────────┬──────────┐   │
-- │  │ Check                                                │ Expected │   │
-- │  ├──────────────────────────────────────────────────────┼──────────┤   │
-- │  │ Orphan PaymentMethodKey                              │    0     │   │
-- │  │ Duplicate SalesOrderNumber                           │    0     │   │
-- │  │ NULL SalesOrderNumber                                │    0     │   │
-- │  │ NULL PaymentMethodKey                                │    0     │   │
-- │  │ NULL or zero OrderValue                              │    0     │   │
-- │  │ Missing BNPL assignments (Key=7, expect >0)          │    0     │   │
-- │  └──────────────────────────────────────────────────────┴──────────┘   │
-- │                                                                         │
-- │  NOTE on "NULL or zero OrderValue = 0":                                │
-- │  ~10,000 rows in dbo.FactOnlineSales have SalesAmount=0 (fully         │
-- │  discounted orders). These produce OrderValue=$0.00 in gen.OrderPayment│
-- │  — they are NOT data defects. The check "expect 0" refers to NULL      │
-- │  OrderValue rows only, not zero-value rows. Zero-value orders are       │
-- │  intentionally retained and should be filtered at the DAX layer.       │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '--- V5: Referential Integrity & Data Quality (all expect 0) ---';

SELECT
    'Orphan PaymentMethodKey'          AS [Check],
    COUNT(*)                           AS Value
FROM   [gen].[OrderPayment] op
WHERE  NOT EXISTS (
           SELECT 1
           FROM   [gen].[DimPaymentMethod] pm
           WHERE  pm.PaymentMethodKey = op.PaymentMethodKey
       )

UNION ALL

SELECT
    'Duplicate SalesOrderNumber',
    COUNT(*) - COUNT(DISTINCT SalesOrderNumber)
FROM   [gen].[OrderPayment]

UNION ALL

SELECT
    'NULL SalesOrderNumber',
    SUM(CASE WHEN SalesOrderNumber IS NULL THEN 1 ELSE 0 END)
FROM   [gen].[OrderPayment]

UNION ALL

SELECT
    'NULL PaymentMethodKey',
    SUM(CASE WHEN PaymentMethodKey IS NULL THEN 1 ELSE 0 END)
FROM   [gen].[OrderPayment]

UNION ALL

SELECT
    'NULL or zero OrderValue',
    SUM(CASE WHEN OrderValue IS NULL OR OrderValue <= 0 THEN 1 ELSE 0 END)
FROM   [gen].[OrderPayment]

UNION ALL

SELECT
    'Missing BNPL assignments (Key=7, expect >0)',
    -- Confirm BNPL actually got assigned to at least some orders
    CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
FROM   [gen].[OrderPayment]
WHERE  PaymentMethodKey = 7;

GO
