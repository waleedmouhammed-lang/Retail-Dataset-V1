/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║    SCRIPT 08: gen.PhysicalReturnEvents — IN-STORE CHANNEL RETURN EVENTS     ║
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
  This script generates gen.PhysicalReturnEvents — one row per return-bearing
  physical (in-store) sales line. Each row in dbo.FactSales where
  ReturnQuantity > 0 produces exactly one return event, enriched with a
  synthesized return date, an assigned return reason, and a refund outcome.

  The Contoso source embeds ReturnQuantity and ReturnAmount inside
  dbo.FactSales but records ZERO context about the return event. Without this
  table, in-store return KPIs are dark:

    ┌───────────────────────────────────────────────────────────────────────┐
    │  BUILT-IN CORRELATIONS (discoverable by students)                    │
    ├───────────────────────────────────────────────────────────────────────┤
    │  Physical returns → shorter lag (in-store, immediate decision)        │
    │  Physical returns → slightly higher refund rate (cash back at till)   │
    │  Return reasons   → excludes 'Online Only' reasons (e.g. Late         │
    │                      Delivery is not applicable in-store)             │
    │  Return volume    → tracks physical sales density by store/date       │
    └───────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Business Questions Unlocked                                            │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  COO:  What is the in-store return rate by product category?            │
  │  COO:  How does in-store return lag compare to online return lag?       │
  │  COO:  What are the top return reasons for in-store purchases?          │
  │  COO:  Which stores have the highest return rates?                      │
  │  CFO:  What is the total in-store refund liability?                     │
  │  CFO:  How does in-store vs. online refund rate compare?                │
  │  CSO:  What is the channel-level return rate difference?                │
  │  PM:   Which product categories drive the most in-store returns?        │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  GRAIN AND SCOPE
--------------------------------------------------------------------------------
  Grain   : One row per physical return event = one row per dbo.FactSales
            line where ReturnQuantity > 0 AND ReturnAmount > 0.
  Scope   : All physical (in-store) channel return lines in dbo.FactSales.
            Online returns are in Script 07 (gen.OnlineReturnEvents).
  Key     : SourceSalesKey (= dbo.FactSales.SalesKey). UNIQUE constraint
            enforces one return event per source row.

  ⚠  NO CUSTOMERKEY OR SALESORDERNUMBER — BY DESIGN
  dbo.FactSales has a SUMMARY GRAIN. Physical sales transactions are NOT
  attributed to individual customers in the Contoso source schema. There is
  no CustomerKey or SalesOrderNumber on FactSales rows. Accordingly:

    • gen.PhysicalReturnEvents has NO CustomerKey column.
    • gen.PhysicalReturnEvents has NO SalesOrderNumber column.
    • fact.vReturns will carry NULL for these columns on physical return rows.

  This is an intentional architectural decision — NOT a data quality issue.
  Students must be aware that physical return analysis is limited to
  store-level and product-level aggregations only. Customer-level return
  analysis (e.g., CLV impact of returns) is available for online returns only.

  ⚠  RETURN REASON SCOPE
  Physical returns can only receive reasons where AppliesTo = 'Both'.
  'Online Only' reasons (e.g., Late Delivery) are logically inapplicable
  to in-store transactions where the customer physically brought the item back.
  The dynamic reason-selection subquery filters this at the DB query layer.

  ⚠  TEMPORAL SCOPE
  All dates stored in raw source range (2007–2009). The +16 year offset to
  2023–2025 is applied EXCLUSIVELY at the fact.vReturns semantic view layer.

--------------------------------------------------------------------------------
  LAG MODEL — DESIGN RATIONALE
--------------------------------------------------------------------------------
  Physical in-store returns have significantly shorter lags than online:

  ┌────────────────────────────────────────────────────────────────────────────┐
  │  ValueTier  │  ReturnAmount     │  ReturnLag Range  │  Behavioral Logic    │
  ├────────────────────────────────────────────────────────────────────────────┤
  │  1 – Low    │  < $50            │  1 –  5 days      │  Same-week return:   │
  │             │                   │                   │  quick regret        │
  ├────────────────────────────────────────────────────────────────────────────┤
  │  2 – Mid    │  $50 – $199.99    │  3 – 10 days      │  Weekend return:     │
  │             │                   │                   │  tested at home      │
  ├────────────────────────────────────────────────────────────────────────────┤
  │  3 – High   │  ≥ $200           │  5 – 21 days      │  Considered:         │
  │             │                   │                   │  checked receipt     │
  └────────────────────────────────────────────────────────────────────────────┘
  Physical lags are consistently shorter than online because the customer must
  physically travel to the store — a friction that accelerates the decision.
  All ReturnDates are clamped to @MaxDate ('2009-12-31').

--------------------------------------------------------------------------------
  REFUND RATE — DESIGN RATIONALE
--------------------------------------------------------------------------------
  In-store refund rates are slightly higher than online (cash back at point
  of sale, less bureaucracy than online RMA processing):
    Low value  (< $50):    92% approval — immediate cash or card refund
    Mid value  ($50–$200): 88% approval — standard store return policy
    High value (≥ $200):   83% approval — may require manager approval
  Overall rate: ~88% — slightly above online (~85%) by design.

--------------------------------------------------------------------------------
  OUTPUT TABLE — gen.PhysicalReturnEvents
--------------------------------------------------------------------------------
  Column                Type                 Notes
  ──────────────────────────────────────────────────────────────────────────────
  PhysicalReturnEventID INT IDENTITY PK      Auto surrogate — resets on re-run
  SourceSalesKey        INT NOT NULL UNIQUE  = dbo.FactSales.SalesKey (grain key)
  StoreKey              INT NOT NULL FK      → dbo.DimStore
  ProductKey            INT NOT NULL FK      → dbo.DimProduct
  OriginalSaleDateKey   INT NOT NULL         YYYYMMDD — raw source (no +16)
  OriginalSaleDate      DATE NOT NULL        Actual sale date from source
  ReturnDateKey         INT NOT NULL         YYYYMMDD — synthesized (no +16)
  ReturnDate            DATE NOT NULL        Synthesized return processing date
  ReturnLagDays         INT NOT NULL         Days between sale and return (>= 0)
  ReturnQuantity        INT NOT NULL         From dbo.FactSales source
  ReturnAmount          MONEY NOT NULL       From dbo.FactSales source
  ReturnReasonKey       INT NOT NULL FK      → gen.DimReturnReason (Both only)
  IsRefunded            BIT NOT NULL         1 = refund approved and issued
  RefundAmount          MONEY NOT NULL       = ReturnAmount if refunded, else 0

  NOTE: No CustomerKey or SalesOrderNumber columns — by design. FactSales has
  a summary grain with no individual customer attribution.

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run order     : Script 08 — can run after Scripts 00 + 01 only
  Parallel with : Script 07 (gen.OnlineReturnEvents) — no shared dependency
  Dependencies  : [gen] schema (Script 00), gen.DimReturnReason (Script 01),
                  dbo.FactSales, dbo.DimStore, dbo.DimProduct
  Impact        : Creates ONE new table in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — idempotent DROP / CREATE guard on the table.

================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/


-- ============================================================================
-- PRE-CHECKS: Verify all dependencies before any DDL executes
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECKS (5 checks)               ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Five sequential dependency checks run before any DDL executes:          ║
-- ║  (1) [gen] schema             → Script 00 required                       ║
-- ║  (2) dbo.FactSales            → Contoso source required (return data)    ║
-- ║  (3) dbo.DimStore             → Required for StoreKey FK validation      ║
-- ║  (4) dbo.DimProduct           → Required for ProductKey FK validation    ║
-- ║  (5) gen.DimReturnReason      → Script 01 required (return reason FK)    ║
-- ║                                                                           ║
-- ║  EXPECTED OUTPUT ON SUCCESS (5 green ticks in Messages tab):             ║
-- ║  ✓ [gen] schema confirmed.                                               ║
-- ║  ✓ [dbo].[FactSales] confirmed.                                          ║
-- ║  ✓ [dbo].[DimStore] confirmed.                                           ║
-- ║  ✓ [dbo].[DimProduct] confirmed.                                         ║
-- ║  ✓ [gen].[DimReturnReason] confirmed.                                    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── CHECK 1 OF 5: [gen] Schema ───────────────────────────────────────────────

IF SCHEMA_ID('gen') IS NULL
BEGIN
    -- RAISERROR('FATAL: [gen] schema not found. Run Script 00 first.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [gen] schema not found. You must run script 00 first.');
    
    THROW 50000, @ErrorMessage, 1;

    ;
END
ELSE
BEGIN
    PRINT '✓ [gen] schema confirmed.';
END
GO
-- GO: T-SQL batch separator — each check is isolated so SET NOEXEC ON propagates correctly.

-- ── CHECK 2 OF 5: dbo.FactSales ──────────────────────────────────────────────

IF OBJECT_ID('[dbo].[FactSales]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[FactSales] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    ;
END
ELSE
BEGIN
    PRINT '✓ [dbo].[FactSales] confirmed.';
END
GO

-- ── CHECK 3 OF 5: dbo.DimStore ───────────────────────────────────────────────

IF OBJECT_ID('[dbo].[DimStore]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[DimStore] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    ;
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimStore] confirmed.';
END
GO

-- ── CHECK 4 OF 5: dbo.DimProduct ─────────────────────────────────────────────

IF OBJECT_ID('[dbo].[DimProduct]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[DimProduct] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    ;
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimProduct] confirmed.';
END
GO

-- ── CHECK 5 OF 5: gen.DimReturnReason ────────────────────────────────────────

IF OBJECT_ID('[gen].[DimReturnReason]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [gen].[DimReturnReason] not found. Run Script 01 first.');
    THROW 50000, @ErrorMessage, 1;
    ;
END
ELSE
BEGIN
    PRINT '✓ [gen].[DimReturnReason] confirmed.';
END
GO


-- ============================================================================
-- STEP 1: Create target table (idempotent — drops and recreates if exists)
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — STEP 1: TARGET TABLE DEFINITION                          ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Drops (if exists) and recreates gen.PhysicalReturnEvents — a 14-column  ║
-- ║  return event table where one row = one return-bearing FactSales line.   ║
-- ║                                                                           ║
-- ║  TABLE DESIGN DECISIONS                                                   ║
-- ║                                                                           ║
-- ║  SourceSalesKey — UNIQUE GRAIN ENFORCEMENT                                ║
-- ║  Unlike gen.OnlineReturnEvents which uses a compound (OrderNumber,       ║
-- ║  LineNumber) grain key, FactSales has a single-column SalesKey PK.      ║
-- ║  SourceSalesKey stores this value and a UNIQUE constraint enforces       ║
-- ║  the one-return-per-source-line grain at the database level.             ║
-- ║                                                                           ║
-- ║  NO CustomerKey or SalesOrderNumber — ARCHITECTURAL DECISION             ║
-- ║  FactSales is a summary-grain table. It records store-level product      ║
-- ║  sales without individual customer attribution. Adding NULL columns      ║
-- ║  here would misrepresent the data — they are simply absent by design.   ║
-- ║  fact.vReturns (the UNION ALL semantic view) will supply NULL for these  ║
-- ║  columns on physical rows explicitly.                                    ║
-- ║                                                                           ║
-- ║  ReturnLagDays CHECK >= 0 (not >= 1 as in the online script)             ║
-- ║  A physical in-store return can theoretically happen on the same day     ║
-- ║  as purchase (customer opens the box in the car park and returns          ║
-- ║  immediately). Online returns require at least 1 day for delivery.       ║
-- ║  This is a deliberate asymmetry between the two return scripts.          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

DROP TABLE IF EXISTS [gen].[PhysicalReturnEvents];
PRINT '✓ Existing [gen].[PhysicalReturnEvents] table dropped (if it existed).';
GO

CREATE TABLE [gen].[PhysicalReturnEvents] (
-- Fully bracketed [schema].[Table] notation — project-wide standard for all object references.

    -- ── Surrogate Primary Key ─────────────────────────────────────────────
    [PhysicalReturnEventID]  INT           IDENTITY(1,1) NOT NULL,
    -- IDENTITY(1,1): auto-incrementing surrogate PK. Resets on every DROP + recreate —
    -- harmless since this is a generated table, not a stable operational system.
    -- INT (not BIGINT): physical return row counts are far below the INT max (~2B).

    -- ── Source Traceability Key ───────────────────────────────────────────
    [SourceSalesKey]         INT                        NOT NULL,
    -- = dbo.FactSales.SalesKey. Degenerate dimension that enables drill-through
    -- to the source row without a formal model relationship. Also serves as the
    -- grain enforcement key (UNIQUE constraint below).

    -- ── Dimensional Foreign Keys ──────────────────────────────────────────
    [StoreKey]               INT                        NOT NULL,
    -- FK to dbo.DimStore. Enables store-level return rate analysis:
    -- which stores have the highest return rates? Is there a correlation
    -- between store size/location and return frequency?

    [ProductKey]             INT                        NOT NULL,
    -- FK to dbo.DimProduct. Enables product-level analysis: which SKUs
    -- are returned most in physical stores vs. online?

    -- ── Temporal Keys (dual date design for role-playing) ─────────────────
    [OriginalSaleDateKey]    INT                        NOT NULL,
    -- YYYYMMDD integer — the date of the original in-store sale.
    -- Sourced from dbo.FactSales.DateKey (no transformation at [gen] layer).
    -- ACTIVE relationship to dim.vDate in Power BI.

    [OriginalSaleDate]       DATE                       NOT NULL,
    -- DATE equivalent of OriginalSaleDateKey. Display column and base for lag.

    [ReturnDateKey]          INT                        NOT NULL,
    -- YYYYMMDD integer — synthesized return processing date.
    -- INACTIVE relationship to dim.vDate; activated via USERELATIONSHIP().

    [ReturnDate]             DATE                       NOT NULL,
    -- DATE equivalent of ReturnDateKey. OriginalSaleDate + value-tiered lag,
    -- clamped to @MaxDate ('2009-12-31').

    -- ── Return Timing Metric ──────────────────────────────────────────────
    [ReturnLagDays]          INT                        NOT NULL,
    -- Days between OriginalSaleDate and ReturnDate.
    -- Physical lags are shorter than online: 1–5 (Low), 3–10 (Mid), 5–21 (High).
    -- CHECK constraint allows >= 0 (same-day returns are possible in-store).

    -- ── Source Measures (copied from dbo.FactSales) ───────────────────────
    [ReturnQuantity]         INT                        NOT NULL,
    -- Units returned — exact copy from dbo.FactSales.ReturnQuantity.

    [ReturnAmount]           DECIMAL(19,4)                      NOT NULL,
    -- Monetary value of returned goods — exact copy from source.

    -- ── Return Classification ─────────────────────────────────────────────
    [ReturnReasonKey]        INT                        NOT NULL,
    -- FK to gen.DimReturnReason. Assigned from reasons where AppliesTo = 'Both'.
    -- 'Online Only' reasons are excluded — they are not applicable in-store.

    -- ── Refund Outcome ────────────────────────────────────────────────────
    [IsRefunded]             BIT                        NOT NULL,
    -- 1 = refund issued. In-store refunds are slightly more common than online
    -- due to lower friction (immediate cash/card reversal at the till).

    [RefundAmount]           DECIMAL(19,4)                      NOT NULL,
    -- = ReturnAmount when IsRefunded = 1, else 0. Stored explicitly.

    -- ── Constraints ───────────────────────────────────────────────────────
    CONSTRAINT [PK_PhysicalReturnEvents]
        PRIMARY KEY CLUSTERED ([PhysicalReturnEventID]),
    -- CLUSTERED PK on IDENTITY: optimal for append-heavy generation workloads.
    -- Sequential IDENTITY values produce physically ordered pages — no page splits
    -- during the INSERT batch, and minimal index fragmentation afterward.

    CONSTRAINT [UQ_PhysicalReturnEvents_SourceSalesKey]
        UNIQUE ([SourceSalesKey]),
    -- ⚠ BEST PRACTICE — GRAIN ENFORCEMENT AT THE DATABASE LEVEL:
    -- One return event per FactSales source row, enforced by the DB engine.
    -- If a pipeline defect causes a FactSales row to be processed twice,
    -- this constraint rejects the duplicate — protecting grain integrity.

    CONSTRAINT [FK_PhysicalReturnEvents_Store]
        FOREIGN KEY ([StoreKey])
        REFERENCES [dbo].[DimStore] ([StoreKey]),
    -- Ensures every return event belongs to a valid store in the dimension.
    -- Rejects any StoreKey in FactSales that has no matching DimStore row.

    CONSTRAINT [FK_PhysicalReturnEvents_Product]
        FOREIGN KEY ([ProductKey])
        REFERENCES [dbo].[DimProduct] ([ProductKey]),
    -- Ensures every return event references a valid product in the dimension.

    CONSTRAINT [FK_PhysicalReturnEvents_ReturnReason]
        FOREIGN KEY ([ReturnReasonKey])
        REFERENCES [gen].[DimReturnReason] ([ReturnReasonKey]),
    -- Ensures the assigned return reason exists in the dimension.
    -- The dynamic modulo-selection logic guarantees this, but the FK
    -- enforces it at the DB layer as a permanent data-quality safety net.

    CONSTRAINT [CHK_PhysicalReturnEvents_ReturnLag]
        CHECK ([ReturnLagDays] >= 0),
    -- ⚠ KEY DIFFERENCE FROM SCRIPT 07 — minimum lag is 0 (not 1):
    -- A physical in-store return can theoretically happen on the same day
    -- as purchase (customer opens the box in the car park and returns
    -- immediately). Online returns require at least 1 day for delivery.
    -- This deliberate asymmetry reflects real-world channel behavior.

    CONSTRAINT [CHK_PhysicalReturnEvents_ReturnAmount]
        CHECK ([ReturnAmount] >= 0),
    -- Pre-filter (WHERE ReturnAmount > 0) ensures this at the source level,
    -- but the constraint provides a permanent data-quality guarantee for
    -- any future inserts outside the current generation pipeline.

    CONSTRAINT [CHK_PhysicalReturnEvents_RefundAmount]
        CHECK ([RefundAmount] >= 0)
    -- Ensures no negative refund amounts enter the table.
    -- Negative values would corrupt SUM([RefundAmount]) KPIs.
);
GO

PRINT '  → [gen].[PhysicalReturnEvents] table created.';
GO


-- ============================================================================
-- STEP 2: Declare reference constant
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2: PRE-CTE VARIABLE DECLARATION                    ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  '2009-12-31': project-standard fixed reference date. Applied here to   ║
-- ║  clamp ReturnDate for late-year FactSales rows with longer lags.        ║
-- ║  GETDATE() is NEVER used — reproducibility requires a frozen anchor.    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

DECLARE @MaxDate DATE = '2009-12-31';
-- Project-standard fixed reference date: the end of the Contoso DW source range.
-- Used to clamp ReturnDate for late-year FactSales rows with longer lag values
-- (e.g., a high-value order placed 2009-12-10 + 21 days = 2010-01-01 without this cap).
-- GETDATE() is NEVER used — it would make ReturnDate non-reproducible across runs
-- and break temporal integrity on a historical dataset.
-- The +16 year shift to '2025-12-31' is applied ONLY at the fact.vReturns view layer.


-- ============================================================================
-- STEP 3: Populate via 4-stage behavioral CTE pipeline
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — STEP 3: 4-STAGE CTE PIPELINE + INSERT                   ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Identical pipeline structure to Script 07, adapted for FactSales:      ║
-- ║                                                                           ║
-- ║  CTE 1: SourceLines     — extract FactSales lines where Ret > 0         ║
-- ║  CTE 2: ValueTiered     — assign ValueTier from ReturnAmount buckets     ║
-- ║  CTE 3: NoiseLocked     — materialise NoiseA + NoiseB via CROSS APPLY   ║
-- ║  CTE 4: FinalRows       — derive lag, dates, reason, refund outcome      ║
-- ║                                                                           ║
-- ║  KEY DIFFERENCES FROM SCRIPT 07:                                         ║
-- ║  • Source: dbo.FactSales (not dbo.FactOnlineSales)                       ║
-- ║  • Grain key: SourceSalesKey (not SalesOrderNumber + LineNumber)         ║
-- ║  • No CustomerKey / SalesOrderNumber in output                           ║
-- ║  • Lag ranges are shorter (physical immediacy vs. online deliberation)   ║
-- ║  • Refund rates are slightly higher (92/88/83 vs. 90/85/80)             ║
-- ║  • Reason filter: AppliesTo = 'Both' ONLY (not 'Online Only')           ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

;WITH
-- Leading semicolon: defensive guard — prevents a syntax error if a prior
-- batch statement was not terminated before this WITH clause begins.

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 0: EligibleReasons (Performance Fix)
-- Pre-calculates the modulo index for physical return reasons ONCE.
-- ────────────────────────────────────────────────────────────────────────────
[EligibleReasons] AS (
    SELECT
        [rdr].[ReturnReasonKey],
        ROW_NUMBER() OVER (ORDER BY [rdr].[ReturnReasonKey]) - 1 AS [RN],
        -- ⚠ BEST PRACTICE — 0-BASED MODULO INDEX:
        -- ROW_NUMBER() is 1-indexed by default. Subtracting 1 produces a 0-based rank
        -- (0, 1, 2 ... N-1). This aligns with the modulo expression in FinalRows:
        --   [nl].[NoiseB] % [er].[TotalEligible]
        -- which returns values in range 0 to N-1 — ensuring every reason is reachable.
        COUNT(*) OVER ()                                          AS [TotalEligible]
        -- COUNT(*) OVER () with no PARTITION BY: counts ALL rows in this CTE result set.
        -- Dynamic denominator for the modulo selection — automatically reflects any
        -- future additions to gen.DimReturnReason without code changes.
    FROM [gen].[DimReturnReason] AS [rdr]
    WHERE [rdr].[AppliesTo] = 'Both'
    -- ⚠ CRITICAL DIFFERENCE FROM SCRIPT 07:
    -- Physical returns filter to 'Both' ONLY — 'Online Only' reasons
    -- (e.g., Late Delivery) are meaningless for in-store transactions where
    -- the customer physically brought the item back to the store.
    -- Script 07 uses AppliesTo IN ('Both', 'Online Only').
    -- This filter respects the AppliesTo column in gen.DimReturnReason.
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 1: SourceLines
-- Extracts all return-bearing lines from dbo.FactSales.
-- ────────────────────────────────────────────────────────────────────────────
[SourceLines] AS (
    SELECT
        [fs].[SalesKey]                               AS [SourceSalesKey],
        -- SalesKey: the surrogate PK of dbo.FactSales. Aliased to SourceSalesKey
        -- to clarify its role — this is a grain enforcement key, not a dimension FK.
        -- The UNIQUE constraint on this column in the output table prevents any
        -- FactSales row from producing more than one return event row.

        [fs].[StoreKey],
        -- FK to dbo.DimStore — sourced directly (no transformation needed).
        -- INT matches DimStore.StoreKey — no implicit type conversion on join.

        [fs].[ProductKey],
        -- FK to dbo.DimProduct — sourced directly.
        -- Enables product-level return analysis per store and channel.

        -- ⚠ BEST PRACTICE — PROJECT-STANDARD INT DATE KEY DERIVATION:
        -- DateKey in dbo.FactSales is stored as DATETIME (legacy Contoso type).
        -- CONVERT(VARCHAR(8), ..., 112): format code 112 = 'YYYYMMDD' — produces
        --   the 8-character string of the date portion only (no time component).
        -- CAST(... AS INT): converts the 8-char string to a YYYYMMDD integer.
        -- This two-step CAST + CONVERT is the project-standard date key derivation —
        -- consistent with Script 07 (FactOnlineSales) and all prior [gen] scripts.
        (YEAR([fs].[DateKey]) * 10000) + (MONTH([fs].[DateKey]) * 100) + DAY([fs].[DateKey]) AS [OriginalSaleDateKey],

        CAST([fs].[DateKey] AS DATE)                  AS [OriginalSaleDate],
        -- CAST to DATE: strips the time component from the DATETIME DateKey.
        -- DATE is used (not DATETIME) to: (a) enable clean DATEDIFF lag calculations,
        -- and (b) match the DATE type of the OriginalSaleDate target column.

        [fs].[ReturnQuantity],
        -- Units returned — exact copy from source. Pre-filtered to > 0 below.

        [fs].[ReturnAmount]
        -- Monetary value — exact copy from source. Pre-filtered to > 0 below.

    FROM  [dbo].[FactSales] AS [fs]
    WHERE [fs].[ReturnQuantity] > 0
    -- Primary filter: only rows that represent an actual return event.
      AND [fs].[ReturnAmount]   > 0
    -- Secondary quality filter: excludes rows where ReturnQuantity > 0 but
    -- ReturnAmount = 0.00 — a known data quality artefact in the Contoso source.
    -- Including them would create zero-value return events distorting KPIs.
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 2: ValueTiered
-- ────────────────────────────────────────────────────────────────────────────
[ValueTiered] AS (
    SELECT
        [sl].[SourceSalesKey],
        [sl].[StoreKey],
        [sl].[ProductKey],
        [sl].[OriginalSaleDateKey],
        [sl].[OriginalSaleDate],
        [sl].[ReturnQuantity],
        [sl].[ReturnAmount],
        -- Pass-throughs from CTE 1: all source columns preserved unchanged.

        CASE
            WHEN [sl].[ReturnAmount] <  50.00 THEN 1   -- Tier 1 Low:  1–5 days lag,  92% refund
            WHEN [sl].[ReturnAmount] < 200.00 THEN 2   -- Tier 2 Mid:  3–10 days lag, 88% refund
            ELSE                                   3   -- Tier 3 High: 5–21 days lag, 83% refund
        END AS [ValueTier]
        -- ValueTier is INTEGER (1/2/3) — downstream CASE uses simple WHEN syntax.
        -- Thresholds match Script 07 exactly — same bucket definitions ensure
        -- that cross-channel V1 comparisons (physical vs online) are like-for-like.
    FROM [SourceLines] AS [sl]
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 3: NoiseLocked
-- Materialises THREE independent random draws per row to prevent hidden
-- statistical correlations between lag time, reason, and refund outcome.
-- ────────────────────────────────────────────────────────────────────────────
[NoiseLocked] AS (
    SELECT
        [vt].*,
        -- Pass-through wildcard: carries all columns from ValueTiered unchanged.
        -- Using [vt].* here (not repeating individual column names) is acceptable
        -- because NoiseLocked is a single-source CTE — no column name ambiguity risk.
        [n].[NoiseA],
        -- Controls: the exact position of ReturnLagDays within the tier's range.
        -- Applied in CTE 4 Step A as: BaseLag + (NoiseA % TierRangeWidth).
        [n].[NoiseB],
        -- Controls: return reason selection via modulo index into EligibleReasons.
        -- Applied in CTE 4 CROSS APPLY as: NoiseB % TotalEligible → maps to RN.
        [n].[NoiseC]
        -- Controls: the refund approval decision, fully independent of lag time.
        -- Separate seed prevents spurious correlation between return wait time
        -- and whether the customer received a refund.
    FROM [ValueTiered] AS [vt]
    CROSS APPLY (
    -- ⚠ BEST PRACTICE — CROSS APPLY NOISE LOCKING (Script 06 pattern):
    -- NEWID() is non-deterministic. In a CTE, SQL Server MAY re-evaluate NEWID()
    -- each time the CTE is referenced, producing a different value per reference.
    -- Placing all three NEWID() calls inside a CROSS APPLY SELECT forces the engine
    -- to materialise all three values exactly ONCE per row at this stage.
    -- All downstream CTEs reference [nl].[NoiseA/B/C] — locked values, not live NEWID().
        SELECT
            ABS(CHECKSUM(NEWID())) AS [NoiseA],
            -- NEWID(): generates a UUID. CHECKSUM() → signed INT. ABS() → non-negative.
            -- Produces a uniformly distributed non-negative integer per row.
            ABS(CHECKSUM(NEWID())) AS [NoiseB],
            -- Second independent NEWID() call — uncorrelated with NoiseA.
            ABS(CHECKSUM(NEWID())) AS [NoiseC]
            -- Third independent draw — uncorrelated with both NoiseA and NoiseB.
    ) AS [n]
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 4: FinalRows
-- Derives all output columns from the locked noise values.
-- ────────────────────────────────────────────────────────────────────────────
[FinalRows] AS (
    SELECT
        [nl].[SourceSalesKey],
        [nl].[StoreKey],
        [nl].[ProductKey],
        [nl].[OriginalSaleDateKey],
        [nl].[OriginalSaleDate],
        [nl].[ReturnQuantity],
        [nl].[ReturnAmount],
        -- Pass-throughs from NoiseLocked: all source columns preserved.

        -- ── Step A: ReturnLagDays (physical: shorter than online) ─────────
        CASE [nl].[ValueTier]
            WHEN 1 THEN 1 + ([nl].[NoiseA] %  5)
            -- Tier 1 Low: base 1 day + noise 0–4 → range 1–5 days.
            -- % 5 produces values 0,1,2,3,4 — exactly a 5-element range.
            -- Same-week return: quick regret purchase.
            WHEN 2 THEN 3 + ([nl].[NoiseA] %  8)
            -- Tier 2 Mid: base 3 days + noise 0–7 → range 3–10 days.
            -- Weekend return: customer tested the item at home first.
            ELSE        5 + ([nl].[NoiseA] % 17)
            -- Tier 3 High: base 5 days + noise 0–16 → range 5–21 days.
            -- Considered return: customer checked receipt, may have consulted policy.
            -- Compare to Script 07: 21–45 days. Physical lags are consistently
            -- shorter because the customer must physically travel to the store.
        END AS [ReturnLagDays],

        -- ── Step B: ReturnDate — cleanly clamped to @MaxDate ──────────────
        LEAST(
        -- ⚠ BEST PRACTICE — LEAST() FOR TEMPORAL CLAMPING (SQL Server 2022+):
        -- Same pattern as Script 07. Prevents ReturnDate from exceeding the
        -- project's temporal boundary ('2009-12-31') for late-year sales rows.
            DATEADD(
                DAY,
                CASE [nl].[ValueTier]
                    WHEN 1 THEN 1 + ([nl].[NoiseA] %  5)
                    WHEN 2 THEN 3 + ([nl].[NoiseA] %  8)
                    ELSE        5 + ([nl].[NoiseA] % 17)
                END,
                -- Same lag formula as Step A — DATEADD adds lag days to OriginalSaleDate.
                [nl].[OriginalSaleDate]
                -- Base date: actual sale date (DATE type after CTE 1 CAST).
            ),
            @MaxDate
            -- Upper bound: project-standard fixed reference date '2009-12-31'.
        ) AS [ReturnDate],

        -- ── Step C: ReturnDateKey ─────────────────────────────────────────
        CAST(CONVERT(VARCHAR(8),
        -- Project-standard DATE → YYYYMMDD INT conversion, identical to Script 07.
        -- CONVERT(112) → 'YYYYMMDD' string; CAST → INT.
            LEAST(
                DATEADD(
                    DAY,
                    CASE [nl].[ValueTier]
                        WHEN 1 THEN 1 + ([nl].[NoiseA] %  5)
                        WHEN 2 THEN 3 + ([nl].[NoiseA] %  8)
                        ELSE        5 + ([nl].[NoiseA] % 17)
                    END,
                    [nl].[OriginalSaleDate]
                ),
                @MaxDate
                -- Clamp applied before INT conversion — keeps ReturnDateKey and
                -- ReturnDate temporally consistent (same clamped date, two formats).
            ), 112
        ) AS INT) AS [ReturnDateKey],

        -- ── Step D: ReturnReasonKey — 'Both' reasons only ─────────────────
        [rr].[ReturnReasonKey],
        -- Sourced from the CROSS APPLY (EligibleReasons) below.
        -- Because EligibleReasons filters to AppliesTo = 'Both', no 'Online Only'
        -- reason will ever appear in a physical return event row.

        -- ── Step E: IsRefunded — physical rates slightly higher ───────────
        CAST(CASE
            WHEN ([nl].[NoiseC] % 100) <
            -- (NoiseC % 100): uniform integer 0–99. Rows where value < threshold
            -- receive a refund. Threshold T → T% approval rate.
                 CASE [nl].[ValueTier]
                     WHEN 1 THEN 92
                     -- Low-value: 92% — immediate cash/card refund at the till.
                     WHEN 2 THEN 88
                     -- Mid-value: 88% — standard store return policy with receipt.
                     ELSE        83
                     -- High-value: 83% — may require manager approval or store policy review.
                     -- All three physical rates exceed the online equivalents (90/85/80)
                     -- reflecting lower processing friction for in-store returns.
                 END
            THEN 1
            -- Refund approved: IsRefunded = 1. RefundAmount = ReturnAmount in INSERT.
            ELSE 0
            -- No refund: IsRefunded = 0. RefundAmount = 0.00 in INSERT.
        END AS BIT) AS [IsRefunded]
        -- CAST AS BIT: converts the 0/1 result to 1-byte BIT storage type.

    FROM [NoiseLocked] AS [nl]

    -- ── CROSS APPLY: Dynamic ReturnReason selection (using CTE 0) ─────────
    CROSS APPLY (
    -- CROSS APPLY against EligibleReasons (CTE 0) — pre-computed 'Both' reasons only.
    -- Finds exactly ONE ReturnReasonKey per row via the locked NoiseB modulo index.
        SELECT [er].[ReturnReasonKey]
        FROM [EligibleReasons] AS [er]
        WHERE [er].[RN] = ([nl].[NoiseB] % [er].[TotalEligible])
        -- [nl].[NoiseB] % [er].[TotalEligible]: maps locked noise to a 0-based RN index.
        -- TotalEligible is consistent across all EligibleReasons rows (window COUNT(*)),
        -- so the modulo always resolves to exactly one matching row.
    ) AS [rr]
)

-- ── Final INSERT ──────────────────────────────────────────────────────────────
INSERT INTO [gen].[PhysicalReturnEvents] (
    [SourceSalesKey],
    [StoreKey],
    [ProductKey],
    [OriginalSaleDateKey],
    [OriginalSaleDate],
    [ReturnDateKey],
    [ReturnDate],
    [ReturnLagDays],
    [ReturnQuantity],
    [ReturnAmount],
    [ReturnReasonKey],
    [IsRefunded],
    [RefundAmount]
-- ⚠ BEST PRACTICE — EXPLICIT COLUMN LIST ON INSERT:
-- All 13 stored columns are named explicitly. PhysicalReturnEventID is excluded —
-- filled automatically by the IDENTITY property on each insert.
-- Explicit column lists prevent silent data misalignment if the table schema
-- is ever altered and the column order changes.
)
SELECT
    [f].[SourceSalesKey],
    -- Grain key — = dbo.FactSales.SalesKey. Validated by UQ_PhysicalReturnEvents_SourceSalesKey.

    [f].[StoreKey],
    -- FK to dbo.DimStore — validated by FK_PhysicalReturnEvents_Store at INSERT time.

    [f].[ProductKey],
    -- FK to dbo.DimProduct — validated by FK_PhysicalReturnEvents_Product at INSERT time.

    [f].[OriginalSaleDateKey],
    -- YYYYMMDD INT — stored in the raw 2007–2009 era. No +16 shift at this layer.

    [f].[OriginalSaleDate],
    -- DATE — consistent with OriginalSaleDateKey (same date, display-friendly type).

    [f].[ReturnDateKey],
    -- YYYYMMDD INT — synthesized and clamped to @MaxDate ('2009-12-31').

    [f].[ReturnDate],
    -- DATE — consistent with ReturnDateKey. Clamped by LEAST() in CTE 4 Step B.

    [f].[ReturnLagDays],
    -- INT >= 0 — validated by CHK_PhysicalReturnEvents_ReturnLag at INSERT time.

    [f].[ReturnQuantity],
    -- INT — exact copy from dbo.FactSales.

    [f].[ReturnAmount],
    -- MONEY — exact copy from dbo.FactSales. Used as RefundAmount base.

    [f].[ReturnReasonKey],
    -- FK to gen.DimReturnReason — validated by FK_PhysicalReturnEvents_ReturnReason.
    -- Only 'Both' reasons appear here (filtered in CTE 0 EligibleReasons).

    [f].[IsRefunded],
    -- BIT — derived from value-tiered refund approval threshold in CTE 4 Step E.

    CASE [f].[IsRefunded]
        WHEN 1 THEN [f].[ReturnAmount]
        -- Refund approved: RefundAmount = full ReturnAmount. No partial refunds modelled.
        ELSE        0.00
        -- No refund: RefundAmount stored as MONEY literal 0.00.
    END AS [RefundAmount]
    -- ⚠ BEST PRACTICE — REFUNDAMOUNT DERIVED IN SELECT:
    -- RefundAmount is stored explicitly rather than as a computed column in DDL.
    -- The derivation here ensures the IsRefunded/RefundAmount pair is always
    -- consistent — a refunded row always has a positive RefundAmount, and a
    -- non-refunded row always has 0.00. The CHK_PhysicalReturnEvents_RefundAmount
    -- constraint provides an additional data-quality guarantee at the DB level.
FROM [FinalRows] AS [f];
GO

PRINT '  → [gen].[PhysicalReturnEvents] populated.';
GO

-- ============================================================================
-- STEP 4: Create supporting index
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — STEP 4: NON-CLUSTERED INDEX                             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  NCI on StoreKey: physical return analysis is typically store-centric.  ║
-- ║  "Which stores have the highest return rates?" is the primary COO        ║
-- ║  question for in-store returns. StoreKey is the leading filter column.  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE NONCLUSTERED INDEX [IX_PhysicalReturnEvents_StoreKey]
    ON [gen].[PhysicalReturnEvents] ([StoreKey])
    INCLUDE ([ProductKey], [ReturnReasonKey], [ReturnAmount], [RefundAmount]);
-- StoreKey: primary filter for store-level return rate analysis.
-- INCLUDE: covers common projection columns for covering index benefit.
GO

PRINT '  → Index [IX_PhysicalReturnEvents_StoreKey] created.';
GO


-- ============================================================================
-- STEP 5: Verification queries
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 6 — SET NOEXEC OFF RESET                                     ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  Resets the session execution state so that the verification queries     ║
-- ║  below always run regardless of whether a pre-check fired SET NOEXEC ON. ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- Unconditionally re-enables execution for all subsequent batches.
-- Project-wide rule: SET NOEXEC OFF always appears after the last DML block
-- and before the verification suite to guarantee V1–V5 run on every execution.
GO

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 7 — VERIFICATION QUERIES V1–V5                               ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  PURPOSE                                                                  ║
-- ║  Five verification queries confirm correctness at multiple levels.       ║
-- ║                                                                           ║
-- ║  V1: Cross-channel population summary — physical vs online comparison    ║
-- ║      (physical AvgLag MUST be < online; physical RefundRate MUST be >)   ║
-- ║  V2: Lag distribution by value tier — monotone ascending, shorter ranges ║
-- ║      than Script 07 online equivalents                                   ║
-- ║  V3: Reason distribution — critical: ZERO 'Online Only' reason rows      ║
-- ║  V4: Top stores by return volume — distribution sanity check             ║
-- ║  V5: Referential integrity and data quality — all 7 checks must be 0    ║
-- ║                                                                           ║
-- ║  DETERMINISM NOTE                                                         ║
-- ║  V5 integrity checks are EXACT (must equal 0). V1–V4 are APPROXIMATE —  ║
-- ║  NEWID() noise varies per run. Verify direction of patterns and range    ║
-- ║  boundaries, not exact numeric values.                                   ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ----------------------------------------------------------------------------
-- V1 — POPULATION SUMMARY
-- Compare against dbo.FactSales WHERE ReturnQuantity > 0 AND ReturnAmount > 0.
-- RefundedRows should be ≈ 88–89% of TotalRows.
-- AvgReturnLagDays should be between 5 and 12 (shorter than online ~15–25).
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — POPULATION SUMMARY                                               │
-- │                                                                         │
-- │  KEY COMPARISON WITH SCRIPT 07 (online):                                │
-- │  AvgReturnLagDays MUST be lower here than in gen.OnlineReturnEvents.    │
-- │  RefundRatePct MUST be higher here than in gen.OnlineReturnEvents.      │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V1: Population summary (compare with gen.OnlineReturnEvents)';

SELECT
    'PhysicalReturnEvents'                                           AS [TableName],
    -- Row label: identifies which table this summary row belongs to in the output.
    COUNT(*)                                                         AS [TotalRows],
    -- Total physical return event rows — must equal source FactSales WHERE filters.
    SUM(CAST([IsRefunded] AS INT))                                                AS [RefundedRows],
    -- CAST([IsRefunded] AS INT): converts BIT to INT for SUM() aggregation.
    CAST(SUM(CAST([IsRefunded] AS INT)) * 100.0 / COUNT(*) AS DECIMAL(5,2))      AS [RefundRatePct],
    -- Expected: ~88–89% (higher than online ~85–87% — by design).
    CAST(AVG(CAST([ReturnLagDays] AS FLOAT))  AS DECIMAL(5,1))      AS [AvgReturnLagDays],
    -- Expected: 5–12 days (shorter than online ~15–25 days — by design).
    CAST(SUM([ReturnAmount])   AS DECIMAL(18,2))                     AS [TotalReturnAmount],
    CAST(SUM([RefundAmount])   AS DECIMAL(18,2))                     AS [TotalRefundAmount]
FROM [gen].[PhysicalReturnEvents]

UNION ALL
-- UNION ALL: combines physical and online rows into a single comparison result set.
-- No deduplication needed — the two tables share no rows.

SELECT
    'OnlineReturnEvents',
    -- Online row: provides the benchmark for the two behavioral assertions in V1.
    COUNT(*),
    SUM(CAST([IsRefunded] AS INT)),
    CAST(SUM(CAST([IsRefunded] AS INT)) * 100.0 / COUNT(*) AS DECIMAL(5,2)),
    CAST(AVG(CAST([ReturnLagDays] AS FLOAT))  AS DECIMAL(5,1)),
    CAST(SUM([ReturnAmount])   AS DECIMAL(18,2)),
    CAST(SUM([RefundAmount])   AS DECIMAL(18,2))
FROM [gen].[OnlineReturnEvents];
-- Cross-channel comparison: the two key behavioral differences (shorter lag,
-- higher refund rate for physical) must be visible in this output.
-- If PhysicalAvgLag >= OnlineAvgLag: lag distribution in CTE 4 is incorrect.
-- If PhysicalRefundRate <= OnlineRefundRate: refund thresholds (92/88/83 vs 90/85/80) failed.
GO

-- ----------------------------------------------------------------------------
-- V2 — LAG DISTRIBUTION BY VALUE TIER
-- Physical lags must be strictly shorter than online equivalents.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — PHYSICAL LAG DISTRIBUTION BY VALUE TIER                          │
-- │                                                                         │
-- │  EXPECTED OUTPUT:                                                       │
-- │  ┌───────────┬──────────────┬──────────────┬──────────────┐             │
-- │  │ ValueTier │ AvgLag       │ MinLag       │ MaxLag       │             │
-- │  ├───────────┼──────────────┼──────────────┼──────────────┤             │
-- │  │ 1 (Low)   │ ~3 days      │  1 day       │  5 days      │             │
-- │  │ 2 (Mid)   │ ~7 days      │  3 days      │ 10 days      │             │
-- │  │ 3 (High)  │ ~13 days     │  5 days      │ 21 days      │             │
-- │  └───────────┴──────────────┴──────────────┴──────────────┘             │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V2: Lag distribution by value tier (physical = shorter than online)';

SELECT
    CASE
        WHEN [ReturnAmount] <  50.00 THEN '1 - Low  (< $50)'
        WHEN [ReturnAmount] < 200.00 THEN '2 - Mid  ($50–$199)'
        ELSE                              '3 - High (>= $200)'
    END                                                              AS [ValueTier],
    COUNT(*)                                                         AS [RowCount],
    MIN([ReturnLagDays])                                             AS [MinLagDays],
    CAST(AVG(CAST([ReturnLagDays] AS FLOAT)) AS DECIMAL(5,1))        AS [AvgLagDays],
    MAX([ReturnLagDays])                                             AS [MaxLagDays],
    CAST(AVG([ReturnAmount])                 AS DECIMAL(10,2))       AS [AvgReturnAmount]
FROM [gen].[PhysicalReturnEvents]
GROUP BY
    CASE
        WHEN [ReturnAmount] <  50.00 THEN '1 - Low  (< $50)'
        WHEN [ReturnAmount] < 200.00 THEN '2 - Mid  ($50–$199)'
        ELSE                              '3 - High (>= $200)'
    END
ORDER BY [ValueTier];
GO

-- ----------------------------------------------------------------------------
-- V3 — RETURN REASON DISTRIBUTION
-- Must contain ONLY 'Both' reasons — zero rows with AppliesTo = 'Online Only'.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — REASON DISTRIBUTION (critical: no 'Online Only' reasons)         │
-- │                                                                         │
-- │  EXPECTED OUTPUT:                                                       │
-- │  All rows in AppliesTo column show 'Both'. Zero rows with 'Online Only'.│
-- │  Reason distribution should be roughly uniform across 'Both' reasons.  │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V3: Reason distribution (Online Only reasons must be absent)';

SELECT
    [rdr].[ReturnReasonName],
    [rdr].[AppliesTo],
    COUNT(*)                                                         AS [ReturnCount],
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
         AS DECIMAL(5,2))                                            AS [PctShare]
FROM      [gen].[PhysicalReturnEvents]  AS [pre]
INNER JOIN [gen].[DimReturnReason]      AS [rdr]
    ON [pre].[ReturnReasonKey] = [rdr].[ReturnReasonKey]
GROUP BY [rdr].[ReturnReasonName], [rdr].[AppliesTo]
ORDER BY [ReturnCount] DESC;

-- Explicit check: must return 0 rows with AppliesTo = 'Online Only'.
SELECT COUNT(*) AS [OnlineOnlyReasonCount_MustBeZero]
FROM      [gen].[PhysicalReturnEvents]  AS [pre]
INNER JOIN [gen].[DimReturnReason]      AS [rdr]
    ON [pre].[ReturnReasonKey] = [rdr].[ReturnReasonKey]
WHERE [rdr].[AppliesTo] = 'Online Only';
GO

-- ----------------------------------------------------------------------------
-- V4 — TOP STORES BY RETURN VOLUME AND AMOUNT
-- Business-logic check: validates StoreKey FK integrity and store distribution.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — TOP STORES BY RETURN VOLUME (store-level sanity check)           │
-- │                                                                         │
-- │  EXPECTED OUTPUT: Multiple stores represented. No single store holds   │
-- │  > 30% of all return events (would indicate a StoreKey assignment bug). │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V4: Top stores by return volume (distribution check)';

SELECT TOP 15
    [pre].[StoreKey],
    [ds].[StoreName],
    [ds].[StoreType],
    COUNT(*)                                                         AS [ReturnCount],
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
         AS DECIMAL(5,2))                                            AS [PctOfTotal],
    CAST(SUM([pre].[ReturnAmount])           AS DECIMAL(18,2))       AS [TotalReturnAmount]
FROM      [gen].[PhysicalReturnEvents]  AS [pre]
INNER JOIN [dbo].[DimStore]             AS [ds]
    ON [pre].[StoreKey] = [ds].[StoreKey]
GROUP BY [pre].[StoreKey], [ds].[StoreName], [ds].[StoreType]
ORDER BY [ReturnCount] DESC;
GO

-- ----------------------------------------------------------------------------
-- V5 — REFERENTIAL INTEGRITY & DATA QUALITY
-- All checks must return 0.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V5 — REFERENTIAL INTEGRITY & DATA QUALITY (all 7 checks must be 0)   │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — all zeros):                                   │
-- │  ┌──────────────────────────────────────────────────────┬──────────┐    │
-- │  │ Check                                                │ Expected │    │
-- │  ├──────────────────────────────────────────────────────┼──────────┤    │
-- │  │ Orphan StoreKeys                                     │    0     │    │
-- │  │ Orphan ProductKeys                                   │    0     │    │
-- │  │ Orphan ReturnReasonKeys                              │    0     │    │
-- │  │ Duplicate SourceSalesKey values                      │    0     │    │
-- │  │ ReturnDate after MaxDate (2009-12-31)                │    0     │    │
-- │  │ ReturnLagDays < 0                                    │    0     │    │
-- │  │ RefundAmount mismatch                                │    0     │    │
-- │  └──────────────────────────────────────────────────────┴──────────┘    │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V5: Referential integrity and data quality (all expect 0)';

SELECT 'Orphan StoreKeys' AS [Check],
    COUNT(*) AS [Value]
FROM [gen].[PhysicalReturnEvents] AS [pre]
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[DimStore] AS [ds]
    WHERE [ds].[StoreKey] = [pre].[StoreKey]
)

UNION ALL

SELECT 'Orphan ProductKeys',
    COUNT(*)
FROM [gen].[PhysicalReturnEvents] AS [pre]
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[DimProduct] AS [dp]
    WHERE [dp].[ProductKey] = [pre].[ProductKey]
)

UNION ALL

SELECT 'Orphan ReturnReasonKeys',
    COUNT(*)
FROM [gen].[PhysicalReturnEvents] AS [pre]
WHERE NOT EXISTS (
    SELECT 1 FROM [gen].[DimReturnReason] AS [rdr]
    WHERE [rdr].[ReturnReasonKey] = [pre].[ReturnReasonKey]
)

UNION ALL

SELECT 'Duplicate SourceSalesKey values',
    COUNT(*) - COUNT(DISTINCT [SourceSalesKey])
-- Non-zero means the UNIQUE constraint was violated — indicates pipeline defect.
FROM [gen].[PhysicalReturnEvents]

UNION ALL

SELECT 'ReturnDate after MaxDate (2009-12-31)',
    SUM(CASE WHEN [ReturnDate] > '2009-12-31' THEN 1 ELSE 0 END)
FROM [gen].[PhysicalReturnEvents]

UNION ALL

SELECT 'ReturnLagDays < 0',
    SUM(CASE WHEN [ReturnLagDays] < 0 THEN 1 ELSE 0 END)
-- Physical allows 0 (same-day return) but not negative.
FROM [gen].[PhysicalReturnEvents]

UNION ALL

SELECT 'RefundAmount mismatch (IsRefunded=1 must have > 0)',
    SUM(CASE
            WHEN [IsRefunded] = 1 AND [RefundAmount] = 0 THEN 1
            WHEN [IsRefunded] = 0 AND [RefundAmount] > 0 THEN 1
            ELSE 0
        END)
FROM [gen].[PhysicalReturnEvents];
GO


PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 08 completed successfully.';
PRINT '  Table created:  [gen].[PhysicalReturnEvents]';
PRINT '  Index created:  [IX_PhysicalReturnEvents_StoreKey]';
PRINT '';
PRINT '  Verify before proceeding:';
PRINT '    ✓ gen.OnlineReturnEvents populated   (Script 07)';
PRINT '    ✓ gen.PhysicalReturnEvents populated  (Script 08 — this script)';
PRINT '';
PRINT '  Next steps (both 07 + 08 must be complete):';
PRINT '    Script 09 → fact.vReturns   (UNION ALL over both return tables)';
PRINT '    Script 09 → dim.vReturnReason (semantic view over gen.DimReturnReason)';
PRINT '════════════════════════════════════════════════════════════════';
GO
