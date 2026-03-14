/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║      SCRIPT 07: gen.OnlineReturnEvents — ONLINE CHANNEL RETURN EVENTS       ║
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
  This script generates gen.OnlineReturnEvents — one row per return-bearing
  online sales line. Each row in dbo.FactOnlineSales where ReturnQuantity > 0
  produces exactly one return event, enriched with a synthesized return date,
  an assigned return reason, and a refund outcome.

  The Contoso source embeds ReturnQuantity and ReturnAmount directly inside
  dbo.FactOnlineSales but records ZERO context about why, when, or how the
  return was processed. Without this table, these KPIs remain dark:

    ┌───────────────────────────────────────────────────────────────────────┐
    │  BUILT-IN CORRELATIONS (discoverable by students)                    │
    ├───────────────────────────────────────────────────────────────────────┤
    │  High ReturnAmount  →  longer return lag (deliberation effect)        │
    │  High ReturnAmount  →  slightly lower refund approval rate           │
    │  Low ReturnAmount   →  faster return decision, near-certain refund   │
    │  Return volume      →  tracks sales density (peak months → more      │
    │                         returns) — temporal coherence with source    │
    │  Return reasons     →  dynamically assigned from gen.DimReturnReason │
    │                         respecting channel eligibility (AppliesTo)   │
    └───────────────────────────────────────────────────────────────────────┘

  Students analysing return lag vs. product value WILL find that higher-value
  items take longer to be returned. This is engineered, not coincidental.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Business Questions Unlocked                                            │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  CSO:  What is the online return rate by product category?              │
  │  CSO:  How does return lag vary by product value tier?                  │
  │  COO:  What are the top return reasons for online orders?               │
  │  COO:  What % of online returns result in a refund?                     │
  │  CFO:  What is the total refund liability from online returns?          │
  │  CFO:  How does refund rate vary by return reason?                      │
  │  CMO:  Do high-return customers have lower CLV?                         │
  │  PM:   Which product subcategories have the highest return rates?       │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  GRAIN AND SCOPE
--------------------------------------------------------------------------------
  Grain   : One row per online return event = one row per dbo.FactOnlineSales
            line where ReturnQuantity > 0 AND ReturnAmount > 0.
  Scope   : All online channel return lines in dbo.FactOnlineSales.
            Physical store returns are in Script 08 (gen.PhysicalReturnEvents).
  Key     : (SalesOrderNumber, SalesOrderLineNumber) — unique per source line.
            UNIQUE constraint enforces this at the database layer.

  ⚠  RETURN AMOUNT FILTER
  Rows where ReturnQuantity > 0 but ReturnAmount = 0 are excluded. These are
  data quality artefacts in the Contoso source (quantity recorded but no
  monetary impact). Including them would produce zero-value return events
  that distort refund rate and average return value calculations.

  ⚠  TEMPORAL SCOPE
  ReturnDate is synthesized relative to OriginalSaleDate using a value-tiered
  lag model. All dates are stored in the raw source range (2007–2009). The
  +16 year offset to 2023–2025 is applied EXCLUSIVELY at the fact.vReturns
  view layer, consistent with the project-wide temporal shift principle.

--------------------------------------------------------------------------------
  LAG MODEL — DESIGN RATIONALE
--------------------------------------------------------------------------------
  ┌────────────────────────────────────────────────────────────────────────────┐
  │  ValueTier  │  ReturnAmount     │  ReturnLag Range  │  Behavioral Logic    │
  ├────────────────────────────────────────────────────────────────────────────┤
  │  1 – Low    │  < $50            │  7 – 14 days      │  Impulse return:     │
  │             │                   │                   │  quick decision      │
  ├────────────────────────────────────────────────────────────────────────────┤
  │  2 – Mid    │  $50 – $199.99    │  14 – 21 days     │  Considered return:  │
  │             │                   │                   │  typical window      │
  ├────────────────────────────────────────────────────────────────────────────┤
  │  3 – High   │  ≥ $200           │  21 – 45 days     │  Deliberate return:  │
  │             │                   │                   │  high-value items    │
  │             │                   │                   │  are scrutinised     │
  └────────────────────────────────────────────────────────────────────────────┘
  All ReturnDates are clamped to @MaxDate ('2009-12-31') to prevent dates
  from escaping the project's temporal boundary.

--------------------------------------------------------------------------------
  REFUND RATE — DESIGN RATIONALE
--------------------------------------------------------------------------------
  Refund approval rates are value-tiered (higher scrutiny on large refunds):
    Low value  (< $50):    90% approval — standard no-questions-asked refund
    Mid value  ($50–$200): 85% approval — standard policy
    High value (≥ $200):   80% approval — additional review may deny refund
  Overall rate: ~85% — consistent with documented e-commerce refund rates.

--------------------------------------------------------------------------------
  RETURN REASON ASSIGNMENT
--------------------------------------------------------------------------------
  Reasons are drawn dynamically from gen.DimReturnReason where
  AppliesTo IN ('Both', 'Online Only'). No reason keys are hardcoded.
  Selection uses a deterministic modulo index on a locked noise seed —
  identical to the CROSS APPLY materialisation pattern in Script 06.

--------------------------------------------------------------------------------
  OUTPUT TABLE — gen.OnlineReturnEvents
--------------------------------------------------------------------------------
  Column                Type                 Notes
  ──────────────────────────────────────────────────────────────────────────────
  OnlineReturnEventID   INT IDENTITY PK      Auto surrogate — resets on re-run
  SalesOrderNumber      NVARCHAR(20) NOT NULL Degenerate dim → FactOnlineSales
  SalesOrderLineNumber  INT NOT NULL          Degenerate dim → source line
  CustomerKey           INT NOT NULL FK       → dbo.DimCustomer
  ProductKey            INT NOT NULL FK       → dbo.DimProduct
  OriginalSaleDateKey   INT NOT NULL          YYYYMMDD — raw source (no +16)
  OriginalSaleDate      DATE NOT NULL         Actual sale date from source
  ReturnDateKey         INT NOT NULL          YYYYMMDD — synthesized (no +16)
  ReturnDate            DATE NOT NULL         Synthesized return processing date
  ReturnLagDays         INT NOT NULL          Days between sale and return
  ReturnQuantity        INT NOT NULL          From dbo.FactOnlineSales source
  ReturnAmount          DECIMAL(19,4) NOT NULL        From dbo.FactOnlineSales source
  ReturnReasonKey       INT NOT NULL FK       → gen.DimReturnReason
  IsRefunded            BIT NOT NULL          1 = refund approved and issued
  RefundAmount          DECIMAL(19,4) NOT NULL        = ReturnAmount if refunded, else 0

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run order     : Script 07 — can run after Scripts 00 + 01 only
  Parallel with : Script 08 (gen.PhysicalReturnEvents) — no shared dependency
  Dependencies  : [gen] schema (Script 00), gen.DimReturnReason (Script 01),
                  dbo.FactOnlineSales, dbo.DimCustomer, dbo.DimProduct
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
-- ║  (2) dbo.FactOnlineSales      → Contoso source required (return data)    ║
-- ║  (3) dbo.DimCustomer          → Required for CustomerKey FK validation   ║
-- ║  (4) dbo.DimProduct           → Required for ProductKey FK validation    ║
-- ║  (5) gen.DimReturnReason      → Script 01 required (return reason FK)    ║
-- ║                                                                           ║
-- ║  EXPECTED OUTPUT ON SUCCESS (5 green ticks in Messages tab):             ║
-- ║  ✓ [gen] schema confirmed.                                               ║
-- ║  ✓ [dbo].[FactOnlineSales] confirmed.                                    ║
-- ║  ✓ [dbo].[DimCustomer] confirmed.                                        ║
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

-- ── CHECK 2 OF 5: dbo.FactOnlineSales ────────────────────────────────────────

IF OBJECT_ID('[dbo].[FactOnlineSales]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[FactOnlineSales] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    ;
END
ELSE
BEGIN
    PRINT '✓ [dbo].[FactOnlineSales] confirmed.';
END
GO

-- ── CHECK 3 OF 5: dbo.DimCustomer ────────────────────────────────────────────

IF OBJECT_ID('[dbo].[DimCustomer]', 'U') IS NULL
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [dbo].[DimCustomer] not found. Ensure ContosoRetailDW database is selected and source tables are present.');
    THROW 50000, @ErrorMessage, 1;
    ;
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimCustomer] confirmed.';
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
-- ║  Drops (if exists) and recreates gen.OnlineReturnEvents — a 15-column    ║
-- ║  return event table where one row = one return-bearing online sales line. ║
-- ║                                                                           ║
-- ║  TABLE DESIGN DECISIONS                                                   ║
-- ║                                                                           ║
-- ║  DUAL DATE KEYS (OriginalSaleDateKey + ReturnDateKey)                     ║
-- ║  Both date keys are stored as INT (YYYYMMDD) to enable role-playing       ║
-- ║  date relationships in Power BI. OriginalSaleDateKey will be the ACTIVE  ║
-- ║  relationship to dim.vDate; ReturnDateKey will be INACTIVE and activated ║
-- ║  via USERELATIONSHIP() in DAX to analyse return timing.                  ║
-- ║  The lag between them (ReturnLagDays) enables return window analytics.   ║
-- ║                                                                           ║
-- ║  DEGENERATE DIMENSIONS (SalesOrderNumber + SalesOrderLineNumber)          ║
-- ║  These are not FK columns — they are natural keys from the source that   ║
-- ║  allow drill-through joins back to fact.vOnlineSales without a formal    ║
-- ║  model relationship (following the same TREATAS-based cross-fact         ║
-- ║  pattern used for SalesOrderNumber in fact.vOrderFulfillment).           ║
-- ║                                                                           ║
-- ║  GRAIN ENFORCEMENT — UNIQUE ON (SalesOrderNumber, SalesOrderLineNumber)  ║
-- ║  Each FactOnlineSales line generates at most ONE return event row.       ║
-- ║  This constraint prevents pipeline bugs from creating duplicate return   ║
-- ║  events for the same source line.                                        ║
-- ║                                                                           ║
-- ║  RefundAmount — STORED, NOT COMPUTED                                      ║
-- ║  RefundAmount is derivable from (IsRefunded × ReturnAmount) but is       ║
-- ║  stored explicitly — identical rationale to WouldRecommend in Script 06. ║
-- ║  Direct additive SUM in Power BI without requiring a DAX expression.     ║
-- ║                                                                           ║
-- ║  ReturnLagDays — CHECK CONSTRAINT >= 1                                    ║
-- ║  Online returns require at least 1 day after purchase (delivery must     ║
-- ║  occur before a return can be initiated). This is enforced at the DB.    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


DROP TABLE IF EXISTS [gen].[OnlineReturnEvents];
PRINT '✓ Existing [gen].[OnlineReturnEvents] table dropped (if it existed).';
GO

CREATE TABLE [gen].[OnlineReturnEvents] (

    -- ── Surrogate Primary Key ─────────────────────────────────────────────
    [OnlineReturnEventID]    INT           IDENTITY(1,1) NOT NULL,
    -- IDENTITY PK: sequentially assigned on each run. Resets on re-run by
    -- design — this is a generated table, not a stable operational system.

    -- ── Degenerate Dimensions (source traceability) ───────────────────────
    [SalesOrderNumber]       NVARCHAR(20)               NOT NULL,
    -- Natural key from dbo.FactOnlineSales. Enables cross-fact drill-through
    -- to fact.vOnlineSales and fact.vOrderFulfillment via TREATAS in DAX.

    [SalesOrderLineNumber]   INT                        NOT NULL,
    -- Line-level identifier within the order. Together with SalesOrderNumber,
    -- uniquely identifies the source row in dbo.FactOnlineSales.

    -- ── Dimensional Foreign Keys ──────────────────────────────────────────
    [CustomerKey]            INT                        NOT NULL,
    -- FK to dbo.DimCustomer. Enables customer-level return rate analysis
    -- and correlation with CLV, satisfaction scores, and acquisition channel.

    [ProductKey]             INT                        NOT NULL,
    -- FK to dbo.DimProduct. Enables product-level return rate analysis,
    -- return reasons by subcategory, and margin impact of returns per SKU.

    -- ── Temporal Keys (dual date design for role-playing) ─────────────────
    [OriginalSaleDateKey]    INT                        NOT NULL,
    -- YYYYMMDD integer — the date the original sale was recorded.
    -- Sourced directly from dbo.FactOnlineSales.DateKey (no transformation).
    -- ACTIVE relationship to dim.vDate in the Power BI model.
    -- Used as the primary date axis for return cohort analysis.

    [OriginalSaleDate]       DATE                       NOT NULL,
    -- DATE equivalent of OriginalSaleDateKey. Display column — avoids
    -- joining back to dim.vDate for simple date label requirements.
    -- Also used as the base date for ReturnLagDays calculation.

    [ReturnDateKey]          INT                        NOT NULL,
    -- YYYYMMDD integer — the synthesized return processing date.
    -- INACTIVE relationship to dim.vDate. Activated in DAX via
    -- USERELATIONSHIP(fact.vReturns[ReturnDateKey], dim.vDate[DateKey]).

    [ReturnDate]             DATE                       NOT NULL,
    -- DATE equivalent of ReturnDateKey. Synthesized: OriginalSaleDate +
    -- value-tiered lag, clamped to @MaxDate ('2009-12-31').

    -- ── Return Timing Metric ──────────────────────────────────────────────
    [ReturnLagDays]          INT                        NOT NULL,
    -- Days between OriginalSaleDate and ReturnDate. The primary KPI for
    -- return window analysis. Value-tiered: 7–14 (low), 14–21 (mid),
    -- 21–45 (high). Enforced >= 1 by CHECK constraint.

    -- ── Source Measures (copied from dbo.FactOnlineSales) ─────────────────
    [ReturnQuantity]         INT                        NOT NULL,
    -- Units returned — exact copy from dbo.FactOnlineSales.ReturnQuantity.
    -- Retained in this table to allow return rate denominators without
    -- requiring a join back to the 13M-row source fact.

    [ReturnAmount]           DECIMAL(19,4)                      NOT NULL,
    -- Monetary value of returned goods — exact copy from source.
    -- Baseline for refund liability calculations.

    -- ── Return Classification ─────────────────────────────────────────────
    [ReturnReasonKey]        INT                        NOT NULL,
    -- FK to gen.DimReturnReason. Dynamically assigned from reasons where
    -- AppliesTo IN ('Both', 'Online Only'). No reason keys hardcoded.

    -- ── Refund Outcome ────────────────────────────────────────────────────
    [IsRefunded]             BIT                        NOT NULL,
    -- 1 = refund approved and issued. 0 = return accepted without monetary
    -- refund (exchange, store credit, or denial after review).
    -- Value-tiered approval: Low 90%, Mid 85%, High 80%.

    [RefundAmount]           DECIMAL(19,4)                      NOT NULL,
    -- Monetary refund issued: = ReturnAmount when IsRefunded = 1, else 0.
    -- Stored explicitly (not computed) for direct SUM usage in Power BI.
    -- Avoids a DAX calculated column at the report layer.

    -- ── Constraints ───────────────────────────────────────────────────────
    CONSTRAINT [PK_OnlineReturnEvents]
        PRIMARY KEY CLUSTERED ([OnlineReturnEventID]),
    -- CLUSTERED PK on IDENTITY: optimal for append-heavy generation workload.
    -- Sequential inserts produce no page splits during the INSERT.

    CONSTRAINT [UQ_OnlineReturnEvents_OrderLine]
        UNIQUE ([SalesOrderNumber], [SalesOrderLineNumber]),
    -- ⚠ BEST PRACTICE — GRAIN ENFORCEMENT AT THE DATABASE LEVEL:
    -- One return event per source line. Prevents duplicates from a pipeline
    -- re-run or a logic error in the CTE that produces multiple rows per line.

    CONSTRAINT [FK_OnlineReturnEvents_Customer]
        FOREIGN KEY ([CustomerKey])
        REFERENCES [dbo].[DimCustomer] ([CustomerKey]),
    -- Ensures every return event belongs to a valid customer in the dimension.

    CONSTRAINT [FK_OnlineReturnEvents_Product]
        FOREIGN KEY ([ProductKey])
        REFERENCES [dbo].[DimProduct] ([ProductKey]),
    -- Ensures every return event references a valid product.

    CONSTRAINT [FK_OnlineReturnEvents_ReturnReason]
        FOREIGN KEY ([ReturnReasonKey])
        REFERENCES [gen].[DimReturnReason] ([ReturnReasonKey]),
    -- Ensures the assigned return reason exists in the dimension.
    -- The dynamic modulo-selection logic guarantees this, but the FK
    -- enforces it at the DB layer as a safety net.

    CONSTRAINT [CHK_OnlineReturnEvents_ReturnLag]
        CHECK ([ReturnLagDays] >= 1),
    -- Minimum 1-day lag: an online return cannot be initiated before delivery.

    CONSTRAINT [CHK_OnlineReturnEvents_ReturnAmount]
        CHECK ([ReturnAmount] >= 0),
    -- Pre-filter (WHERE ReturnAmount > 0) ensures this, but the constraint
    -- provides a permanent data-quality guarantee for future inserts.

    CONSTRAINT [CHK_OnlineReturnEvents_RefundAmount]
        CHECK ([RefundAmount] >= 0)
    -- Ensures no negative refund amounts enter the table.
);
GO

PRINT '  → [gen].[OnlineReturnEvents] table created.';
GO


-- ============================================================================
-- STEP 2: Declare reference constant — anchors all temporal calculations
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2: PRE-CTE VARIABLE DECLARATION                    ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Declares @MaxDate ONCE before the CTE chain begins.                     ║
-- ║                                                                           ║
-- ║  ⚠ BEST PRACTICE — PRE-CTE VARIABLE FOR DATE CLAMPING (Script 03 pattern):║
-- ║  @MaxDate is referenced in DATEADD() expressions within the CTE to       ║
-- ║  clamp ReturnDate to the project boundary. Declaring it before the CTE   ║
-- ║  as a scalar variable allows the engine to substitute it at compile      ║
-- ║  time — avoiding a correlated subquery or window function spool.         ║
-- ║                                                                           ║
-- ║  '2009-12-31' is the project-standard fixed reference date. GETDATE()   ║
-- ║  is NEVER used — it would make ReturnDate non-reproducible across runs   ║
-- ║  and break temporal integrity on a historical dataset.                   ║
-- ║                                                                           ║
-- ║  The +16 year shift to '2025-12-31' is applied ONLY at the              ║
-- ║  fact.vReturns semantic view layer.                                      ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

DECLARE @MaxDate DATE = '2009-12-31';
-- Project-standard fixed reference date: the end of the Contoso DW source range.
-- Used to clamp ReturnDate for late-year sales (e.g., a high-value order placed
-- in December 2009 with a 45-day lag would overshoot 2009-12-31 without this cap).


-- ============================================================================
-- STEP 3: Populate via 4-stage behavioral CTE pipeline
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — STEP 3: 4-STAGE CTE PIPELINE + INSERT                   ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Generates all return event rows via a 4-CTE pipeline that enriches      ║
-- ║  each return-bearing FactOnlineSales line with timing, reason, and       ║
-- ║  refund outcome — all behaviorally calibrated to ReturnAmount.           ║
-- ║                                                                           ║
-- ║  ┌─────────────────────────────────────────────────────────────────────┐ ║
-- ║  │  PIPELINE OVERVIEW                                                  │ ║
-- ║  ├─────────────────────────────────────────────────────────────────────┤ ║
-- ║  │  CTE 1: SourceLines                                                 │ ║
-- ║  │    Filters dbo.FactOnlineSales to return-bearing rows only.         │ ║
-- ║  │    Produces: all FK keys, date, ReturnQuantity, ReturnAmount.       │ ║
-- ║  │                                                                     │ ║
-- ║  │  CTE 2: ValueTiered                                                 │ ║
-- ║  │    Assigns ValueTier (1/2/3) based on ReturnAmount buckets.         │ ║
-- ║  │    Drives lag distribution and refund approval rate downstream.     │ ║
-- ║  │                                                                     │ ║
-- ║  │  CTE 3: NoiseLocked                                                 │ ║
-- ║  │    Materialises two independent random draws per row via           │ ║
-- ║  │    CROSS APPLY (Script 06 pattern). NoiseA controls lag and        │ ║
-- ║  │    refund decision; NoiseB selects the return reason. Locking      │ ║
-- ║  │    here ensures both draws are evaluated exactly once per row.     │ ║
-- ║  │                                                                     │ ║
-- ║  │  CTE 4: FinalRows                                                   │ ║
-- ║  │    Derives ReturnLagDays, ReturnDate, ReturnDateKey, ReturnReason,  │ ║
-- ║  │    IsRefunded, and RefundAmount from the locked noise values.      │ ║
-- ║  └─────────────────────────────────────────────────────────────────────┘ ║
-- ║                                                                           ║
-- ║  ⚠  CROSS APPLY NOISE LOCKING (Script 06 pattern):                       ║
-- ║  NEWID() is non-deterministic and may be re-evaluated across CTE        ║
-- ║  references. The CROSS APPLY (SELECT ABS(CHECKSUM(NEWID()))) pattern   ║
-- ║  in CTE 3 materialises the value once per row. Downstream CTEs that    ║
-- ║  reference [nl].[NoiseA] and [nl].[NoiseB] use the locked values —     ║
-- ║  no additional NEWID() calls are made after CTE 3.                     ║
-- ║                                                                           ║
-- ║  ⚠  DYNAMIC REASON SELECTION (no hardcoded keys):                        ║
-- ║  ReturnReasonKey is selected via a modulo index into a ranked subset    ║
-- ║  of gen.DimReturnReason filtered by AppliesTo IN ('Both','Online Only').║
-- ║  This design is resilient to future additions to DimReturnReason — no  ║
-- ║  code change is needed if new reasons are added to the dimension.       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

;WITH
-- Leading semicolon: defensive guard — prevents a syntax error if a prior
-- batch statement was not terminated before this WITH clause begins.

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 0: EligibleReasons (Performance Fix)
-- Pre-calculates the modulo index for return reasons ONCE, preventing a massive
-- N+1 Table Spool in the FinalRows CROSS APPLY.
-- ────────────────────────────────────────────────────────────────────────────
[EligibleReasons] AS (
    SELECT
        [rdr].[ReturnReasonKey],
        ROW_NUMBER() OVER (ORDER BY [rdr].[ReturnReasonKey]) - 1 AS [RN],
        -- ⚠ BEST PRACTICE — 0-BASED MODULO INDEX:
        -- ROW_NUMBER() is 1-indexed by default. Subtracting 1 produces a 0-based rank
        -- (0, 1, 2 ... N-1). This aligns with the modulo expression in FinalRows:
        --   [nl].[NoiseB] % [er].[TotalEligible]
        -- which returns values in the range 0 to N-1, ensuring every reason
        -- is reachable. A 1-based RN would skip the first reason (modulo never = 1
        -- when the range starts at 1 and modulus equals N).
        COUNT(*) OVER ()                                          AS [TotalEligible]
        -- COUNT(*) OVER () with no PARTITION BY: counts ALL rows in this CTE result set.
        -- This is the dynamic denominator for the modulo selection.
        -- If new reasons are added to gen.DimReturnReason, TotalEligible updates
        -- automatically — no hardcoded key counts need to change in this script.
    FROM [gen].[DimReturnReason] AS [rdr]
    WHERE [rdr].[AppliesTo] IN ('Both', 'Online Only')
    -- ⚠ BEST PRACTICE — CHANNEL-AWARE REASON FILTERING:
    -- Online returns may receive reasons from either 'Both' or 'Online Only' categories.
    -- 'Online Only' reasons (e.g., Late Delivery) are applicable because this is the
    -- online channel. Physical returns (Script 08) use AppliesTo = 'Both' only.
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 1: SourceLines
-- Extracts all return-bearing lines from dbo.FactOnlineSales.
-- ────────────────────────────────────────────────────────────────────────────
[SourceLines] AS (
    SELECT
        [fos].[SalesOrderNumber],
        -- Degenerate dimension: the natural order identifier from the Contoso source.
        -- Carried into the output table to enable TREATAS drill-through in DAX
        -- from fact.vReturns back to fact.vOnlineSales without a formal model relationship.

        [fos].[SalesOrderLineNumber],
        -- Line-level identifier within the order. Together with SalesOrderNumber,
        -- uniquely identifies a single row in dbo.FactOnlineSales — this pair
        -- forms the compound grain key enforced by UQ_OnlineReturnEvents_OrderLine.

        [fos].[CustomerKey],
        -- FK to dbo.DimCustomer — sourced directly, no transformation needed.
        -- INT data type matches DimCustomer.CustomerKey — no implicit type conversion on join.

        [fos].[ProductKey],
        -- FK to dbo.DimProduct — sourced directly.
        -- Enables product-level return rate analysis (which SKUs are returned most?).

        (YEAR([fos].[DateKey]) * 10000) + 
        (MONTH([fos].[DateKey]) * 100) + 
        DAY([fos].[DateKey])                          AS [OriginalSaleDateKey],
        -- ⚠ BEST PRACTICE — PROJECT-STANDARD INT DATE KEY DERIVATION:
        -- DateKey in dbo.FactOnlineSales is stored as DATETIME (legacy Contoso type).
        -- CONVERT(VARCHAR(8), ..., 112): format code 112 = 'YYYYMMDD' — produces
        --   the 8-character string representation of the date portion only.
        -- CAST(... AS INT): converts the 8-char string to a YYYYMMDD integer.
        -- This two-step CAST + CONVERT is the project-standard pattern for all
        -- date key derivations — consistent with Scripts 02–06.

        CAST([fos].[DateKey] AS DATE)                 AS [OriginalSaleDate],
        -- CAST to DATE: strips the time component from the DATETIME DateKey.
        -- DATE is used here (not DATETIME) to: (a) enable clean DATEDIFF calculations
        -- for lag derivation, and (b) match the DATE type of the target column.

        [fos].[ReturnQuantity],
        -- Units returned — exact copy from source. Pre-filtered to > 0 by the WHERE below.
        -- Retained so the output table supports per-unit analysis without a join back
        -- to the 13M-row source fact table.

        [fos].[ReturnAmount]
        -- Monetary value of returned goods — exact copy from source.
        -- Pre-filtered to > 0 below to exclude data quality artefacts.

    FROM  [dbo].[FactOnlineSales] AS [fos]
    WHERE [fos].[ReturnQuantity] > 0
    -- Primary filter: only rows that represent an actual return event.
    -- dbo.FactOnlineSales contains both forward sales and embedded return signals
    -- in the same rows; this filter isolates the return-bearing subset.
      AND [fos].[ReturnAmount]   > 0
    -- Secondary quality filter: excludes rows where ReturnQuantity > 0 but
    -- ReturnAmount = 0.00 — a known data quality artefact in the Contoso source.
    -- Including them would create zero-value return events that distort:
    --   (1) average return value KPIs, (2) refund rate denominators.
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 2: ValueTiered
-- Classifies each return by monetary value into one of three tiers.
-- ────────────────────────────────────────────────────────────────────────────
[ValueTiered] AS (
    SELECT
        [sl].[SalesOrderNumber],
        [sl].[SalesOrderLineNumber],
        [sl].[CustomerKey],
        [sl].[ProductKey],
        [sl].[OriginalSaleDateKey],
        [sl].[OriginalSaleDate],
        [sl].[ReturnQuantity],
        [sl].[ReturnAmount],
        -- Pass-throughs from CTE 1: all upstream columns preserved unchanged.

        CASE
            WHEN [sl].[ReturnAmount] <   50.00 THEN 1
            -- Tier 1 — Low: impulse returns; customer made a quick decision to return.
            -- Lag range: 7–14 days. Refund approval: 90%.
            WHEN [sl].[ReturnAmount] <  200.00 THEN 2
            -- Tier 2 — Mid: considered returns; customer evaluated the product at home.
            -- Lag range: 14–21 days. Refund approval: 85%.
            ELSE                                    3
            -- Tier 3 — High (≥ $200): deliberate returns; high-value items face more scrutiny.
            -- Lag range: 21–45 days. Refund approval: 80%.
        END AS [ValueTier]
        -- ValueTier is an INTEGER (1/2/3) — not a string — so downstream CASE expressions
        -- use CASE [nl].[ValueTier] WHEN 1 THEN ... (simple CASE, not searched CASE).
        -- Simple CASE is more readable and marginally faster for short lookup lists.
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
        -- Separate seed prevents spurious correlation between how long the customer
        -- waited to return and whether they received a refund.
    FROM [ValueTiered] AS [vt]
    CROSS APPLY (
    -- ⚠ BEST PRACTICE — CROSS APPLY NOISE LOCKING (Script 06 pattern):
    -- NEWID() is non-deterministic. In a CTE, SQL Server MAY re-evaluate NEWID()
    -- each time the CTE is referenced, producing a different value per reference.
    -- Placing all three NEWID() calls inside a CROSS APPLY SELECT forces the engine
    -- to materialise all three values exactly ONCE per row at this stage.
    -- All downstream CTEs reference [nl].[NoiseA/B/C] — locked values, not live NEWID().
    -- This eliminates hidden correlations: lag, reason, and refund decisions are
    -- independently randomised but each row's three values are internally consistent.
        SELECT
            ABS(CHECKSUM(NEWID())) AS [NoiseA],
            -- NEWID(): generates a globally unique UUID (type UNIQUEIDENTIFIER).
            -- CHECKSUM(): hashes the UUID into a signed 32-bit INT (can be negative).
            -- ABS(): returns the absolute value — guaranteed non-negative integer.
            -- The result is a uniformly distributed non-negative integer for each row.
            ABS(CHECKSUM(NEWID())) AS [NoiseB],
            -- Second independent NEWID() call — produces a value uncorrelated with NoiseA.
            ABS(CHECKSUM(NEWID())) AS [NoiseC]
            -- Third independent NEWID() call — uncorrelated with both NoiseA and NoiseB.
    ) AS [n]
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 4: FinalRows
-- Derives all output columns from the locked noise values using LEAST() 
-- for clean temporal clamping.
-- ────────────────────────────────────────────────────────────────────────────
[FinalRows] AS (
    SELECT
        [nl].[SalesOrderNumber],
        [nl].[SalesOrderLineNumber],
        [nl].[CustomerKey],
        [nl].[ProductKey],
        [nl].[OriginalSaleDateKey],
        [nl].[OriginalSaleDate],
        [nl].[ReturnQuantity],
        [nl].[ReturnAmount],
        -- Pass-throughs from NoiseLocked: all source columns preserved.

        -- ── Step A: ReturnLagDays ─────────────────────────────────────────
        CASE [nl].[ValueTier]
            WHEN 1 THEN  7 + ([nl].[NoiseA] % 8)
            -- Tier 1 Low: base 7 days + uniform noise 0–7 → range 7–14 days.
            -- % 8 produces values 0,1,2,3,4,5,6,7 — exactly an 8-element range.
            WHEN 2 THEN 14 + ([nl].[NoiseA] % 8)
            -- Tier 2 Mid: base 14 days + noise 0–7 → range 14–21 days.
            ELSE        21 + ([nl].[NoiseA] % 25)
            -- Tier 3 High: base 21 days + noise 0–24 → range 21–45 days.
            -- Wider noise range (% 25 = 0–24) reflects higher variability in
            -- deliberate high-value return decisions.
        END AS [ReturnLagDays],

        -- ── Step B: ReturnDate — cleanly clamped to @MaxDate ──────────────
        LEAST(
        -- ⚠ BEST PRACTICE — LEAST() FOR TEMPORAL CLAMPING (SQL Server 2022+):
        -- LEAST() is a SQL Server 2022 function that returns the minimum of N values.
        -- Used here instead of CASE WHEN ... > @MaxDate THEN @MaxDate ELSE ... END —
        -- the LEAST() form is more readable and slightly more compact.
        -- Clamp is required: a high-value order placed 2009-12-01 + 45 days lag
        -- would produce 2010-01-15, which escapes the project's temporal boundary.
            DATEADD(
                DAY,
                CASE [nl].[ValueTier]
                    WHEN 1 THEN  7 + ([nl].[NoiseA] % 8)
                    WHEN 2 THEN 14 + ([nl].[NoiseA] % 8)
                    ELSE        21 + ([nl].[NoiseA] % 25)
                END,
                -- Same lag formula as Step A — DATEADD adds the lag days to OriginalSaleDate.
                [nl].[OriginalSaleDate]
                -- Base date: the actual sale date (DATE type after CTE 1 CAST).
            ),
            @MaxDate
            -- Upper bound: project-standard fixed reference date '2009-12-31'.
            -- Declared before the CTE chain as a scalar variable (not computed inline).
        ) AS [ReturnDate],

        -- ── Step C: ReturnDateKey — YYYYMMDD integer ──────────────────────
        CAST(CONVERT(VARCHAR(8),
        -- Project-standard DATE → YYYYMMDD INT conversion applied to ReturnDate.
        -- Same two-step CAST + CONVERT(112) pattern as OriginalSaleDateKey in CTE 1.
            LEAST(
                DATEADD(
                    DAY,
                    CASE [nl].[ValueTier]
                        WHEN 1 THEN  7 + ([nl].[NoiseA] % 8)
                        WHEN 2 THEN 14 + ([nl].[NoiseA] % 8)
                        ELSE        21 + ([nl].[NoiseA] % 25)
                    END,
                    [nl].[OriginalSaleDate]
                ),
                @MaxDate
                -- Clamp applied to ReturnDate before converting to INT key.
                -- Ensures the key is consistent with the ReturnDate DATE value.
            ), 112
        -- Format 112 = YYYYMMDD (no separators): matches the INT date key convention.
        ) AS INT) AS [ReturnDateKey],

        -- ── Step D: ReturnReasonKey — dynamic modulo selection ────────────
        [rr].[ReturnReasonKey],
        -- Sourced from the CROSS APPLY (EligibleReasons) below — not computed here.
        -- The modulo expression ([nl].[NoiseB] % [er].[TotalEligible]) in the
        -- CROSS APPLY maps the locked NoiseB value to a specific RN in EligibleReasons,
        -- which in turn identifies the ReturnReasonKey to assign.

        -- ── Step E: IsRefunded — value-tiered approval rate ───────────────
        CAST(CASE
            WHEN ([nl].[NoiseC] % 100) <
            -- (NoiseC % 100): produces a uniform integer in the range 0–99.
            -- Comparing to a threshold T: rows where value < T → IsRefunded = 1.
            -- Threshold = 90 → 90% of rows pass (receive a refund).
            -- This is equivalent to: RAND() < 0.90 but deterministic per locked NoiseC.
                 CASE [nl].[ValueTier]
                     WHEN 1 THEN 90
                     -- Low-value return: 90% approval rate — near-certain refund.
                     WHEN 2 THEN 85
                     -- Mid-value return: 85% approval rate — standard policy.
                     ELSE        80
                     -- High-value return: 80% approval — additional review may deny.
                 END
            THEN 1
            -- Refund approved: IsRefunded = 1. RefundAmount = ReturnAmount in INSERT.
            ELSE 0
            -- Refund denied: IsRefunded = 0. RefundAmount = 0.00 in INSERT.
        END AS BIT) AS [IsRefunded]
        -- CAST AS BIT: converts the 0/1 integer result to a 1-byte BIT type.
        -- BIT is the correct storage type for boolean flags — avoids INT overhead.

    FROM [NoiseLocked] AS [nl]

    -- ── CROSS APPLY: Dynamic ReturnReason selection (using CTE 0) ─────────
    CROSS APPLY (
    -- CROSS APPLY with a correlated subquery against EligibleReasons (CTE 0).
    -- For each row in NoiseLocked, this finds exactly ONE matching ReturnReasonKey
    -- where the pre-computed RN matches the NoiseB modulo result.
    -- Because CTE 0 is pre-computed, this avoids re-scanning DimReturnReason
    -- for every row — eliminating the Table Spool anti-pattern.
        SELECT [er].[ReturnReasonKey]
        FROM [EligibleReasons] AS [er]
        WHERE [er].[RN] = ([nl].[NoiseB] % [er].[TotalEligible])
        -- [nl].[NoiseB] % [er].[TotalEligible]: maps the locked noise value to a 0-based
        -- index within the eligible reason set. Because TotalEligible is consistent across
        -- all rows in EligibleReasons (it is a window COUNT(*)), every row in [EligibleReasons]
        -- sees the same denominator — the modulo always produces a valid RN.
    ) AS [rr]
)

-- ── Final INSERT ──────────────────────────────────────────────────────────────
INSERT INTO [gen].[OnlineReturnEvents] (
    [SalesOrderNumber],
    [SalesOrderLineNumber],
    [CustomerKey],
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
-- All 14 stored columns are named explicitly. OnlineReturnEventID is excluded —
-- it is filled automatically by the IDENTITY property on each insert.
-- Explicit column lists prevent silent data misalignment if the table schema
-- is ever altered and the column order changes.
)
SELECT
    [f].[SalesOrderNumber],
    -- Degenerate dimension — carried unchanged from CTE 1 via NoiseLocked → FinalRows.

    [f].[SalesOrderLineNumber],
    -- Degenerate dimension — completes the compound grain identifier for drill-through.

    [f].[CustomerKey],
    -- FK to dbo.DimCustomer — validated by FK_OnlineReturnEvents_Customer at INSERT time.

    [f].[ProductKey],
    -- FK to dbo.DimProduct — validated by FK_OnlineReturnEvents_Product at INSERT time.

    [f].[OriginalSaleDateKey],
    -- YYYYMMDD INT — stored in the raw 2007–2009 era. No +16 shift at this layer.

    [f].[OriginalSaleDate],
    -- DATE — consistent with OriginalSaleDateKey (same date, display-friendly type).

    [f].[ReturnDateKey],
    -- YYYYMMDD INT — synthesized and clamped to @MaxDate ('2009-12-31').

    [f].[ReturnDate],
    -- DATE — consistent with ReturnDateKey. Clamped by LEAST() in CTE 4 Step B.

    [f].[ReturnLagDays],
    -- INT >= 1 — validated by CHK_OnlineReturnEvents_ReturnLag at INSERT time.

    [f].[ReturnQuantity],
    -- INT — exact copy from dbo.FactOnlineSales.

    [f].[ReturnAmount],
    -- DECIMAL(19,4) — exact copy from dbo.FactOnlineSales. Used as RefundAmount base.

    [f].[ReturnReasonKey],
    -- FK to gen.DimReturnReason — validated by FK_OnlineReturnEvents_ReturnReason.
    -- Selected dynamically by the EligibleReasons CROSS APPLY in CTE 4.

    [f].[IsRefunded],
    -- BIT — derived from value-tiered refund approval threshold in CTE 4 Step E.

    CASE [f].[IsRefunded]
        WHEN 1 THEN [f].[ReturnAmount]
        -- Refund approved: RefundAmount = full ReturnAmount. No partial refunds modelled.
        ELSE        0.00
        -- No refund: RefundAmount stored as 0.00 (DECIMAL(19,4) literal).
    END AS [RefundAmount]
    -- ⚠ BEST PRACTICE — REFUNDAMOUNT DERIVED IN SELECT (not stored computed column):
    -- RefundAmount is derivable as IsRefunded × ReturnAmount, but is stored explicitly.
    -- The derivation must happen here (not in the DDL as a computed column) because
    -- IsRefunded is itself derived in the same CTE pipeline — SQL Server does not
    -- allow a regular column to reference a sibling computed-in-SELECT column.
    -- The explicit CASE ensures the IsRefunded/RefundAmount pair is always consistent.
FROM [FinalRows] AS [f];
GO

PRINT '  → [gen].[OnlineReturnEvents] populated.';
GO


-- ============================================================================
-- STEP 4: Create supporting index
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — STEP 4: NON-CLUSTERED INDEX                             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Creates a non-clustered index on CustomerKey (the highest-cardinality   ║
-- ║  FK) with ProductKey and ReturnReasonKey included for covering queries.  ║
-- ║  Most analytical queries on this table filter or group by CustomerKey    ║
-- ║  (CLV–return correlation, customer return rate) — this index serves     ║
-- ║  those access patterns without a full table scan.                        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE NONCLUSTERED INDEX [IX_OnlineReturnEvents_CustomerKey]
    ON [gen].[OnlineReturnEvents] ([CustomerKey])
    INCLUDE ([ProductKey], [ReturnReasonKey], [ReturnAmount], [RefundAmount]);
-- CustomerKey: the leading key column — supports WHERE CustomerKey = ? lookups.
-- INCLUDE: covers the most common projection columns so the engine can satisfy
-- queries from the index leaf page without touching the clustered index data page.
GO

PRINT '  → Index [IX_OnlineReturnEvents_CustomerKey] created.';
GO


-- ============================================================================
-- STEP 5: Verification queries (SET NOEXEC OFF first to re-enable execution)
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
-- Re-enables statement execution after any pre-check that used SET NOEXEC ON.
GO

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 7 — VERIFICATION QUERIES V1–V5                               ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  PURPOSE                                                                  ║
-- ║  Five verification queries confirm correctness at multiple levels.       ║
-- ║  These are BUSINESS LOGIC CHECKS — not just row counts. They validate    ║
-- ║  that the behavioral calibration model produced the intended output.     ║
-- ║                                                                           ║
-- ║  V1: Population summary — directional sanity (row count, refund rate,    ║
-- ║      avg lag, total amounts)                                              ║
-- ║  V2: Lag distribution by value tier — must be monotone ascending         ║
-- ║  V3: Reason distribution and date boundary check                         ║
-- ║  V4: Refund rate by value tier — must be monotone descending             ║
-- ║  V5: Referential integrity and data quality — all 7 checks must be 0    ║
-- ║                                                                           ║
-- ║  DETERMINISM NOTE                                                         ║
-- ║  V5 integrity checks are EXACT (must equal 0). V1–V4 distributions are  ║
-- ║  APPROXIMATE — NEWID() noise varies per run. Verify direction of         ║
-- ║  patterns and range boundaries, not exact numeric values.                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ----------------------------------------------------------------------------
-- V1 — POPULATION SUMMARY
-- Quick overview of what was generated.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — POPULATION SUMMARY                                               │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate):                                         │
-- │  TotalRows matches COUNT(*) from dbo.FactOnlineSales WHERE              │
-- │    ReturnQuantity > 0 AND ReturnAmount > 0                              │
-- │  RefundedRows ≈ 85–87% of TotalRows                                     │
-- │  AvgReturnLagDays between 15 and 25 (weighted across all tiers)         │
-- │  TotalRefundAmount ≈ 85% of TotalReturnAmount                           │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V1: Population summary';

SELECT
    COUNT(*)                                                         AS [TotalRows],
    -- Total return event rows. Must equal COUNT(*) from dbo.FactOnlineSales
    -- WHERE ReturnQuantity > 0 AND ReturnAmount > 0 (the source filter in CTE 1).
    SUM(CAST([IsRefunded] AS INT))                                                AS [RefundedRows],
    -- CAST([IsRefunded] AS INT): converts BIT to INT so SUM() can aggregate it.
    -- SUM of a BIT column is not directly supported in T-SQL — CAST is required.
    CAST(SUM(CAST([IsRefunded] AS INT)) * 100.0 / COUNT(*) AS DECIMAL(5,2))      AS [RefundRatePct],
    -- Refund rate as a percentage. * 100.0 forces FLOAT division before DECIMAL cast.
    -- Expected: ~85–87% (weighted average of 90/85/80 across tiers).
    CAST(AVG(CAST([ReturnLagDays] AS FLOAT))  AS DECIMAL(5,1))      AS [AvgReturnLagDays],
    -- AVG requires FLOAT cast for decimal precision. Expected: 15–25 days (tier-weighted).
    CAST(SUM([ReturnAmount])   AS DECIMAL(18,2))                     AS [TotalReturnAmount],
    -- Total gross return value. Reference point for RefundAmount below.
    CAST(SUM([RefundAmount])   AS DECIMAL(18,2))                     AS [TotalRefundAmount]
    -- Total refund liability. Expected: ~85% of TotalReturnAmount.
FROM [gen].[OnlineReturnEvents];
GO

-- ----------------------------------------------------------------------------
-- V2 — DATE RANGE AND LAG DISTRIBUTION BY VALUE TIER
-- Validates the behavioral lag model: higher tiers must show longer avg lags.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — LAG DISTRIBUTION BY VALUE TIER                                   │
-- │                                                                         │
-- │  EXPECTED OUTPUT (monotone — each tier must exceed the prior):          │
-- │  ┌───────────┬──────────────┬──────────────┬──────────────┐             │
-- │  │ ValueTier │ AvgLag       │ MinLag       │ MaxLag       │             │
-- │  ├───────────┼──────────────┼──────────────┼──────────────┤             │
-- │  │ 1 (Low)   │ ~10–11 days  │  7 days      │ 14 days      │             │
-- │  │ 2 (Mid)   │ ~17–18 days  │ 14 days      │ 21 days      │             │
-- │  │ 3 (High)  │ ~33–34 days  │ 21 days      │ 45 days      │             │
-- │  └───────────┴──────────────┴──────────────┴──────────────┘             │
-- │  ✗ Non-monotone Avg: lag model inverted or bucket thresholds wrong.     │
-- │  ✗ MaxLag > 45: date clamping was bypassed.                             │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V2: Lag distribution by value tier (must be monotone ascending)';

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
FROM [gen].[OnlineReturnEvents]
GROUP BY
    CASE
        WHEN [ReturnAmount] <  50.00 THEN '1 - Low  (< $50)'
        WHEN [ReturnAmount] < 200.00 THEN '2 - Mid  ($50–$199)'
        ELSE                              '3 - High (>= $200)'
    END
ORDER BY [ValueTier];
GO

-- ----------------------------------------------------------------------------
-- V3 — RETURN REASON DISTRIBUTION AND DATE BOUNDARY
-- Validates: uniform reason coverage, no dates beyond @MaxDate.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — REASON DISTRIBUTION AND DATE CHECKS                              │
-- │                                                                         │
-- │  EXPECTED OUTPUT:                                                       │
-- │  Each reason receives a roughly equal share of return events.           │
-- │  No reason key of 0 or NULL should appear.                              │
-- │  MaxReturnDate must be <= '2009-12-31'.                                 │
-- │  MinOriginalSaleDate must be >= '2007-01-01'.                           │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V3: Return reason distribution and date boundary';

SELECT
    [rdr].[ReturnReasonName],
    [rdr].[AppliesTo],
    COUNT(*)                                                         AS [ReturnCount],
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
         AS DECIMAL(5,2))                                            AS [PctShare]
FROM      [gen].[OnlineReturnEvents]  AS [ore]
INNER JOIN [gen].[DimReturnReason]    AS [rdr]
    ON [ore].[ReturnReasonKey] = [rdr].[ReturnReasonKey]
GROUP BY [rdr].[ReturnReasonName], [rdr].[AppliesTo]
ORDER BY [ReturnCount] DESC;

SELECT
    MIN([OriginalSaleDate])  AS [MinOriginalSaleDate],
    MAX([OriginalSaleDate])  AS [MaxOriginalSaleDate],
    MIN([ReturnDate])        AS [MinReturnDate],
    MAX([ReturnDate])        AS [MaxReturnDate]
FROM [gen].[OnlineReturnEvents];
GO

-- ----------------------------------------------------------------------------
-- V4 — REFUND RATE BY VALUE TIER
-- Higher-value returns must show lower refund approval rates.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — REFUND RATE BY VALUE TIER (must be monotone descending)          │
-- │                                                                         │
-- │  EXPECTED OUTPUT:                                                       │
-- │  ┌────────────┬─────────────────────────────────────────────────┐       │
-- │  │ ValueTier  │ RefundRatePct                                   │       │
-- │  ├────────────┼─────────────────────────────────────────────────┤       │
-- │  │ 1 (Low)    │ ~89–91%  (target: 90%)                          │       │
-- │  │ 2 (Mid)    │ ~84–86%  (target: 85%)                          │       │
-- │  │ 3 (High)   │ ~79–81%  (target: 80%)                          │       │
-- │  └────────────┴─────────────────────────────────────────────────┘       │
-- │  ✗ Non-monotone: IsRefunded derivation in CTE 4 is inverted.            │
-- │  ✗ All tiers same rate: ValueTier branching in CASE is not firing.      │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V4: Refund rate by value tier (must be monotone descending)';

SELECT
    CASE
        WHEN [ReturnAmount] <  50.00 THEN '1 - Low  (< $50)'
        WHEN [ReturnAmount] < 200.00 THEN '2 - Mid  ($50–$199)'
        ELSE                              '3 - High (>= $200)'
    END                                                              AS [ValueTier],
    COUNT(*)                                                         AS [RowCount],
    SUM(CAST([IsRefunded] AS INT))                                   AS [RefundedCount],
    CAST(SUM(CAST([IsRefunded] AS INT)) * 100.0 / COUNT(*)
         AS DECIMAL(5,2))                                            AS [RefundRatePct],
    CAST(SUM([RefundAmount]) AS DECIMAL(18,2))                       AS [TotalRefunded]
FROM [gen].[OnlineReturnEvents]
GROUP BY
    CASE
        WHEN [ReturnAmount] <  50.00 THEN '1 - Low  (< $50)'
        WHEN [ReturnAmount] < 200.00 THEN '2 - Mid  ($50–$199)'
        ELSE                              '3 - High (>= $200)'
    END
ORDER BY [ValueTier];
GO

-- ----------------------------------------------------------------------------
-- V5 — REFERENTIAL INTEGRITY & DATA QUALITY
-- All checks must return 0. Any non-zero value indicates a pipeline defect.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V5 — REFERENTIAL INTEGRITY & DATA QUALITY (all 7 checks must be 0)   │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — all zeros):                                   │
-- │  ┌──────────────────────────────────────────────────────┬──────────┐    │
-- │  │ Check                                                │ Expected │    │
-- │  ├──────────────────────────────────────────────────────┼──────────┤    │
-- │  │ Orphan CustomerKeys                                  │    0     │    │
-- │  │ Orphan ProductKeys                                   │    0     │    │
-- │  │ Orphan ReturnReasonKeys                              │    0     │    │
-- │  │ Duplicate (OrderNumber, LineNumber) pairs            │    0     │    │
-- │  │ ReturnDate after MaxDate (2009-12-31)                │    0     │    │
-- │  │ ReturnLagDays < 1                                    │    0     │    │
-- │  │ RefundAmount mismatch (refunded rows must have > 0)  │    0     │    │
-- │  └──────────────────────────────────────────────────────┴──────────┘    │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V5: Referential integrity and data quality (all expect 0)';

SELECT 'Orphan CustomerKeys' AS [Check],
    COUNT(*) AS [Value]
FROM [gen].[OnlineReturnEvents] AS [ore]
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[DimCustomer] AS [dc]
    WHERE [dc].[CustomerKey] = [ore].[CustomerKey]
)

UNION ALL

SELECT 'Orphan ProductKeys',
    COUNT(*)
FROM [gen].[OnlineReturnEvents] AS [ore]
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[DimProduct] AS [dp]
    WHERE [dp].[ProductKey] = [ore].[ProductKey]
)

UNION ALL

SELECT 'Orphan ReturnReasonKeys',
    COUNT(*)
FROM [gen].[OnlineReturnEvents] AS [ore]
WHERE NOT EXISTS (
    SELECT 1 FROM [gen].[DimReturnReason] AS [rdr]
    WHERE [rdr].[ReturnReasonKey] = [ore].[ReturnReasonKey]
)

UNION ALL

SELECT 'Duplicate (OrderNumber, LineNumber) pairs',
    COUNT(*) - COUNT(DISTINCT
        CAST([SalesOrderNumber] AS NVARCHAR(20))
        + '|' + CAST([SalesOrderLineNumber] AS NVARCHAR(10)))
-- Composite string key: encodes both degenerate dim columns for DISTINCT counting.
-- Non-zero means the UNIQUE constraint failed or the source had duplicates.
FROM [gen].[OnlineReturnEvents]

UNION ALL

SELECT 'ReturnDate after MaxDate (2009-12-31)',
    SUM(CASE WHEN [ReturnDate] > '2009-12-31' THEN 1 ELSE 0 END)
-- Validates the CASE-based date clamp in CTE 4.
-- Non-zero means the clamping logic failed for late-year sales.
FROM [gen].[OnlineReturnEvents]

UNION ALL

SELECT 'ReturnLagDays < 1',
    SUM(CASE WHEN [ReturnLagDays] < 1 THEN 1 ELSE 0 END)
-- CHECK constraint prevents this — check confirms the constraint fired correctly.
FROM [gen].[OnlineReturnEvents]

UNION ALL

SELECT 'RefundAmount mismatch (IsRefunded=1 must have > 0)',
    SUM(CASE
            WHEN [IsRefunded] = 1 AND [RefundAmount] = 0     THEN 1
            -- Refund approved but no DECIMAL(19,4) recorded: INSERT logic error.
            WHEN [IsRefunded] = 0 AND [RefundAmount] > 0     THEN 1
            -- No refund but DECIMAL(19,4) recorded: RefundAmount derivation error.
            ELSE 0
        END)
FROM [gen].[OnlineReturnEvents];
GO


PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 07 completed successfully.';
PRINT '  Table created:  [gen].[OnlineReturnEvents]';
PRINT '  Index created:  [IX_OnlineReturnEvents_CustomerKey]';
PRINT '';
PRINT '  Next steps:';
PRINT '    Script 08 → gen.PhysicalReturnEvents (parallel — run now if not already)';
PRINT '    After BOTH 07 and 08 complete:';
PRINT '    Script 09 → fact.vReturns (UNION ALL view over both return tables)';
PRINT '    Script 09 → dim.vReturnReason (semantic view over gen.DimReturnReason)';
PRINT '════════════════════════════════════════════════════════════════';
GO
