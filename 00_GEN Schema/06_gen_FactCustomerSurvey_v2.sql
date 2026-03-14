/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║        SCRIPT 06: gen.FactCustomerSurvey — NPS & CSAT SURVEY RESPONSES      ║
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
  This script generates gen.FactCustomerSurvey — one row per survey response
  linking each surveyed customer to their NPS score, CSAT score, and the
  context in which the survey was delivered.

  The Contoso source has zero customer satisfaction data. Without this table
  every loyalty and satisfaction KPI is dark: NPS, CSAT, Promoter/Detractor
  segmentation, satisfaction trend over time, and — critically — the ability
  to correlate satisfaction with CLV and purchase behavior.

  This is the most analytically sophisticated generation script in the project.
  Unlike random-assignment approaches, survey scores are BEHAVIORALLY COHERENT
  with each customer's actual transaction history in dbo.FactOnlineSales:

    ┌───────────────────────────────────────────────────────────────────────┐
    │  BUILT-IN CORRELATIONS (discoverable by students)                    │
    ├───────────────────────────────────────────────────────────────────────┤
    │  High purchase frequency  →  higher NPS base score                   │
    │  High return rate         →  score penalty (dissatisfaction signal)  │
    │  Recent last purchase     →  recency bonus (engaged customer)        │
    │  High total spend         →  slight investment bonus                 │
    │  Long tenure              →  longitudinal survey cadence unlocked    │
    └───────────────────────────────────────────────────────────────────────┘

  Students running NPS vs CLV analysis WILL find that Promoters have
  meaningfully higher lifetime value. This is not a coincidence — it is
  engineered into the generation logic to produce genuine insight.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Business Questions Unlocked                                            │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  CSO:  What is our overall Net Promoter Score?                          │
  │  CSO:  What % of customers are Promoters / Passives / Detractors?       │
  │  CSO:  How does NPS trend over time?                                    │
  │  CMO:  Which acquisition channel produces the most Promoters?           │
  │  CMO:  Do Promoters have higher CLV than Detractors?                    │
  │  COO:  How does CSAT correlate with product return rate?                │
  │  CFO:  What is the revenue at risk from Detractor segments?             │
  │  PM:   Which customer segments report the lowest CSAT scores?           │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  GRAIN AND SCOPE
--------------------------------------------------------------------------------
  Grain   : One row per survey response. Not every customer is surveyed.
            Customers can appear up to 3 times (one per trigger type).
  Scope   : Customers from dbo.DimCustomer where CustomerType = 'Person'
            and who have at least one transaction in dbo.FactOnlineSales.
  Trigger : 'Post-Purchase' (7–14 days post first purchase — all sampled customers)
            'Quarterly'     (≈180 days post first purchase — tenure > 200 days,
                             2+ orders)
            'Annual'        (≈365 days post first purchase — tenure > 380 days,
                             3+ orders)
  Sample  : ~15–30% realistic response rate. Frequency-biased:
            10+ orders → 30%, 2–9 orders → 20%, 1 order → 15%.

  ⚠  NOT ALL CUSTOMERS APPEAR IN THIS TABLE
  Students must compute NPS from SURVEYED customers only. Dividing by
  dbo.DimCustomer row count would produce an incorrect denominator.
  The standard NPS formula applied in DAX:
    NPS = (Promoters / Total Responses × 100) - (Detractors / Total Responses × 100)
  Or equivalently using NPSContribution in fact.vCustomerSurvey:
    NPS = DIVIDE(SUM([NPSContribution]), COUNT([SurveyResponseID])) * 100

--------------------------------------------------------------------------------
  SCORE ARCHITECTURE — DESIGN RATIONALE
--------------------------------------------------------------------------------
  Survey scores are derived from a 4-signal composite SatisfactionScore (0–1):

  ┌───────────────────┬──────────────────────────────────────────────────────┐
  │  Signal           │  Logic                                   │  Max  │   │
  ├───────────────────┼──────────────────────────────────────────┼───────┤   │
  │  Frequency        │  More orders → higher score              │  0.25 │   │
  │  Recency          │  Recent last purchase → higher score     │  0.25 │   │
  │  Return Rate      │  Lower returns → higher score            │  0.25 │   │
  │  Spend Magnitude  │  Higher lifetime spend → higher score    │  0.25 │   │
  └───────────────────┴──────────────────────────────────────────┴───────┘   │
  SatisfactionScore range: ~0.20 (churned, high-return, low-spend) to 1.00
  (frequent, recent, no returns, high spend).

  AdjustedScore = SatisfactionScore × TriggerMultiplier × RandomNoise(0.90–1.10)
  TriggerMultipliers: Post-Purchase = 0.85 (first impression), Quarterly = 1.00,
  Annual = 0.90–1.10 (variance reflects experience over full year).

  AdjustedScore → NPSScore mapping (0–10):
    ≥ 0.80 → Promoter band  (9–10, with small downward noise)
    ≥ 0.60 → Passive band   (7–8, with variance ±1)
    ≥ 0.40 → Low Passive / High Detractor band (5–7)
    ≥ 0.25 → Detractor band (3–6)
    < 0.25 → Strong Detractor band (0–4)

  CSATScore (1–5) is correlated with NPSScore but derived independently with
  its own noise draw — realistic because satisfaction and recommendation intent
  are related but not perfectly aligned.

--------------------------------------------------------------------------------
  TEMPORAL SHIFT — ARCHITECTURE NOTE
--------------------------------------------------------------------------------
  SurveyDate and SurveyDateKey are stored in the RAW source date range
  (2007–2009, anchored to actual dbo.FactOnlineSales transaction dates).
  No +16 year offset is applied at the [gen] layer.

  The +16 year temporal shift is applied EXCLUSIVELY at the semantic view layer
  (fact.vCustomerSurvey), consistent with the project-wide principle that all
  temporal transformations happen at the semantic layer, never at the physical
  data layer.

  @MaxDate = '2009-12-31' is the project-standard fixed reference date for
  DaysSinceLastPurchase. GETDATE() is NEVER used in [gen] scripts — it would
  make recency scores non-reproducible and break analytical integrity.

--------------------------------------------------------------------------------
  OUTPUT TABLE — gen.FactCustomerSurvey
--------------------------------------------------------------------------------
  Column              Type                   Notes
  ──────────────────────────────────────────────────────────────────────────────
  SurveyResponseID    INT IDENTITY PK        Auto surrogate — resets on re-run
  CustomerKey         INT NOT NULL FK        → dbo.DimCustomer
  SurveyDateKey       INT NOT NULL           YYYYMMDD (no +16 shift)
  SurveyDate          DATE NOT NULL          Actual survey completion date
  NPSScore            TINYINT NOT NULL       0–10 scale. CHECK(0–10).
  NPSCategory         NVARCHAR(20) PERSISTED Computed: Promoter/Passive/Detractor
  CSATScore           TINYINT NOT NULL       1–5 scale. CHECK(1–5).
  CSATCategory        NVARCHAR(20) PERSISTED Computed: Satisfied/Neutral/Dissatisfied
  WouldRecommend      BIT NOT NULL           1 if NPSScore >= 9, else 0
  SurveyTrigger       NVARCHAR(30) NOT NULL  Post-Purchase / Quarterly / Annual

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run order     : Script 06 — can run after Script 01 (gen schema only)
  Dependencies  : [gen] schema (Script 00), dbo.FactOnlineSales, dbo.DimCustomer
  Impact        : Creates ONE new table in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — idempotent DROP / CREATE guard on the table.
  Can parallel  : YES — no dependency on Scripts 02–05.

================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/


-- ============================================================================
-- PRE-CHECKS: Verify all dependencies before any DDL executes
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECKS (3 checks)               ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Three sequential dependency checks run before any DDL executes:         ║
-- ║  (1) [gen] schema           → Script 00 required                         ║
-- ║  (2) dbo.FactOnlineSales    → Contoso source required (behavioral data)  ║
-- ║  (3) dbo.DimCustomer        → Contoso source required (customer keys)    ║
-- ║                                                                           ║
-- ║  EXPECTED OUTPUT ON SUCCESS (3 green ticks in Messages tab):             ║
-- ║  ✓ [gen] schema confirmed.                                               ║
-- ║  ✓ [dbo].[FactOnlineSales] confirmed.                                    ║
-- ║  ✓ [dbo].[DimCustomer] confirmed.                                        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── CHECK 1 OF 3: [gen] Schema ───────────────────────────────────────────────

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
-- GO: T-SQL batch separator — each check is isolated so SET NOEXEC ON propagates correctly.

-- ── CHECK 2 OF 3: dbo.FactOnlineSales ────────────────────────────────────────

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

-- ── CHECK 3 OF 3: dbo.DimCustomer ────────────────────────────────────────────

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


-- ============================================================================
-- STEP 1: Create target table (idempotent — drops and recreates if exists)
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — STEP 1: TARGET TABLE DEFINITION                          ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Drops (if exists) and recreates gen.FactCustomerSurvey — a 10-column    ║
-- ║  survey response table where one row represents one survey response.      ║
-- ║  A single customer can appear up to 3 times (one per trigger type).      ║
-- ║                                                                           ║
-- ║  TABLE DESIGN DECISIONS                                                   ║
-- ║                                                                           ║
-- ║  PERSISTED COMPUTED COLUMNS — NPSCategory and CSATCategory                ║
-- ║  Unlike Script 05 (where computed columns were virtual/non-persisted),    ║
-- ║  NPSCategory and CSATCategory are PERSISTED. This means SQL Server        ║
-- ║  physically stores the derived string values in the data page at INSERT   ║
-- ║  time. Rationale: (a) NPSCategory is used in GROUP BY and WHERE clauses  ║
-- ║  so persistence avoids re-evaluation on every read; (b) PERSISTED columns ║
-- ║  can be indexed — a future NCI on NPSCategory becomes possible;           ║
-- ║  (c) the expression is deterministic (CASE on a stored TINYINT), which   ║
-- ║  is the SQL Server requirement for PERSISTED designation.                 ║
-- ║                                                                           ║
-- ║  TINYINT FOR SCORE COLUMNS                                                ║
-- ║  NPSScore (0–10) and CSATScore (1–5) use TINYINT (0–255 range, 1 byte)   ║
-- ║  rather than INT (4 bytes). For a table that will have ~20k–50k rows,     ║
-- ║  this is a meaningful storage reduction. CHECK constraints enforce the    ║
-- ║  narrower business range (0–10, 1–5) within the TINYINT domain.          ║
-- ║                                                                           ║
-- ║  GRAIN ENFORCEMENT — UNIQUE ON (CustomerKey, SurveyTrigger)               ║
-- ║  Each customer can receive at most ONE survey per trigger type. The       ║
-- ║  UNIQUE constraint on (CustomerKey, SurveyTrigger) enforces this at the  ║
-- ║  database level. It does NOT prevent a customer from appearing 3 times   ║
-- ║  (once per trigger) — that is expected and correct. It DOES prevent the  ║
-- ║  same customer from being inserted twice with the same trigger type due   ║
-- ║  to a generation pipeline defect.                                         ║
-- ║                                                                           ║
-- ║  SURVEYDATE vs SURVEYDATEKEY — TWO DATE COLUMNS                           ║
-- ║  • SurveyDate DATE: human-readable date for display and Power BI axis.   ║
-- ║  • SurveyDateKey INT (YYYYMMDD): FK to dim.vDate. Enables the Power BI   ║
-- ║    relationship for time intelligence (YTD, SAMEPERIODLASTYEAR etc.).    ║
-- ║    Computed inline at INSERT from SurveyDate using CONVERT(VARCHAR,112). ║
-- ║                                                                           ║
-- ║  ⚠  STUDENT CRITICAL NOTE — WouldRecommend vs NPSScore                   ║
-- ║  WouldRecommend (BIT) is stored separately from NPSScore even though it  ║
-- ║  is derivable from NPSScore >= 9. Storing it explicitly allows students  ║
-- ║  to use it as a direct slicer filter in Power BI without writing a DAX   ║
-- ║  calculated column. Both columns are correct — they are not redundant    ║
-- ║  in the BI context.                                                       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── Drop guard (idempotent) ───────────────────────────────────────────────────

DROP TABLE IF EXISTS [gen].[FactCustomerSurvey];
PRINT '→ Existing [gen].[FactCustomerSurvey] dropped if it existed.';
GO

-- ── Table creation ────────────────────────────────────────────────────────────

CREATE TABLE [gen].[FactCustomerSurvey]
-- Fully bracketed [schema].[Table] notation — project-wide standard for all object refs.
(
    -- ── Identity / Primary Key ────────────────────────────────────────────
    [SurveyResponseID]  INT IDENTITY(1,1)       NOT NULL,
    -- IDENTITY(1,1): auto-incrementing surrogate PK. No natural single-column key exists
    -- because one customer can have multiple responses. Resets on every DROP + recreate —
    -- harmless since fact.vCustomerSurvey reads from the live table.

    -- ── Foreign Keys ──────────────────────────────────────────────────────
    [CustomerKey]       INT                     NOT NULL,
    -- FK to dbo.DimCustomer (and by extension to dim.vCustomer in Power BI).
    -- INT matches the data type of DimCustomer.CustomerKey — no implicit conversion on join.

    -- ── Date columns ──────────────────────────────────────────────────────
    [SurveyDateKey]     INT                     NOT NULL,
    -- YYYYMMDD integer. FK to dim.vDate after the +16 year shift applied in fact.vCustomerSurvey.
    -- Raw source era stored here (2007–2009). Power BI relationship uses this column.

    [SurveyDate]        DATE                    NOT NULL,
    -- Actual survey completion date in DATE type — used for Power BI axis display
    -- and for DATEADD() derivation in the scoring CTE pipeline.

    -- ── NPS columns ───────────────────────────────────────────────────────
    [NPSScore]          TINYINT                 NOT NULL,
    -- 0–10 scale. TINYINT (1 byte, 0–255) is the correct storage type for small
    -- integer ranges — 4× more storage efficient than INT for score columns.
    -- CHECK constraint below enforces the 0–10 business range within the TINYINT domain.

    [NPSCategory]       AS CAST(
                            CASE
                                WHEN [NPSScore] >= 9 THEN 'Promoter'
                                WHEN [NPSScore] >= 7 THEN 'Passive'
                                ELSE                      'Detractor'
                            END
                        AS NVARCHAR(20)) PERSISTED,
    -- ⚠ BEST PRACTICE — PERSISTED COMPUTED COLUMN FOR HIGH-FREQUENCY GROUP BY:
    -- PERSISTED: SQL Server evaluates and stores this value at INSERT time.
    -- The expression is DETERMINISTIC (same NPSScore → same NPSCategory always),
    -- which is the SQL Server requirement for PERSISTED designation.
    -- Benefit: GROUP BY NPSCategory reads stored values rather than re-evaluating
    -- the CASE expression on every row scan — meaningful for large survey tables.
    -- NVARCHAR(20): 'Detractor' is 9 chars — 20 provides comfortable headroom.

    -- ── CSAT columns ──────────────────────────────────────────────────────
    [CSATScore]         TINYINT                 NOT NULL,
    -- 1–5 scale. Same TINYINT rationale as NPSScore above.
    -- CHECK constraint enforces 1–5 range below.

    [CSATCategory]      AS CAST(
                            CASE
                                WHEN [CSATScore] >= 4 THEN 'Satisfied'
                                WHEN [CSATScore] =  3 THEN 'Neutral'
                                ELSE                       'Dissatisfied'
                            END
                        AS NVARCHAR(20)) PERSISTED,
    -- PERSISTED computed column — same rationale as NPSCategory above.
    -- Thresholds: 4–5 = Satisfied, 3 = Neutral, 1–2 = Dissatisfied.
    -- These match the industry-standard CSAT classification used in fact.vCustomerSurvey.

    -- ── Recommendation Flag ───────────────────────────────────────────────
    [WouldRecommend]    BIT                     NOT NULL,
    -- 1 = would recommend (NPSScore >= 9 = Promoter), 0 = would not.
    -- Stored explicitly (not computed) to allow direct use as a Power BI slicer
    -- filter without requiring a DAX calculated column. Technically derivable
    -- from NPSScore but the redundancy is intentional for BI usability.

    -- ── Survey Context ────────────────────────────────────────────────────
    [SurveyTrigger]     NVARCHAR(30)            NOT NULL,
    -- The occasion that triggered this survey response.
    -- Valid values: 'Post-Purchase', 'Quarterly', 'Annual'.
    -- NVARCHAR(30): 'Post-Purchase' is 14 chars — 30 allows future trigger types.

    -- ── Score Range Constraints ───────────────────────────────────────────
    CONSTRAINT [CHK_FactCustomerSurvey_NPSScore]
        CHECK ([NPSScore] BETWEEN 0 AND 10),
    -- Enforces NPS scale at the database level. Any INSERT with NPSScore = 11
    -- (a TINYINT-valid value) is rejected by this constraint before reaching storage.

    CONSTRAINT [CHK_FactCustomerSurvey_CSATScore]
        CHECK ([CSATScore] BETWEEN 1 AND 5),
    -- Enforces CSAT scale. TINYINT allows 0 and 6–255 — this constraint narrows
    -- the valid domain to the 1–5 business range. Note: 0 is excluded (no CSAT zero).

    -- ── Primary Key ───────────────────────────────────────────────────────
    CONSTRAINT [PK_FactCustomerSurvey]
        PRIMARY KEY CLUSTERED ([SurveyResponseID]),
    -- CLUSTERED PK on IDENTITY: optimal for append-heavy generation workloads.
    -- Rows are physically stored in SurveyResponseID order — sequential inserts
    -- avoid page splits and produce minimal fragmentation.

    -- ── Grain Enforcement ─────────────────────────────────────────────────
    CONSTRAINT [UQ_FactCustomerSurvey_CustomerTrigger]
        UNIQUE ([CustomerKey], [SurveyTrigger]),
    -- ⚠ BEST PRACTICE — GRAIN ENFORCEMENT AT THE DATABASE LEVEL:
    -- Each customer receives AT MOST ONE survey per trigger type.
    -- A customer can appear 3 times (Post-Purchase + Quarterly + Annual) — this
    -- is correct and expected. This constraint prevents the SAME trigger appearing
    -- twice for the same customer due to a pipeline defect (duplicate UNION branch).
    -- Consistent with the Script 05 pattern: always enforce composite grain via UNIQUE.

    -- ── Referential Integrity ─────────────────────────────────────────────
    CONSTRAINT [FK_FactCustomerSurvey_Customer]
        FOREIGN KEY ([CustomerKey])
        REFERENCES [dbo].[DimCustomer] ([CustomerKey])
    -- Ensures every survey response belongs to a valid customer in the source.
    -- Rejects orphan CustomerKey values that the CTE pipeline might generate
    -- if the sampling logic produces a CustomerKey outside the DimCustomer range.
);
GO

PRINT '  → [gen].[FactCustomerSurvey] table created.';
GO


-- ============================================================================
-- STEP 2: Declare reference constant — anchors all recency calculations
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2: PRE-CTE VARIABLE DECLARATION                    ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Declares @MaxDate ONCE before the CTE chain begins.                     ║
-- ║                                                                           ║
-- ║  ⚠ BEST PRACTICE — PRE-CTE VARIABLE FOR WINDOW ANCHOR (Script 03 pattern):║
-- ║  If @MaxDate were derived inside the CTE as                              ║
-- ║    MAX(MAX(CAST(f.DateKey AS DATE))) OVER()                               ║
-- ║  SQL Server would materialise a Table Spool to evaluate that window      ║
-- ║  function across the entire 13M-row FactOnlineSales scan. Declaring the  ║
-- ║  constant before the CTE eliminates the spool entirely — the engine      ║
-- ║  replaces it with a scalar parameter substitution at compile time.       ║
-- ║                                                                           ║
-- ║  '2009-12-31' is the project-standard fixed reference date. GETDATE()   ║
-- ║  is NEVER used here — it would make DaysSinceLastPurchase non-           ║
-- ║  reproducible across runs and break analytical integrity on a historical ║
-- ║  dataset. Consistent with the temporal freeze principle used in all      ║
-- ║  prior gen scripts.                                                       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

DECLARE @MaxDate DATE = '2009-12-31';
-- Project-standard fixed reference date: the end of the Contoso DW source range.
-- All DaysSinceLastPurchase and TenureDays calculations reference this constant.
-- The +16 year shift to 2025-12-31 is applied ONLY at the fact.vCustomerSurvey view layer.


-- ============================================================================
-- STEP 3: Populate via behavioral calibration pipeline
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — STEP 3: 5-STAGE BEHAVIORAL CTE PIPELINE + INSERT        ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Generates all survey responses via a 5-CTE pipeline that converts       ║
-- ║  actual purchase behavior into behaviorally coherent satisfaction scores. ║
-- ║                                                                           ║
-- ║  ┌─────────────────────────────────────────────────────────────────────┐ ║
-- ║  │  PIPELINE OVERVIEW                                                  │ ║
-- ║  ├─────────────────────────────────────────────────────────────────────┤ ║
-- ║  │  CTE 1: CustomerBehavior                                            │ ║
-- ║  │    Per-customer aggregation from dbo.FactOnlineSales.               │ ║
-- ║  │    Joins dbo.DimCustomer BEFORE aggregation (predicate pushdown).   │ ║
-- ║  │    WHERE CustomerType = 'Person' is applied here — at the source    │ ║
-- ║  │    scan — not in a later CTE. This prevents the engine from running  │ ║
-- ║  │    COUNT(DISTINCT) and SUM aggregations on company accounts that    │ ║
-- ║  │    would ultimately be discarded.                                   │ ║
-- ║  │    Produces: TotalOrders, TotalSpend, ReturnRate, LastPurchaseDate,  │ ║
-- ║  │    FirstPurchaseDate (Person customers only).                       │ ║
-- ║  │                                                                     │ ║
-- ║  │  CTE 2: CustomerSatisfactionProfile                                 │ ║
-- ║  │    Pure mathematical projection layer — no join logic here.         │ ║
-- ║  │    Converts the pre-filtered, pre-aggregated CTE 1 output into a   │ ║
-- ║  │    composite SatisfactionScore (0.0–1.0) using a 4-signal model.   │ ║
-- ║  │    Adds DaysSinceLastPurchase and TenureDays derived from @MaxDate. │ ║
-- ║  │    (DimCustomer join was moved to CTE 1 as an architecture fix.)    │ ║
-- ║  │                                                                     │ ║
-- ║  │  CTE 3: SurveyedCustomers                                           │ ║
-- ║  │    Applies probabilistic sampling: 15–30% response rate, biased     │ ║
-- ║  │    toward more active customers (higher purchase frequency).        │ ║
-- ║  │                                                                     │ ║
-- ║  │  CTE 4: SurveyInstances                                             │ ║
-- ║  │    UNION ALL expands each sampled customer into up to 3 rows:       │ ║
-- ║  │    Post-Purchase (all), Quarterly (tenure > 200 days, 2+ orders),  │ ║
-- ║  │    Annual (tenure > 380 days, 3+ orders).                           │ ║
-- ║  │    Applies per-trigger AdjustedScore multiplier.                   │ ║
-- ║  │                                                                     │ ║
-- ║  │  CTE 5: ScoredResponses                                             │ ║
-- ║  │    Converts AdjustedScore into NPSScore, CSATScore, WouldRecommend  │ ║
-- ║  │    using CROSS APPLY to lock each random draw exactly once per row. │ ║
-- ║  └─────────────────────────────────────────────────────────────────────┘ ║
-- ║                                                                           ║
-- ║  SCORE FORMULA                                                            ║
-- ║  SatisfactionScore = FrequencySignal + RecencySignal                     ║
-- ║                    + ReturnRateSignal + SpendSignal                      ║
-- ║  AdjustedScore = SatisfactionScore × TriggerMultiplier                  ║
-- ║                × RandomNoise(0.90–1.10)                                 ║
-- ║  Noise: (0.90 + (ABS(CHECKSUM(NEWID())) % 200) / 1000.0) → ±10%        ║
-- ║  Narrower noise than Script 05 (±20%): survey scores should vary less  ║
-- ║  than marketing spend — personality is more stable than market prices.  ║
-- ║                                                                           ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                              ║
-- ║  1. DimCustomer is joined in CTE 1 (not CTE 2 as in the original       ║
-- ║     design). This is an architecture fix that enables predicate pushdown ║
-- ║     — the engine filters company accounts BEFORE running the expensive  ║
-- ║     COUNT(DISTINCT SalesOrderNumber) and SUM aggregations. The original ║
-- ║     approach wasted CPU/IO on rows that CTE 2 would have discarded.    ║
-- ║  2. CROSS APPLY materialises the NPS random draw BEFORE the CSAT draw. ║
-- ║     These are two independent NEWID() calls producing independent       ║
-- ║     randomness — NPS and CSAT are correlated through AdjustedScore but  ║
-- ║     not through the noise term. This is intentional: real-world NPS     ║
-- ║     and CSAT are related but not perfectly aligned.                     ║
-- ║  3. SurveyDate is clamped to <= @MaxDate using LEAST(). A customer      ║
-- ║     with FirstPurchaseDate = 2009-10-01 + 365 days would land in 2010  ║
-- ║     without the clamp, violating the project date range constraint.     ║
-- ║  4. V5 referential integrity check validates CustomerKey → DimCustomer. ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- Leading semicolon: defensive pattern. Prevents syntax error if a prior
-- batch statement was not terminated — T-SQL parses WITH as part of the prior
-- statement without the semicolon guard.

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 1: CustomerBehavior
-- Aggregates raw transaction history per customer from dbo.FactOnlineSales.
-- This is the GROUND TRUTH that all satisfaction scores are derived from.
-- ────────────────────────────────────────────────────────────────────────────
-- ;with [CustomerBehavior] AS (
--     SELECT
--         [f].[CustomerKey],
--         -- CustomerKey: the join key to dbo.DimCustomer and the FK stored in the output table.

--         COUNT(DISTINCT [f].[SalesOrderNumber])          AS [TotalOrders],
--         -- COUNT DISTINCT on SalesOrderNumber: counts unique orders, not line items.
--         -- A single order can have multiple product rows in FactOnlineSales —
--         -- DISTINCT prevents inflating order frequency for high-SKU orders.

--         SUM([f].[SalesAmount])                          AS [TotalSpend],
--         -- Lifetime gross sales amount. Used in the SpendSignal scoring tier.
--         -- No deduction for discounts: SalesAmount is already the net line price.

--         SUM([f].[ReturnQuantity])                       AS [TotalReturnQty],
--         -- Total units returned across all orders — numerator of ReturnRate below.

--         SUM([f].[SalesQuantity])                        AS [TotalSalesQty],
--         -- Total units sold — denominator of ReturnRate.
--         -- Stored separately (not pre-divided) to enable safe CASE division below.

--         CAST(
--             CASE WHEN SUM([f].[SalesQuantity]) > 0
--                  THEN SUM([f].[ReturnQuantity]) * 1.0
--                       / SUM([f].[SalesQuantity])
--                  ELSE 0.0
--             END
--         AS FLOAT)                                       AS [ReturnRate],
--         -- Return Rate = TotalReturns / TotalSalesQty (unit-based, not order-based).
--         -- CASE guard: prevents divide-by-zero for customers with SalesQuantity=0.
--         -- * 1.0: forces FLOAT division — without it, INT / INT truncates to 0 for rates < 1.
--         -- CAST AS FLOAT: explicit type for the scoring arithmetic below.
--         -- A ReturnRate of 0.15 = 15% returns: a strong dissatisfaction signal.

--         MAX(CAST([f].[DateKey] AS DATE))                AS [LastPurchaseDate],
--         -- Most recent purchase date. DateKey in FactOnlineSales is a DATETIME —
--         -- CAST to DATE strips the time component for clean DATEDIFF calculations.

--         MIN(CAST([f].[DateKey] AS DATE))                AS [FirstPurchaseDate]
--         -- Earliest purchase date. Used to anchor survey dates (Post-Purchase trigger
--         -- is 7–14 days after the first purchase).
--     FROM  [dbo].[FactOnlineSales]  AS [f]
--     -- Source: Contoso source fact (~13M rows). Full table scan — no WHERE filter
--     -- because every customer's behavioral history is needed for the scoring model.
--     -- The @MaxDate pre-declaration (Code Block 3) eliminates the Table Spool
--     -- that would otherwise materialise from a window function inside this CTE.
--     GROUP BY [f].[CustomerKey]
--     -- One row per customer — the unit of analysis for the satisfaction model.
-- ),

-- -- ────────────────────────────────────────────────────────────────────────────
-- -- CTE 2: CustomerSatisfactionProfile
-- -- Converts raw behavioral metrics into a composite SatisfactionScore (0.0–1.0).
-- -- Joins dbo.DimCustomer to filter to individual person accounts only.
-- -- ────────────────────────────────────────────────────────────────────────────
-- [CustomerSatisfactionProfile] AS (
--     SELECT
--         [cb].[CustomerKey],
--         [cb].[TotalOrders],
--         [cb].[TotalSpend],
--         [cb].[ReturnRate],
--         [cb].[LastPurchaseDate],
--         [cb].[FirstPurchaseDate],
--         -- Pass-throughs from CTE 1: needed for survey date derivation in CTE 4.

--         DATEDIFF(DAY, [cb].[LastPurchaseDate],  @MaxDate) AS [DaysSinceLastPurchase],
--         -- How long ago the customer last purchased (relative to project reference date).
--         -- Low values (<90) → recently engaged → recency bonus in SatisfactionScore.
--         -- High values (>365) → churned → significant recency penalty.
--         -- @MaxDate used here (NOT GETDATE()) — project-standard temporal freeze.

--         DATEDIFF(DAY, [cb].[FirstPurchaseDate], @MaxDate) AS [TenureDays],
--         -- Customer tenure from first to last known date in the dataset.
--         -- Used in CTE 3 (survey sampling eligibility) and CTE 4 (trigger unlocking).

--         -- ── SATISFACTION PREDICTOR SCORE (0.0 to 1.0) ──────────────────────
--         -- Four independent signals, each capped at 0.25, sum to max 1.0.
--         -- ⚠ BEST PRACTICE — ADDITIVE SIGNAL MODEL WITH EQUAL MAX WEIGHTS:
--         -- Each signal contributes equally to the maximum possible score.
--         -- This prevents any single dimension from dominating the prediction —
--         -- a high-spend but churned customer does not get an unfairly high score.
--         CAST(
--             -- ── Signal 1: Purchase Frequency (0.05–0.25) ──────────────────
--             CASE
--                 WHEN [cb].[TotalOrders] >= 10 THEN 0.25
--                 -- Very frequent buyer: strong loyalty signal.
--                 WHEN [cb].[TotalOrders] >=  5 THEN 0.20
--                 -- Regular multi-purchase customer.
--                 WHEN [cb].[TotalOrders] >=  3 THEN 0.15
--                 -- Established repeat buyer.
--                 WHEN [cb].[TotalOrders] >=  2 THEN 0.10
--                 -- Has returned at least once — some loyalty shown.
--                 ELSE                               0.05
--                 -- Single-purchase customer: minimum frequency score.
--             END

--             -- ── Signal 2: Recency (0.05–0.25) ─────────────────────────────
--             + CASE
--                 WHEN DATEDIFF(DAY, [cb].[LastPurchaseDate], @MaxDate) <  90 THEN 0.25
--                 -- Very recent: actively engaged with the brand.
--                 WHEN DATEDIFF(DAY, [cb].[LastPurchaseDate], @MaxDate) < 180 THEN 0.20
--                 -- Purchased in the last 6 months: still in active relationship.
--                 WHEN DATEDIFF(DAY, [cb].[LastPurchaseDate], @MaxDate) < 365 THEN 0.12
--                 -- Purchased within a year: at-risk but recoverable.
--                 ELSE                                                         0.05
--                 -- No purchase in 365+ days: likely churned — minimum recency score.
--             END

--             -- ── Signal 3: Return Rate (0.03–0.25) ──────────────────────────
--             + CASE
--                 WHEN [cb].[ReturnRate] = 0                THEN 0.25
--                 -- No returns: full product satisfaction signal.
--                 WHEN [cb].[ReturnRate] < 0.05             THEN 0.18
--                 -- <5% return rate: minor dissatisfaction, well within normal range.
--                 WHEN [cb].[ReturnRate] < 0.15             THEN 0.10
--                 -- 5–15% return rate: material product/quality issues signal.
--                 ELSE                                           0.03
--                 -- >15% return rate: strong dissatisfaction — significant score penalty.
--             END

--             -- ── Signal 4: Spend Magnitude (0.07–0.25) ──────────────────────
--             + CASE
--                 WHEN [cb].[TotalSpend] > 5000 THEN 0.25
--                 -- High-lifetime-value customer: investment in the brand correlates
--                 -- with satisfaction (dissatisfied customers stop spending first).
--                 WHEN [cb].[TotalSpend] > 1000 THEN 0.18
--                 -- Meaningful lifetime value: above-average engagement.
--                 WHEN [cb].[TotalSpend] >  200 THEN 0.12
--                 -- Moderate spend: normal mid-tier customer engagement.
--                 ELSE                               0.07
--                 -- Low-spend customer: minimal investment floor.
--             END
--         AS FLOAT)                                       AS [SatisfactionScore]
--         -- CAST AS FLOAT: ensures the CASE arithmetic produces decimal precision
--         -- rather than truncated integer sums. Range: ~0.20 (disengaged) to 1.00.

--     FROM      [CustomerBehavior]  AS [cb]
--     INNER JOIN [dbo].[DimCustomer] AS [dc]
--         ON [cb].[CustomerKey] = [dc].[CustomerKey]
--     -- INNER JOIN to DimCustomer: filters to only customers who exist in the dimension.
--     -- Excludes any CustomerKey in FactOnlineSales that has no matching DimCustomer row
--     -- (data quality protection — orphan transactions should not generate survey records).
--     WHERE [dc].[CustomerType] = 'Person'
--     -- Excludes company/business accounts from the survey generation.
--     -- Business customers follow different satisfaction measurement protocols (B2B surveys)
--     -- and should not appear in the consumer NPS/CSAT dataset.
-- ),
-- ────────────────────────────────────────────────────────────────────────────
-- CTE 1: CustomerBehavior
-- Aggregates raw transaction history per customer from dbo.FactOnlineSales.
-- ⚠ ARCHITECTURE FIX: DimCustomer is joined BEFORE aggregation to filter out
-- 'Company' accounts early. This ensures predicate pushdown, preventing the 
-- engine from wasting CPU/IO on COUNT(DISTINCT) and SUM aggregations for 
-- customers that will ultimately be discarded.
-- ────────────────────────────────────────────────────────────────────────────
;with [CustomerBehavior] AS (
-- Leading semicolon: defensive T-SQL pattern — prevents a syntax error if a prior
-- batch statement was not terminated before this WITH clause.
-- CTE NAME: CustomerBehavior — the ground truth for all satisfaction scoring.
-- Every signal in the 4-factor model (frequency, recency, return rate, spend) is
-- derived from the aggregates computed here against the actual transaction history.
    SELECT
        [f].[CustomerKey],
        -- CustomerKey: the join key from FactOnlineSales to DimCustomer. This is the
        -- FK written to gen.FactCustomerSurvey and used for all customer-level analysis.
        COUNT(DISTINCT [f].[SalesOrderNumber])          AS [TotalOrders],
        -- ⚠ BEST PRACTICE — COUNT DISTINCT ON ORDER NUMBER, NOT ROW COUNT:
        -- A single SalesOrderNumber can span multiple product rows in FactOnlineSales.
        -- COUNT(*) would inflate order frequency for multi-SKU orders.
        -- COUNT(DISTINCT SalesOrderNumber) counts unique orders — the correct frequency
        -- metric for Frequency Signal scoring in the satisfaction model.
        SUM([f].[SalesAmount])                          AS [TotalSpend],
        -- Lifetime gross sales amount: sum of all SalesAmount values across all orders.
        -- SalesAmount is already the net line price — no discount deduction needed.
        -- Used in Signal 4 (Spend Magnitude) scoring tier in CTE 2.
        SUM([f].[ReturnQuantity])                       AS [TotalReturnQty],
        -- Total units returned across all orders — the numerator of the ReturnRate ratio.
        -- Stored separately (not pre-divided) to enable the safe CASE division below.
        SUM([f].[SalesQuantity])                        AS [TotalSalesQty],
        -- Total units sold — the denominator of the ReturnRate ratio.
        -- Stored separately so the CASE guard can check for zero before dividing.
        CAST(
            CASE WHEN SUM([f].[SalesQuantity]) > 0
                 THEN SUM([f].[ReturnQuantity]) * 1.0
                      / SUM([f].[SalesQuantity])
                 ELSE 0.0
            END
        AS FLOAT)                                       AS [ReturnRate],
        -- Return Rate = TotalReturnQty / TotalSalesQty — unit-based (not order-based).
        -- CASE guard: prevents divide-by-zero for customers whose SalesQuantity = 0
        -- (defensive — all rows in this CTE have SalesQuantity > 0 by source data design).
        -- * 1.0: forces FLOAT division. Without it, INT / INT truncates to 0 for rates < 1.
        -- CAST AS FLOAT: explicit type required for the scoring arithmetic in CTE 2.
        -- Example: ReturnRate of 0.15 = 15% returns — a strong dissatisfaction signal.
        MAX(CAST([f].[DateKey] AS DATE))                AS [LastPurchaseDate],
        -- Most recent purchase date. DateKey in FactOnlineSales is DATETIME (legacy Contoso).
        -- CAST to DATE strips the time component for clean DATEDIFF calculations in CTE 2.
        -- MAX = most recent date = the customer's last known engagement with the brand.
        MIN(CAST([f].[DateKey] AS DATE))                AS [FirstPurchaseDate]
        -- Earliest purchase date — anchors survey dates in CTE 4 (Post-Purchase trigger
        -- is 7–14 days after the first purchase; Quarterly and Annual triggers build on it).
        -- MIN = earliest date = the customer's acquisition event.
    FROM  [dbo].[FactOnlineSales]  AS [f]
    -- Source: the ~13M-row Contoso online fact table. Alias [f] for brevity.
    INNER JOIN [dbo].[DimCustomer] AS [dc]
        ON [f].[CustomerKey] = [dc].[CustomerKey]
    -- ⚠ BEST PRACTICE — JOIN DIMENSION BEFORE AGGREGATION FOR PREDICATE PUSHDOWN:
    -- Joining DimCustomer HERE (inside CTE 1, before GROUP BY) allows the optimizer
    -- to push the CustomerType = 'Person' predicate down to the FactOnlineSales scan.
    -- The engine filters out company account rows BEFORE computing COUNT(DISTINCT) and SUM —
    -- avoiding wasted aggregation work on records that would be discarded after GROUP BY.
    -- In the original design, this join appeared in CTE 2 (after aggregation) — meaning
    -- the engine first aggregated ALL customers (including companies), then discarded them.
    WHERE [dc].[CustomerType] = 'Person'
    -- Filters to individual consumer accounts only. Business/company accounts use
    -- different B2B satisfaction measurement protocols and must not appear in this
    -- consumer NPS/CSAT dataset. Applied at the join layer for predicate pushdown.
    GROUP BY [f].[CustomerKey]
    -- One row per CustomerKey — the unit of analysis for the satisfaction scoring model.
    -- All aggregated values above (TotalOrders, TotalSpend etc.) are at this customer grain.
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 2: CustomerSatisfactionProfile
-- Converts raw behavioral metrics into a composite SatisfactionScore (0.0–1.0).
-- Stripped of the join logic, this CTE now functions purely as a mathematical
-- projection layer over the pre-filtered, pre-aggregated base.
-- ────────────────────────────────────────────────────────────────────────────
[CustomerSatisfactionProfile] AS (
-- CTE NAME: CustomerSatisfactionProfile — a pure calculation layer.
-- Reads exclusively from CTE 1 ([CustomerBehavior]). No table joins here.
-- All person-account filtering was completed in CTE 1 via predicate pushdown.
    SELECT
        [cb].[CustomerKey],
        -- Pass-through: FK to dbo.DimCustomer — written to the output table and used
        -- for the CustomerKey → DimCustomer FK constraint and Power BI relationships.
        [cb].[TotalOrders],
        -- Pass-through: needed for survey sampling threshold in CTE 3 (10+ orders →
        -- 30% response rate) and trigger eligibility checks in CTE 4 (2+ orders for
        -- Quarterly trigger, 3+ orders for Annual trigger).
        [cb].[TotalSpend],
        -- Pass-through: needed for Signal 4 (Spend Magnitude) scoring below.
        [cb].[ReturnRate],
        -- Pass-through: needed for Signal 3 (Return Rate) scoring below.
        [cb].[LastPurchaseDate],
        -- Pass-through: needed for survey date derivation in CTE 4.
        [cb].[FirstPurchaseDate],
        -- Pass-through: anchors the Post-Purchase survey date (FirstPurchaseDate + 7–14 days)
        -- and the tenure calculation (TenureDays) below.

        DATEDIFF(DAY, [cb].[LastPurchaseDate],  @MaxDate) AS [DaysSinceLastPurchase],
        -- Days between the customer's last purchase and the project reference date (2009-12-31).
        -- Low values (<90): recently engaged → maximum recency bonus in Signal 2 scoring.
        -- High values (>365): likely churned → significant recency penalty (0.05 floor).
        -- @MaxDate used here (NOT GETDATE()) — project-standard temporal freeze ensures
        -- reproducible recency scores across all runs on this historical dataset.
        DATEDIFF(DAY, [cb].[FirstPurchaseDate], @MaxDate) AS [TenureDays],
        -- Customer tenure: days from first purchase to project reference date.
        -- Used in CTE 4 to gate trigger eligibility: TenureDays > 200 → Quarterly eligible,
        -- TenureDays > 380 → Annual eligible.

        -- ── SATISFACTION PREDICTOR SCORE (0.0 to 1.0) ──────────────────────
        -- ⚠ BEST PRACTICE — ADDITIVE SIGNAL MODEL WITH EQUAL MAX WEIGHTS:
        -- Four independent signals, each capped at 0.25, sum to a maximum of 1.0.
        -- Equal weighting prevents any single dimension from dominating the score —
        -- a high-spend but churned customer does not receive an unfairly high rating.
        CAST(
            -- ── Signal 1: Purchase Frequency (0.05–0.25) ──────────────────
            CASE
                WHEN [cb].[TotalOrders] >= 10 THEN 0.25
                -- Very frequent buyer: 10+ orders — strong loyalty signal, maximum score.
                WHEN [cb].[TotalOrders] >=  5 THEN 0.20
                -- Regular multi-purchase customer: 5–9 orders.
                WHEN [cb].[TotalOrders] >=  3 THEN 0.15
                -- Established repeat buyer: 3–4 orders.
                WHEN [cb].[TotalOrders] >=  2 THEN 0.10
                -- Has returned at least once — some brand loyalty demonstrated.
                ELSE                               0.05
                -- Single-purchase customer: minimum frequency score (not necessarily dissatisfied).
            END

            -- ── Signal 2: Recency (0.05–0.25) ─────────────────────────────
            + CASE
                WHEN DATEDIFF(DAY, [cb].[LastPurchaseDate], @MaxDate) <  90 THEN 0.25
                -- Purchased in the last 90 days: actively engaged with the brand — maximum recency.
                WHEN DATEDIFF(DAY, [cb].[LastPurchaseDate], @MaxDate) < 180 THEN 0.20
                -- Purchased in the last 6 months: still within an active customer relationship.
                WHEN DATEDIFF(DAY, [cb].[LastPurchaseDate], @MaxDate) < 365 THEN 0.12
                -- Purchased within a year: at-risk but potentially recoverable.
                ELSE                                                         0.05
                -- No purchase in 365+ days: likely churned — minimum recency score.
            END

            -- ── Signal 3: Return Rate (0.03–0.25) ──────────────────────────
            + CASE
                WHEN [cb].[ReturnRate] = 0                THEN 0.25
                -- Zero returns: full product satisfaction signal — no dissatisfaction detected.
                WHEN [cb].[ReturnRate] < 0.05             THEN 0.18
                -- <5% return rate: minor dissatisfaction, well within industry-normal ranges.
                WHEN [cb].[ReturnRate] < 0.15             THEN 0.10
                -- 5–15% return rate: material product or quality issues evident.
                ELSE                                           0.03
                -- >15% return rate: strong dissatisfaction signal — significant score penalty.
            END

            -- ── Signal 4: Spend Magnitude (0.07–0.25) ──────────────────────
            + CASE
                WHEN [cb].[TotalSpend] > 5000 THEN 0.25
                -- High-lifetime-value customer: investment in the brand correlates with
                -- satisfaction — dissatisfied customers stop spending before churning.
                WHEN [cb].[TotalSpend] > 1000 THEN 0.18
                -- Meaningful lifetime value: above-average brand engagement.
                WHEN [cb].[TotalSpend] >  200 THEN 0.12
                -- Moderate spend: normal mid-tier customer engagement level.
                ELSE                               0.07
                -- Low-spend customer: minimum investment floor — could be new or disengaged.
            END
        AS FLOAT)                                       AS [SatisfactionScore]
        -- ⚠ BEST PRACTICE — CAST AS FLOAT FOR MULTI-TERM CASE ARITHMETIC:
        -- Each CASE branch produces a DECIMAL literal (0.25, 0.20 etc.). The addition of
        -- four DECIMAL values is DECIMAL + DECIMAL arithmetic — accurate, but the final
        -- value is consumed by FLOAT multiplication in CTE 4 (AdjustedScore). CAST AS FLOAT
        -- here ensures consistent type propagation without implicit conversion surprises.
        -- Score range: ~0.20 (churned, high-return, low-spend) to 1.00 (maximum all signals).

    FROM [CustomerBehavior]  AS [cb]
    -- Source: CTE 1 — one row per customer, already filtered to CustomerType = 'Person'.
    -- No join to DimCustomer needed here: the person-account filter was applied in CTE 1.
),
-- ────────────────────────────────────────────────────────────────────────────
-- CTE 3: SurveyedCustomers
-- Applies probabilistic sampling to select ~15–30% of customers.
-- More active customers are more likely to be sampled (realistic response bias).
-- ────────────────────────────────────────────────────────────────────────────
[SurveyedCustomers] AS (
    SELECT
        [csp].[CustomerKey],
        [csp].[TotalOrders],
        [csp].[TotalSpend],
        [csp].[ReturnRate],
        [csp].[SatisfactionScore],
        [csp].[LastPurchaseDate],
        [csp].[FirstPurchaseDate],
        [csp].[DaysSinceLastPurchase],
        [csp].[TenureDays]
        -- Pass-throughs: all metrics needed by CTE 4 for survey date derivation
        -- and trigger eligibility checks.
    FROM [CustomerSatisfactionProfile]  AS [csp]
    WHERE ABS(CHECKSUM(NEWID())) % 100 <
    -- ⚠ BEST PRACTICE — WHERE-CLAUSE PROBABILISTIC SAMPLING:
    -- ABS(CHECKSUM(NEWID())) % 100: generates a uniform random integer 0–99 per row.
    -- Comparing to a threshold value: rows where the random value < threshold are KEPT.
    -- Threshold = 30 → 30% of rows pass; threshold = 15 → 15% of rows pass.
    -- This is more efficient than a TOP with ORDER BY NEWID() (which sorts all rows)
    -- and more transparent than a ROW_NUMBER() sampling pattern.
          CASE
              WHEN [csp].[TotalOrders] >= 10 THEN 30
              -- Frequent buyers: 30% response rate. Highly engaged customers respond more.
              WHEN [csp].[TotalOrders] >=  5 THEN 25
              -- Regular buyers: 25% response rate.
              WHEN [csp].[TotalOrders] >=  2 THEN 20
              -- Repeat buyers: 20% response rate.
              ELSE                                15
              -- One-time buyers: 15% minimum response rate.
          END
    -- Result: overall average response rate ≈ 18–22% across the customer base —
    -- consistent with documented real-world B2C survey response rates.
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 4: SurveyInstances
-- Expands each sampled customer into up to 3 survey response rows via UNION ALL.
-- Each branch adds a trigger-specific AdjustedScore multiplier and date offset.
-- ────────────────────────────────────────────────────────────────────────────

    -- ── Branch 1: Post-Purchase Survey ────────────────────────────────────
    -- All sampled customers receive a Post-Purchase survey 7–14 days after
    -- their first purchase. Score is discounted 15% (first impression effect:
    -- the customer has not yet built a full relationship with the brand).
    [SurveyInstances] AS (

    -- ── Branch 1: Post-Purchase Survey ────────────────────────────────────
    SELECT
        [sc].[CustomerKey],
        LEAST(DATEADD(DAY, 7 + ABS(CHECKSUM(NEWID())) % 8, [sc].[FirstPurchaseDate]), @MaxDate) AS [SurveyDate],
        [sc].[SatisfactionScore] * 0.85                 AS [AdjustedScore],
        'Post-Purchase'                                 AS [SurveyTrigger],
        ABS(CHECKSUM(NEWID())) % 100                    AS [NPSRandSeed],
        ABS(CHECKSUM(NEWID())) % 100                    AS [CSATRandSeed]
    FROM [SurveyedCustomers]  AS [sc]

    UNION ALL

    -- ── Branch 2: Quarterly Survey ────────────────────────────────────────
    SELECT
        [sc].[CustomerKey],
        LEAST(DATEADD(DAY, 180 + ABS(CHECKSUM(NEWID())) % 30, [sc].[FirstPurchaseDate]), @MaxDate) AS [SurveyDate],
        [sc].[SatisfactionScore] * 1.00                 AS [AdjustedScore],
        'Quarterly'                                     AS [SurveyTrigger],
        ABS(CHECKSUM(NEWID())) % 100                    AS [NPSRandSeed],
        ABS(CHECKSUM(NEWID())) % 100                    AS [CSATRandSeed]
    FROM [SurveyedCustomers]  AS [sc]
    WHERE [sc].[TenureDays] > 200 AND [sc].[TotalOrders] >= 2

    UNION ALL

    -- ── Branch 3: Annual Survey ───────────────────────────────────────────
    SELECT
        [sc].[CustomerKey],
        LEAST(DATEADD(DAY, 365 + ABS(CHECKSUM(NEWID())) % 30, [sc].[FirstPurchaseDate]), @MaxDate) AS [SurveyDate],
        [sc].[SatisfactionScore] * (0.90 + (ABS(CHECKSUM(NEWID())) % 200) / 1000.0) AS [AdjustedScore],
        'Annual'                                        AS [SurveyTrigger],
        ABS(CHECKSUM(NEWID())) % 100                    AS [NPSRandSeed],
        ABS(CHECKSUM(NEWID())) % 100                    AS [CSATRandSeed]
    FROM [SurveyedCustomers]  AS [sc]
    WHERE [sc].[TenureDays] > 380 AND [sc].[TotalOrders] >= 3
),

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 5: ScoredResponses
-- Converts AdjustedScore into integer NPSScore and CSATScore via CROSS APPLY.
-- Uses the two-stage materialisation pattern from Script 05 to lock each
-- random draw independently before the next derivation reads it.
-- ────────────────────────────────────────────────────────────────────────────
[ScoredResponses] AS (
    SELECT
        [si].[CustomerKey],
        [si].[SurveyDate],
        CAST(CONVERT(VARCHAR(8), [si].[SurveyDate], 112) AS INT) AS [SurveyDateKey],
        [si].[SurveyTrigger],
        [si].[AdjustedScore],
        [NPS].[NPSScore],
        [CSAT].[CSATScore],
        CAST(CASE WHEN [NPS].[NPSScore] >= 9 THEN 1 ELSE 0 END AS BIT) AS [WouldRecommend]
    FROM [SurveyInstances]  AS [si]
    
    -- ── Stage 1: Lock NPS Score using Static Seed ──────────────────────────
    CROSS APPLY (
        SELECT CAST(
            CASE
                WHEN [si].[AdjustedScore] >= 0.80 THEN
                    8 + CASE
                            WHEN [si].[NPSRandSeed] < 55 THEN 2
                            WHEN [si].[NPSRandSeed] < 85 THEN 1
                            ELSE                              0
                        END
                WHEN [si].[AdjustedScore] >= 0.60 THEN
                    6 + CASE
                            WHEN [si].[NPSRandSeed] < 50 THEN 2
                            WHEN [si].[NPSRandSeed] < 85 THEN 1
                            ELSE                              0
                        END
                WHEN [si].[AdjustedScore] >= 0.40 THEN
                    5 + ABS(CHECKSUM(NEWID())) % 3  -- Safe: Evaluated only once per tier
                WHEN [si].[AdjustedScore] >= 0.25 THEN
                    3 + ABS(CHECKSUM(NEWID())) % 4  -- Safe: Evaluated only once per tier
                ELSE
                    ABS(CHECKSUM(NEWID())) % 5      -- Safe: Evaluated only once per tier
            END
        AS TINYINT) AS [NPSScore]
    )  AS [NPS]

    -- ── Stage 2: Lock CSAT Score using Static Seed ─────────────────────────
    CROSS APPLY (
        SELECT CAST(
            CASE
                WHEN [NPS].[NPSScore] >= 9 THEN
                    CASE
                        WHEN [si].[CSATRandSeed] < 60 THEN 5
                        WHEN [si].[CSATRandSeed] < 90 THEN 4
                        ELSE                               3
                    END
                WHEN [NPS].[NPSScore] >= 7 THEN
                    CASE
                        WHEN [si].[CSATRandSeed] < 40 THEN 4
                        WHEN [si].[CSATRandSeed] < 80 THEN 3
                        ELSE                               2
                    END
                WHEN [NPS].[NPSScore] >= 4 THEN
                    CASE
                        WHEN [si].[CSATRandSeed] < 50 THEN 3
                        WHEN [si].[CSATRandSeed] < 80 THEN 2
                        ELSE                               1
                    END
                ELSE
                    CASE
                        WHEN [si].[CSATRandSeed] < 55 THEN 1
                        ELSE                               2
                    END
            END
        AS TINYINT) AS [CSATScore]
    )  AS [CSAT]
)

-- ────────────────────────────────────────────────────────────────────────────
-- FINAL INSERT — projects all 7 stored columns from ScoredResponses
-- Computed columns (NPSCategory, CSATCategory) are auto-generated by SQL Server.
-- ────────────────────────────────────────────────────────────────────────────
INSERT INTO [gen].[FactCustomerSurvey]
    ([CustomerKey], [SurveyDateKey], [SurveyDate],
     [NPSScore], [CSATScore], [WouldRecommend], [SurveyTrigger])
-- Explicit column list: 7 stored columns.
-- SurveyResponseID excluded — filled by IDENTITY automatically.
-- NPSCategory and CSATCategory excluded — filled by PERSISTED computed columns.
SELECT
    [sr].[CustomerKey],
    -- FK to dbo.DimCustomer — validated by the FK constraint at INSERT time.

    [sr].[SurveyDateKey],
    -- YYYYMMDD integer — stored in raw 2007–2009 era (no +16 shift at this layer).

    [sr].[SurveyDate],
    -- DATE value — stored in raw 2007–2009 era (consistent with SurveyDateKey).

    [sr].[NPSScore],
    -- TINYINT 0–10 — validated by CHK_FactCustomerSurvey_NPSScore at INSERT time.

    [sr].[CSATScore],
    -- TINYINT 1–5 — validated by CHK_FactCustomerSurvey_CSATScore at INSERT time.

    [sr].[WouldRecommend],
    -- BIT 1/0 — derived from locked NPSScore in CTE 5.

    [sr].[SurveyTrigger]
    -- NVARCHAR(30): 'Post-Purchase', 'Quarterly', or 'Annual'.

FROM [ScoredResponses]  AS [sr];
GO

PRINT '  → [gen].[FactCustomerSurvey] populated.';
GO


-- ============================================================================
-- STEP 4: Performance index
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — STEP 4: PERFORMANCE INDEX                                ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  WHAT THIS DOES                                                           ║
-- ║  Creates a Non-Clustered Index on CustomerKey to support the most common  ║
-- ║  access patterns against gen.FactCustomerSurvey:                         ║
-- ║  (1) JOIN to dim.vCustomer for customer attribute enrichment              ║
-- ║  (2) CROSS APPLY or SUBQUERY from FactOnlineSales by CustomerKey          ║
-- ║  (3) Correlation queries: NPS Promoters → purchase behavior              ║
-- ║                                                                           ║
-- ║  WHY INCLUDE NPSScore AND SurveyDateKey                                   ║
-- ║  The most analytically valuable queries combine customer lookup with      ║
-- ║  score and date filtering (e.g., NPS trend by month, Promoter CLV).      ║
-- ║  Including both in the NCI leaf pages creates a covering index for these  ║
-- ║  patterns — no Key Lookup back to the clustered PK page is needed.       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE NONCLUSTERED INDEX [IX_FactCustomerSurvey_CustomerKey]
-- NONCLUSTERED: secondary B-tree index. Physical row order (clustered on
-- SurveyResponseID IDENTITY) is undisturbed — this adds a parallel lookup path.
    ON [gen].[FactCustomerSurvey] ([CustomerKey])
    -- Index key: CustomerKey — the most common JOIN column in satisfaction analysis
    -- (e.g., join FactCustomerSurvey to FactOnlineSales on CustomerKey for CLV vs NPS).
    INCLUDE ([NPSScore], [CSATScore], [SurveyDateKey], [SurveyTrigger], [WouldRecommend]);
    -- INCLUDE columns: appended to index leaf pages (not key columns).
    -- Any query filtering by CustomerKey and reading any of these five columns
    -- is fully satisfied from NCI leaf pages — the "covering index" pattern.
GO

PRINT '  → Index IX_FactCustomerSurvey_CustomerKey created.';
GO


-- ============================================================================
-- RESET NOEXEC — ensures verification suite always executes
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 6 — SET NOEXEC OFF RESET                                     ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  Resets the session execution state so that the verification queries     ║
-- ║  below always run regardless of whether a pre-check fired SET NOEXEC ON. ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- Unconditionally re-enables execution for all subsequent batches.
-- Project-wide rule: always appears after the last DML block and before
-- the verification suite to guarantee V1–V5 run on every execution.
GO


-- ============================================================================
-- VERIFICATION SUITE (V1 – V5)
-- Run all checks after STEP 3 completes. All "expect 0" rows must be 0.
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 7 — VERIFICATION SUITE (V1 – V5)                             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                           ║
-- ║  PURPOSE                                                                  ║
-- ║  Five verification queries confirm correctness at multiple levels:       ║
-- ║  V1: Row count and grain integrity (directional + exact constraint check)║
-- ║  V2: NPS distribution — Promoter/Passive/Detractor mix (directional)    ║
-- ║  V3: CSAT distribution — score spread and NPS correlation (directional) ║
-- ║  V4: NPS vs behavior correlation — the engineered signal (directional)   ║
-- ║  V5: Referential integrity and data quality (all must return 0 — exact) ║
-- ║                                                                           ║
-- ║  DETERMINISM NOTES                                                        ║
-- ║  V5 integrity checks and V1 DuplicateGrainRows are EXACT (must be 0).   ║
-- ║  V1 TotalRows, V2–V4 distributions are APPROXIMATE — NEWID() noise       ║
-- ║  varies per run. Verify direction of patterns, not exact values.         ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  gen.FactCustomerSurvey — Verification Suite';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';


-- ----------------------------------------------------------------------------
-- V1 — ROW COUNT & GRAIN INTEGRITY
-- Total rows = Post-Purchase rows (all sampled) + Quarterly-eligible rows
-- + Annual-eligible rows. DuplicateGrainRows must be exactly 0.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — ROW COUNT & GRAIN INTEGRITY                                       │
-- │                                                                         │
-- │  EXPECTED OUTPUT (directional — varies per run due to NEWID() sampling):│
-- │  ┌──────────────────────────────────────────────────────────┬────────┐  │
-- │  │ Metric                                                   │ Value  │  │
-- │  ├──────────────────────────────────────────────────────────┼────────┤  │
-- │  │ TotalResponses        (indicative range: 10k–40k)        │  ~    │  │
-- │  │ UniqueCustomersSurveyed (TotalResponses / ~1.5 avg)      │  ~    │  │
-- │  │ PostPurchaseRows       (should = UniqueCustomersSurveyed) │  ~    │  │
-- │  │ QuarterlyRows         (subset with tenure>200, 2+ orders)│  ~    │  │
-- │  │ AnnualRows            (subset with tenure>380, 3+ orders)│  ~    │  │
-- │  │ DuplicateGrainRows    (MUST = 0 — enforced by UNIQUE)    │   0   │  │
-- │  └──────────────────────────────────────────────────────────┴────────┘  │
-- │                                                                         │
-- │  ✗ TotalResponses = 0: NEWID() sampling produced 0 rows — re-run.      │
-- │  ✗ DuplicateGrainRows > 0: UNIQUE constraint violation — pipeline bug. │
-- │  ✗ QuarterlyRows = 0: TenureDays > 200 filter too restrictive — check  │
-- │    dbo.FactOnlineSales date range vs @MaxDate.                          │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V1: Row count and grain integrity';

SELECT
    COUNT(*)                                                AS [TotalResponses],
    -- Total survey response rows. Varies per run due to probabilistic sampling.

    COUNT(DISTINCT [CustomerKey])                           AS [UniqueCustomersSurveyed],
    -- Distinct customers with at least one survey response.
    -- Should be ~15–30% of total customers in CustomerSatisfactionProfile.

    SUM(CASE WHEN [SurveyTrigger] = 'Post-Purchase' THEN 1 ELSE 0 END)
                                                            AS [PostPurchaseRows],
    -- Should equal UniqueCustomersSurveyed — every sampled customer gets one.

    SUM(CASE WHEN [SurveyTrigger] = 'Quarterly'     THEN 1 ELSE 0 END)
                                                            AS [QuarterlyRows],
    -- Subset: customers with tenure > 200 days and 2+ orders.

    SUM(CASE WHEN [SurveyTrigger] = 'Annual'        THEN 1 ELSE 0 END)
                                                            AS [AnnualRows],
    -- Subset: customers with tenure > 380 days and 3+ orders.

    COUNT(*) - COUNT(DISTINCT CAST([CustomerKey] AS BIGINT) * 100
                   + CASE [SurveyTrigger]
                         WHEN 'Post-Purchase' THEN 1
                         WHEN 'Quarterly'     THEN 2
                         WHEN 'Annual'        THEN 3
                         ELSE 0
                     END)                                   AS [DuplicateGrainRows]
    -- ⚠ BEST PRACTICE — COMPOSITE GRAIN DUPLICATE DETECTION:
    -- Encodes (CustomerKey, SurveyTrigger) as a single BIGINT key:
    -- CustomerKey * 100 + TriggerOrdinal. BIGINT prevents INT overflow for large keys.
    -- COUNT(*) - COUNT(DISTINCT encoded_key) = 0 → no duplicates.
    -- The UNIQUE constraint should guarantee this, but explicit surfacing aids students.
FROM [gen].[FactCustomerSurvey];


-- ----------------------------------------------------------------------------
-- V2 — NPS DISTRIBUTION
-- Verifies the behavioral scoring model produced a realistic NPS mix.
-- Expected rough distribution: ~35–45% Promoters, ~25–35% Passives,
-- ~25–35% Detractors (typical retail NPS profile). Also computes the
-- actual NPS score using the standard formula.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — NPS DISTRIBUTION (Promoter/Passive/Detractor Mix + NPS Score)    │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate — directional):                           │
-- │  ┌─────────────────────┬──────────┬───────────┬──────────────────────┐  │
-- │  │ NPSCategory         │ Count    │ Pct       │ Notes                │  │
-- │  ├─────────────────────┼──────────┼───────────┼──────────────────────┤  │
-- │  │ Promoter (9–10)     │ largest  │ ~35–45%   │ High-freq customers  │  │
-- │  │ Passive  (7–8)      │ medium   │ ~25–35%   │ Mid-tier customers   │  │
-- │  │ Detractor (0–6)     │ smallest │ ~25–35%   │ Churned/high-return  │  │
-- │  └─────────────────────┴──────────┴───────────┴──────────────────────┘  │
-- │  Computed NPS: DIVIDE(SUM(NPSContribution), COUNT(*)) * 100             │
-- │  Expected NPS range: +5 to +25 (realistic retail NPS benchmark).       │
-- │                                                                         │
-- │  ✗ Promoters > 70%: SatisfactionScore thresholds are too generous.     │
-- │  ✗ Detractors > 60%: SatisfactionScore thresholds are too punitive.    │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V2: NPS distribution and computed NPS score';

SELECT
    [NPSCategory],
    -- Reads from the PERSISTED computed column — no CASE re-evaluation needed.

    COUNT(*)                                                AS [ResponseCount],
    -- Count of responses in each NPS category.

    CAST(
        COUNT(*) * 100.0
        / NULLIF(SUM(COUNT(*)) OVER (), 0)
    AS DECIMAL(5,2))                                        AS [PctOfTotal],
    -- Percentage of all responses in this category.
    -- NULLIF guard on window sum denominator — defensive but correct pattern.

    CAST(AVG(CAST([NPSScore] AS FLOAT)) AS DECIMAL(4,2))    AS [AvgNPSScore],
    -- Average NPS score within the category. Promoter avg should be ~9.4–9.7.
    -- CAST NPSScore to FLOAT before AVG: TINYINT / INT division would truncate.

    -- Inline NPS calculation per category (for the NPSContribution formula reference):
    SUM(CASE
            WHEN [NPSScore] >= 9 THEN  1
            WHEN [NPSScore] >= 7 THEN  0
            ELSE                      -1
        END)                                                AS [SumContributions]
    -- NPSContribution per category: +1 per Promoter, 0 per Passive, -1 per Detractor.
    -- SUM(SumContributions) across all categories / COUNT(*) * 100 = NPS score.
    -- This is the DAX formula in fact.vCustomerSurvey:
    -- [NPS Score] = DIVIDE(SUM([NPSContribution]), COUNT([SurveyResponseID])) * 100
FROM [gen].[FactCustomerSurvey]
GROUP BY [NPSCategory]
ORDER BY [AvgNPSScore] DESC;
-- Descending average score: Promoter first, Detractor last.


-- Also compute the single aggregated NPS number:
SELECT
    'Computed NPS Score'                                    AS [Metric],
    CAST(
        SUM(CASE WHEN [NPSScore] >= 9 THEN  1.0
                 WHEN [NPSScore] >= 7 THEN  0.0
                 ELSE                       -1.0
             END)
        / NULLIF(COUNT(*), 0) * 100
    AS DECIMAL(5,1))                                        AS [Value]
    -- Standard NPS formula: (Promoters − Detractors) / Total × 100.
    -- Implemented as: SUM(contributions) / COUNT(*) × 100 — mathematically equivalent
    -- and simpler to express as a single aggregation over the contribution column.
FROM [gen].[FactCustomerSurvey];


-- ----------------------------------------------------------------------------
-- V3 — CSAT DISTRIBUTION
-- Verifies the CSAT scoring model and its correlation with NPS.
-- Expected: Promoters predominantly score CSAT 4–5. Detractors score 1–3.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — CSAT DISTRIBUTION AND NPS CORRELATION                            │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate):                                         │
-- │  ┌──────────────┬───────────────┬───────────────────────────────────┐   │
-- │  │ NPSCategory  │ AvgCSATScore  │ Notes                             │   │
-- │  ├──────────────┼───────────────┼───────────────────────────────────┤   │
-- │  │ Promoter     │ ~4.3–4.8      │ Predominantly Satisfied (4–5)     │   │
-- │  │ Passive      │ ~3.0–3.8      │ Mix of Neutral and Satisfied      │   │
-- │  │ Detractor    │ ~1.5–2.5      │ Predominantly Dissatisfied (1–3)  │   │
-- │  └──────────────┴───────────────┴───────────────────────────────────┘   │
-- │  ✗ Promoter AvgCSAT < 3: CSAT CROSS APPLY correlation logic is wrong.  │
-- │  ✗ Detractor AvgCSAT > 4: Same — CSAT/NPS correlation has a defect.   │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V3: CSAT distribution by NPS category (correlation check)';

SELECT
    [NPSCategory],
    -- PERSISTED computed column — read directly.

    COUNT(*)                                                AS [ResponseCount],
    CAST(AVG(CAST([CSATScore] AS FLOAT)) AS DECIMAL(4,2))   AS [AvgCSATScore],
    -- Average CSAT per NPS category. Correlated but independent due to separate
    -- noise draws in CTE 5 Stage 2.

    SUM(CASE WHEN [CSATCategory] = 'Satisfied'     THEN 1 ELSE 0 END)
                                                            AS [SatisfiedCount],
    -- Reads from the PERSISTED CSATCategory computed column.

    SUM(CASE WHEN [CSATCategory] = 'Neutral'       THEN 1 ELSE 0 END)
                                                            AS [NeutralCount],

    SUM(CASE WHEN [CSATCategory] = 'Dissatisfied'  THEN 1 ELSE 0 END)
                                                            AS [DissatisfiedCount],

    CAST(
        SUM(CASE WHEN [WouldRecommend] = 1 THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0) * 100
    AS DECIMAL(5,2))                                        AS [PctWouldRecommend]
    -- % that would recommend Contoso within each NPS category.
    -- Expected: 100% for Promoters (NPSScore >= 9 → WouldRecommend = 1 always).
    -- If this is not 100% for Promoters, the WouldRecommend derivation has a defect.
FROM [gen].[FactCustomerSurvey]
GROUP BY [NPSCategory]
ORDER BY [AvgCSATScore] DESC;


-- ----------------------------------------------------------------------------
-- V4 — NPS vs PURCHASE BEHAVIOR CORRELATION
-- This is the KEY validation of the behavioral scoring model.
-- Promoters should have materially higher purchase frequency, spend, and
-- lower return rates than Detractors — the correlations engineered in CTE 2.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — ENGINEERED CORRELATION: NPS vs BEHAVIORAL METRICS                │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERN (the "discoverable insight" for students):│
-- │  ┌─────────────────┬──────────────┬────────────────┬─────────────────┐  │
-- │  │ NPSCategory     │ AvgOrders    │ AvgSpend       │ AvgReturnRate   │  │
-- │  ├─────────────────┼──────────────┼────────────────┼─────────────────┤  │
-- │  │ Promoter        │ Highest      │ Highest        │ Lowest          │  │
-- │  │ Passive         │ Middle       │ Middle         │ Middle          │  │
-- │  │ Detractor       │ Lowest       │ Lowest         │ Highest         │  │
-- │  └─────────────────┴──────────────┴────────────────┴─────────────────┘  │
-- │  ✗ Flat results across categories: scoring model has no discrimination. │
-- │  ✗ Detractor AvgOrders > Promoter: SatisfactionScore formula inverted. │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V4: NPS vs purchase behavior correlation (Promoters must lead)';

WITH AggregatedSales AS (
    SELECT 
        [CustomerKey],
        COUNT(DISTINCT [SalesOrderNumber]) AS [TotalOrders],
        SUM([SalesAmount])                 AS [TotalSpend],
        CASE WHEN SUM([SalesQuantity]) > 0 
             THEN CAST(SUM([ReturnQuantity]) AS FLOAT) / SUM([SalesQuantity]) 
             ELSE 0.0 
        END                                AS [ReturnRate]
    FROM [dbo].[FactOnlineSales]
    GROUP BY [CustomerKey]
)
SELECT
    [s].[NPSCategory],
    COUNT(DISTINCT [s].[CustomerKey])                       AS [UniqueCustomers],
    CAST(AVG([b].[TotalOrders])  AS DECIMAL(6,2))           AS [AvgOrders],
    CAST(AVG([b].[TotalSpend])   AS DECIMAL(12,2))          AS [AvgLifetimeSpend],
    CAST(AVG([b].[ReturnRate])   AS DECIMAL(5,4))           AS [AvgReturnRate]
FROM [gen].[FactCustomerSurvey]  AS [s]
INNER JOIN AggregatedSales AS [b] 
    ON [s].[CustomerKey] = [b].[CustomerKey]
GROUP BY [s].[NPSCategory]
ORDER BY [AvgOrders] DESC;


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
-- │  │ Duplicate (CustomerKey, SurveyTrigger) pairs         │    0     │    │
-- │  │ NPSScore out of range (0–10)                         │    0     │    │
-- │  │ CSATScore out of range (1–5)                         │    0     │    │
-- │  │ WouldRecommend mismatch (NPSScore >= 9 must be 1)    │    0     │    │
-- │  │ SurveyDate after project MaxDate (2009-12-31)        │    0     │    │
-- │  │ Invalid SurveyTrigger value                          │    0     │    │
-- │  └──────────────────────────────────────────────────────┴──────────┘    │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V5: Referential integrity and data quality (all expect 0)';

SELECT
    'Orphan CustomerKeys'                           AS [Check],
    COUNT(*)                                        AS [Value]
FROM [gen].[FactCustomerSurvey]  AS [s]
WHERE NOT EXISTS (
    -- NOT EXISTS anti-join: detects CustomerKey values with no matching DimCustomer row.
    -- The FK constraint prevents this at INSERT, but explicit surfacing is correct
    -- verification discipline — consistent with the V5 pattern in all prior scripts.
    SELECT 1 FROM [dbo].[DimCustomer]  AS [c]
    WHERE [c].[CustomerKey] = [s].[CustomerKey]
)

UNION ALL

SELECT
    'Duplicate (CustomerKey, SurveyTrigger) pairs',
    COUNT(*) - COUNT(DISTINCT CAST([CustomerKey] AS BIGINT) * 100
               + CASE [SurveyTrigger]
                     WHEN 'Post-Purchase' THEN 1
                     WHEN 'Quarterly'     THEN 2
                     WHEN 'Annual'        THEN 3
                     ELSE 0
                 END)
    -- Same BIGINT composite grain key technique from V1 — identical encoding ensures
    -- the V5 check is mathematically equivalent to the V1 DuplicateGrainRows metric.
FROM [gen].[FactCustomerSurvey]

UNION ALL

SELECT 'NPSScore out of range (0–10)',
    SUM(CASE WHEN [NPSScore] NOT BETWEEN 0 AND 10 THEN 1 ELSE 0 END)
    -- CHECK constraint should prevent this — this check confirms it worked.
FROM [gen].[FactCustomerSurvey]

UNION ALL

SELECT 'CSATScore out of range (1–5)',
    SUM(CASE WHEN [CSATScore] NOT BETWEEN 1 AND 5 THEN 1 ELSE 0 END)
    -- CHECK constraint should prevent this — same rationale as NPSScore check above.
FROM [gen].[FactCustomerSurvey]

UNION ALL

SELECT 'WouldRecommend mismatch (NPSScore >= 9 must = 1)',
    SUM(CASE
            WHEN [NPSScore] >= 9 AND [WouldRecommend] = 0 THEN 1
            -- A Promoter (NPS >= 9) with WouldRecommend = 0 is a logic defect.
            WHEN [NPSScore] <  9 AND [WouldRecommend] = 1 THEN 1
            -- A non-Promoter (NPS < 9) with WouldRecommend = 1 is a logic defect.
            ELSE 0
        END)
    -- Validates the WouldRecommend BIT derivation in CTE 5.
FROM [gen].[FactCustomerSurvey]

UNION ALL

SELECT 'SurveyDate after project MaxDate (2009-12-31)',
    SUM(CASE WHEN [SurveyDate] > '2009-12-31' THEN 1 ELSE 0 END)
    -- Validates the LEAST() date clamp applied in CTE 4 SurveyInstances.
    -- Non-zero means LEAST() failed to cap dates for late-cohort customers.
FROM [gen].[FactCustomerSurvey]

UNION ALL

SELECT 'Invalid SurveyTrigger value',
    SUM(CASE WHEN [SurveyTrigger] NOT IN ('Post-Purchase', 'Quarterly', 'Annual')
             THEN 1 ELSE 0 END)
    -- Validates that only the three defined trigger values appear in the table.
    -- A non-zero result means the UNION ALL branches in CTE 4 contain an unlisted value.
FROM [gen].[FactCustomerSurvey];
GO


PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 06 completed successfully.';
PRINT '  Table created:  [gen].[FactCustomerSurvey]';
PRINT '  Index created:  [IX_FactCustomerSurvey_CustomerKey]';
PRINT '';
PRINT '  Next steps:';
PRINT '    Script 07 → gen.OnlineReturnEvents   (depends on Script 01 only)';
PRINT '    Script 08 → gen.PhysicalReturnEvents (depends on Script 01 only)';
PRINT '    Scripts 07 and 08 can run in parallel with each other.';
PRINT '════════════════════════════════════════════════════════════════';
GO
