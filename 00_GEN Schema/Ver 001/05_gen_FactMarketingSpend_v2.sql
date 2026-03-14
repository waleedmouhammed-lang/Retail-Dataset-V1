/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║          SCRIPT 05: gen.FactMarketingSpend — MONTHLY MARKETING SPEND         ║
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
  This script generates gen.FactMarketingSpend — one row per calendar month
  per acquisition channel containing the marketing investment, funnel metrics,
  and actual new-customer results for that month.

  The Contoso source has no marketing cost data whatsoever. Without this table
  the entire CMO efficiency layer is dark: no CAC, no ROAS, no CPC, no CPA,
  no budget allocation analysis, and no payback-period modelling.

  This is the MOST ANALYTICALLY COMPLEX generation script in the project.
  Unlike Scripts 02–04 (which assign properties to existing rows), this script
  generates NEW time-series rows that did not exist anywhere in the source.
  All spend figures must be internally consistent with the actual customer
  acquisition counts produced by Script 02.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Business Questions Unlocked                                            │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  CMO:  What is our customer acquisition cost (CAC) by channel?          │
  │  CMO:  What is our paid vs. organic spend mix?                          │
  │  CMO:  How does spend align with seasonality peaks?                     │
  │  CMO:  Which channel delivers the lowest CPC?                           │
  │  CMO:  How has our channel mix investment shifted over time?            │
  │  CFO:  What is our total marketing spend as % of revenue?               │
  │  CFO:  What is the CAC-to-CLV payback period per channel?               │
  │  CEO:  What is ROAS by channel? Where should we invest next quarter?    │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  GRAIN AND SCOPE
--------------------------------------------------------------------------------
  Grain    : One row per (calendar month × acquisition channel).
  Scope    : All months spanned by gen.CustomerAcquisition (raw source range
             2007–2009). All 7 acquisition channels — including Direct (Key=5)
             which records $0 spend.
  Row count: 7 channels × ~36 months ≈ 252 rows total (small but dense).

  ⚠  ZERO-SPEND CHANNELS — Direct (Key=5)
  Direct-channel customers are brand-aware visitors who arrive without any
  paid stimulus. MonthlySpend, Impressions, and Clicks are all 0 for
  Direct rows. This is correct — Direct = no marketing investment.
  DAX measures must guard against divide-by-zero when computing CAC and CPC
  for Direct rows. Use DIVIDE() with an alternate result of BLANK().

--------------------------------------------------------------------------------
  TEMPORAL SHIFT — ARCHITECTURE NOTE
--------------------------------------------------------------------------------
  YearMonth and MonthStartDateKey are stored in the RAW source date range
  (200701–200912, 20070101–20091231). No +16 year offset is applied at the
  [gen] layer.

  The +16 year temporal shift is applied EXCLUSIVELY at the fact view layer
  (fact.vMarketingSpend), consistent with the project-wide principle that all
  temporal transformations happen at the semantic layer, never at the physical
  data layer.

  Channel benchmark rates (CPM, CTR, CAC midpoints) reflect the 2023–2025
  business era, not the raw 2007–2009 source dates. This is correct: the
  business question is "what would marketing have cost in 2023–2025 given
  the actual customer volumes we observe?"

--------------------------------------------------------------------------------
  CALIBRATION MODEL — DESIGN RATIONALE
--------------------------------------------------------------------------------
  Spend is calibrated BACKWARDS from actual acquisition data to ensure
  internal consistency. The pipeline has 6 stages:

  1. Count actual new customers per month per channel
     (from gen.CustomerAcquisition — ground truth).

  2. Multiply by the channel CAC midpoint from gen.DimAcquisitionChannel
     to derive a baseline spend. This anchors marketing spend to the exact
     CAC ranges established in Script 01, ensuring cross-table consistency.

  3. Apply a seasonality multiplier derived from actual monthly revenue
     from dbo.FactOnlineSales. Marketing spend follows revenue patterns —
     higher spend in Q4, lower in Q1. SeasonIndex = MonthRevenue /
     AVG(MonthRevenue) OVER () — 1.0 = average month.

  4. Apply ±20% random noise to prevent suspiciously uniform data.
     Pattern: (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0).
     This produces values 0.80–1.20 with uniform distribution.

  5. Derive Impressions from MonthlySpend using channel-specific CPM rates.
     Non-impression channels (Email, Referral, Organic) derive Impressions
     from estimated audience reach multipliers.

  6. Derive Clicks from Impressions using channel-specific CTR rates.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Channel Benchmark Rates — 2023–2025 Era                                │
  ├──────────────────┬──────────────┬──────────┬────────────────────────── ┤
  │  Channel (Key)   │  CAC Range   │  CPM     │  CTR     │  Notes         │
  ├──────────────────┼──────────────┼──────────┼──────────┼─────────────── ┤
  │  Organic (1)     │  $0 – $5     │  N/A     │  ~3.5%   │  SEO/content   │
  │  Paid Search (2) │  $25 – $40   │  ~$40    │  ~3.5%   │  Google Ads    │
  │  Social Media(3) │  $15 – $35   │  ~$12    │  ~0.9%   │  Meta/TikTok   │
  │  Email Mktg (4)  │  $5 – $12    │  N/A     │  ~3.5%   │  Owned list    │
  │  Direct (5)      │  $0          │  N/A     │  N/A     │  Zero spend    │
  │  Referral (6)    │  $10 – $25   │  N/A     │  ~5.0%   │  Warm traffic  │
  │  Affiliate (7)   │  $8 – $18    │  ~$15    │  ~1.5%   │  Creator econ  │
  └──────────────────┴──────────────┴──────────┴──────────┴────────────────┘

  CAC midpoints used in spend calculation (directly from gen.DimAcquisitionChannel):
    Organic=$2.50, Paid Search=$32.50, Social=$25.00, Email=$8.50,
    Direct=$0.00, Referral=$17.50, Affiliate=$13.00

--------------------------------------------------------------------------------
  OUTPUT TABLE — gen.FactMarketingSpend
--------------------------------------------------------------------------------
  Column                  Type              Notes
  ──────────────────────────────────────────────────────────────────────────
  MarketingSpendID        INT IDENTITY PK   Auto surrogate — resets on re-run
  YearMonth               INT NOT NULL      YYYYMM raw source (no +16 shift)
  MonthStartDateKey       INT NOT NULL      YYYYMMDD first of month — FK → DimDate
  AcquisitionChannelKey   INT NOT NULL FK   → gen.DimAcquisitionChannel
  MonthlySpend            MONEY NOT NULL    Total channel spend for the month
  Impressions             INT NOT NULL      Ad impressions or audience reach
  Clicks                  INT NOT NULL      Click-throughs to site
  NewCustomersAcquired    INT NOT NULL      Actual from gen.CustomerAcquisition
  CostPerClick            COMPUTED          MonthlySpend / Clicks (NULL if Clicks=0)
  CostPerAcquisition      COMPUTED          MonthlySpend / NewCustomers (NULL if 0)
  ClickThroughRate        COMPUTED          Clicks / Impressions × 100 (NULL if 0)

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run order    : Script 05 — MUST run after Script 02
  Dependencies : [gen] schema (Script 00), gen.DimAcquisitionChannel (Script 01),
                 gen.CustomerAcquisition (Script 02), dbo.FactOnlineSales (source)
  Impact       : Creates ONE new table in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — idempotent DROP / CREATE guard on the table.
  Can parallel  : NO — Script 02 must complete first.

================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/


-- ============================================================================
-- PRE-CHECKS: Verify all dependencies before any DDL executes
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECKS (4 checks)                  ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Four sequential dependency checks run before any DDL executes:            ║
-- ║  (1) [gen] schema                  → Script 00 required                    ║
-- ║  (2) gen.DimAcquisitionChannel     → Script 01 required (7 rows)           ║
-- ║  (3) gen.CustomerAcquisition       → Script 02 required (MUST COMPLETE)    ║
-- ║  (4) dbo.FactOnlineSales           → Contoso source required               ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — STRICT DEPENDENCY ORDER                        ║
-- ║  Script 05 is the ONLY script in the build queue that cannot run in        ║
-- ║  parallel with others. It requires gen.CustomerAcquisition to be fully     ║
-- ║  populated (Script 02 complete) because it reads actual acquisition        ║
-- ║  counts from that table to derive spend figures. If Script 02 is partial   ║
-- ║  or empty, all spend amounts will be wrong or zero.                        ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT ON SUCCESS (4 green ticks in Messages tab):               ║
-- ║  ✓ [gen] schema confirmed.                                                 ║
-- ║  ✓ [gen].[DimAcquisitionChannel] confirmed.                               ║
-- ║  ✓ [gen].[CustomerAcquisition] confirmed.                                  ║
-- ║  ✓ [dbo].[FactOnlineSales] confirmed.                                      ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── CHECK 1 OF 4: [gen] Schema ───────────────────────────────────────────────

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
-- GO: T-SQL batch separator. Sends the preceding block to the engine as one atomic unit.
-- Each pre-check is its own GO batch so SET NOEXEC ON propagates correctly across checks.

-- ── CHECK 2 OF 4: gen.DimAcquisitionChannel ──────────────────────────────────

IF OBJECT_ID('[gen].[DimAcquisitionChannel]', 'U') IS NULL
-- OBJECT_ID(): returns the internal object ID of a named database object, or NULL if not found.
-- Second argument 'U': filters to User tables only — avoids false matches to views or procedures.
-- IS NULL: evaluates to TRUE when the table does not yet exist.
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [gen].[DimAcquisitionChannel] not found. Run Script 01 first.');
    THROW 50000, @ErrorMessage, 1;
END
ELSE
BEGIN
    PRINT '✓ [gen].[DimAcquisitionChannel] confirmed.';
END
GO

-- ── CHECK 3 OF 4: gen.CustomerAcquisition ────────────────────────────────────

IF OBJECT_ID('[gen].[CustomerAcquisition]', 'U') IS NULL
-- Critical dependency: this script reads ACTUAL acquisition counts from this table.
-- If missing or empty, all CalcSpend values will be wrong or zero.
BEGIN
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('FATAL: [gen].[CustomerAcquisition] not found. Run Script 02 first.');
    THROW 50000, @ErrorMessage, 1;
END
ELSE
BEGIN
    PRINT '✓ [gen].[CustomerAcquisition] confirmed.';
END
GO

-- ── CHECK 4 OF 4: dbo.FactOnlineSales ────────────────────────────────────────

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


-- ============================================================================
-- STEP 1: Create target table (idempotent — drops and recreates if exists)
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — STEP 1: TARGET TABLE DEFINITION                             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Drops (if exists) and recreates gen.FactMarketingSpend — an 11-column    ║
-- ║  monthly aggregate table with one row per (month × channel) combination.  ║
-- ║                                                                             ║
-- ║  TABLE DESIGN DECISIONS                                                     ║
-- ║                                                                             ║
-- ║  IDENTITY(1,1) SURROGATE KEY                                               ║
-- ║  Unlike the other gen tables which use natural keys (CustomerKey,          ║
-- ║  SalesOrderNumber), this table has no natural unique key in the source.    ║
-- ║  The combination (YearMonth, AcquisitionChannelKey) is the logical grain   ║
-- ║  and a UNIQUE constraint enforces it — but IDENTITY provides a clean       ║
-- ║  integer PK for Power BI relationship use. Note: MarketingSpendID resets   ║
-- ║  from 1 every time the script re-runs (DROP + recreate). This is correct   ║
-- ║  for a generation script — fact.vMarketingSpend will always have valid IDs ║
-- ║  because it reads from the live table, not stored IDs.                     ║
-- ║                                                                             ║
-- ║  YEARMONTH vs MONTHSTARTDATEKEY — TWO DATE COLUMNS                         ║
-- ║  • YearMonth INT (YYYYMM): optimised for period-based calculations in DAX  ║
-- ║    (e.g., filter WHERE YearMonth = 202309). Not the FK — this format does  ║
-- ║    not exist as a date key in dim.vDate.                                   ║
-- ║  • MonthStartDateKey INT (YYYYMMDD): the FK to dim.vDate. Points to the    ║
-- ║    first day of each month. The Power BI relationship is on this column.   ║
-- ║    The +16 year shift in fact.vMarketingSpend makes this 20230101–20251201.║
-- ║                                                                             ║
-- ║  COMPUTED COLUMNS — WHY IN THE TABLE, NOT JUST IN THE VIEW                 ║
-- ║  CostPerClick, CostPerAcquisition, and ClickThroughRate are virtual        ║
-- ║  computed columns (not PERSISTED). SQL Server evaluates them on read from  ║
-- ║  the stored columns with zero storage cost. They appear as regular columns ║
-- ║  in fact.vMarketingSpend — students can use them in Power BI directly      ║
-- ║  without needing to write DAX division expressions with DIVIDE() guards.   ║
-- ║  NULL is returned when the denominator is 0 (Direct channel).              ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  1. The UNIQUE constraint on (YearMonth, AcquisitionChannelKey) enforces   ║
-- ║     the grain at the database level. If the CTE produces duplicate         ║
-- ║     (month, channel) pairs — which it should not — the INSERT will fail    ║
-- ║     with a constraint violation rather than silently creating bad data.    ║
-- ║  2. MonthlySpend is MONEY, not DECIMAL. All CAC midpoint arithmetic in     ║
-- ║     the CTE uses MONEY * MONEY / MONEY patterns. Avoid mixing MONEY with   ║
-- ║     FLOAT — it can cause rounding errors in financial calculations.        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── Drop guard (idempotent) ───────────────────────────────────────────────────

DROP TABLE IF EXISTS [gen].[FactMarketingSpend];
PRINT '→ Existing [gen].[FactMarketingSpend] dropped (if it existed).';
GO

-- ── Table creation ────────────────────────────────────────────────────────────

CREATE TABLE [gen].[FactMarketingSpend]
-- DDL to create the physical table in the [gen] schema (synthetic extension layer).
-- Fully bracketed [schema].[Table] notation is the project-wide standard.
(
    -- ── Identity / Primary Key ─────────────────────────────────────────────
    [MarketingSpendID]      INT IDENTITY(1,1)   NOT NULL,
    -- INT IDENTITY(1,1): auto-incrementing surrogate PK, starting at 1, step 1.
    -- No natural unique key exists (no source rows) — IDENTITY fills that role.
    -- Resets from 1 on every DROP + CREATE re-run; Power BI relationships use this
    -- column only at load time, so reset is harmless.

    -- ── Date dimension columns ─────────────────────────────────────────────
    [YearMonth]             INT                 NOT NULL,
    -- YYYYMM integer (e.g., 200701). Raw source era — no +16 year shift at [gen] layer.
    -- Used as a period filter in DAX (e.g., FILTER(... , [YearMonth] = 202309)).
    -- NOT a FK to dim.vDate — that role belongs to MonthStartDateKey below.

    [MonthStartDateKey]     INT                 NOT NULL,
    -- YYYYMMDD integer pointing to the first day of the month (e.g., 20070101).
    -- FK to dim.vDate — the +16 year shift applied in fact.vMarketingSpend
    -- converts this to 20230101–20251201 to align with the 2023–2025 analytical era.

    [AcquisitionChannelKey] INT                 NOT NULL,
    -- Integer FK to gen.DimAcquisitionChannel. Not a TINYINT — INT used for
    -- consistency with the FK target column type in DimAcquisitionChannel.

    -- ── Spend metric ──────────────────────────────────────────────────────
    [MonthlySpend]          DECIMAL(19,4)               NOT NULL,
    -- MONEY type (4 decimal places, 8-byte storage) for all financial amounts.
    -- Project-wide rule: never mix MONEY with FLOAT — causes silent rounding errors.
    -- $0.00 for Direct channel (Key=5) by design.

    -- ── Funnel metrics ────────────────────────────────────────────────────
    [Impressions]           INT                 NOT NULL,
    -- Total ad impressions served (CPM channels) or audience reach estimate
    -- (organic/email/referral). 0 for Direct — no paid media placed.

    [Clicks]                INT                 NOT NULL,
    -- Click-throughs from impressions to the Contoso site.
    -- Guaranteed ≥ NewCustomersAcquired by the GREATEST() guard in the INSERT.

    -- ── Actual acquisition metric (from gen.CustomerAcquisition) ──────────
    [NewCustomersAcquired]  INT                 NOT NULL,
    -- Ground-truth count from gen.CustomerAcquisition (Script 02).
    -- This is NOT derived — it is the exact count of customers acquired via
    -- this channel in this month. All spend figures are calibrated to this value.

    -- ── Virtual computed KPI columns ──────────────────────────────────────
    -- NULL for Direct channel (spend=0) and any zero-denominator edge case.
    -- Use DAX DIVIDE([MonthlySpend], [Clicks], BLANK()) for equivalent logic.
    [CostPerClick]          AS CAST(
                                CASE WHEN [Clicks] > 0
                                -- Guard: only compute when denominator is positive.
                                     THEN [MonthlySpend] / [Clicks]
                                     -- Divides MONEY by INT — SQL Server widens INT to MONEY automatically.
                                     ELSE NULL
                                     -- Returns NULL (not 0) for zero-click rows — correct semantic:
                                     -- "no data" not "free clicks".
                                END AS DECIMAL(19,4)),
    -- CAST to DECIMAL(19,4): rounds to 4 decimal places for dollar display.
    -- Non-PERSISTED computed column: zero storage cost, evaluated on read.
    -- Appears as a regular column in fact.vMarketingSpend and Power BI.

    [CostPerAcquisition]    AS CAST(
                                CASE WHEN [NewCustomersAcquired] > 0
                                -- Guard: Direct channel rows will always evaluate to NULL here.
                                     THEN [MonthlySpend] / [NewCustomersAcquired]
                                     ELSE NULL
                                END AS DECIMAL(19,4)),
    -- Same DECIMAL(19,4) pattern as CostPerClick — consistent display precision.

    [ClickThroughRate]      AS CAST(
                                CASE WHEN [Impressions] > 0
                                -- Guard: prevents divide-by-zero for Direct channel (Impressions ≈ 0).
                                     THEN [Clicks] * 100.0 / [Impressions]
                                     -- Multiply by 100.0 (FLOAT literal) to express as a percentage
                                     -- (e.g., 3.50 = 3.5%). The .0 suffix forces floating-point division
                                     -- rather than integer division.
                                     ELSE NULL
                                END AS DECIMAL(5,2)),
    -- DECIMAL(5,2): supports values from 0.00 to 999.99 — sufficient for any CTR %.

    -- ── Constraints ───────────────────────────────────────────────────────
    CONSTRAINT [PK_FactMarketingSpend]
        PRIMARY KEY CLUSTERED ([MarketingSpendID]),
    -- CLUSTERED PK on IDENTITY: default and optimal for append-heavy fact tables.
    -- Rows are physically sorted and stored by MarketingSpendID on disk.

    CONSTRAINT [UQ_FactMarketingSpend_MonthChannel]
        UNIQUE ([YearMonth], [AcquisitionChannelKey]),
    -- ⚠ BEST PRACTICE — GRAIN ENFORCEMENT AT THE DATABASE LEVEL:
    -- A UNIQUE constraint on the composite grain (month × channel) makes it
    -- impossible for the CTE pipeline to silently insert duplicate rows.
    -- Any duplicate produced by a logic defect causes a hard constraint violation
    -- rather than corrupting the analytical dataset without warning.
    -- This is the correct pattern for any fact table with a known composite grain.

    CONSTRAINT [FK_FactMarketingSpend_Channel]
        FOREIGN KEY ([AcquisitionChannelKey])
        REFERENCES [gen].[DimAcquisitionChannel] ([AcquisitionChannelKey])
    -- Referential integrity: the engine rejects any INSERT with an
    -- AcquisitionChannelKey value that does not exist in the dimension table.
    -- Protects against calibration pipeline bugs that generate orphan channel keys.
);
GO

PRINT '  → [gen].[FactMarketingSpend] table created.';
-- Confirmation audit message visible in the Messages tab after successful DDL.
GO


-- ============================================================================
-- STEP 2: Populate via calibration pipeline
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2: 4-STAGE CALIBRATION CTE PIPELINE                    ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Generates all spend, impression, and click values via a 4-CTE pipeline    ║
-- ║  anchored to actual acquisition counts — ensuring every spend figure is    ║
-- ║  internally consistent with what Script 02 produced.                       ║
-- ║                                                                             ║
-- ║  ┌─────────────────────────────────────────────────────────────────────┐   ║
-- ║  │  PIPELINE OVERVIEW                                                  │   ║
-- ║  ├─────────────────────────────────────────────────────────────────────┤   ║
-- ║  │  CTE 1: MonthlyAcquisitions                                         │   ║
-- ║  │    Groups gen.CustomerAcquisition by (YYYYMM, ChannelKey).          │   ║
-- ║  │    Produces: YearMonth, AcquisitionChannelKey, NewCustomers.         │   ║
-- ║  │    This is the GROUND TRUTH that all spend figures are anchored to. │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 2: MonthlyRevenue                                              │   ║
-- ║  │    Groups dbo.FactOnlineSales by YYYYMM to get total revenue.       │   ║
-- ║  │    Used to build the seasonality multiplier.                        │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 3: SeasonalityIndex                                            │   ║
-- ║  │    Computes SeasonIndex = MonthRevenue / AVG(MonthRevenue) OVER (). │   ║
-- ║  │    SeasonIndex = 1.0 → average month. >1.0 → above-average month.  │   ║
-- ║  │    This drives proportionally higher spend in peak revenue months   │   ║
-- ║  │    (Q4 seasonality). NULLIF guards against zero average revenue.    │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 4: SpendCalculation                                            │   ║
-- ║  │    Joins MonthlyAcquisitions × DimAcquisitionChannel × SeasonIndex. │   ║
-- ║  │    Computes CalcSpend = NewCustomers × CACMidpoint × SeasonIndex    │   ║
-- ║  │                         × RandomNoise.                              │   ║
-- ║  │    CACMidpoint comes directly from gen.DimAcquisitionChannel        │   ║
-- ║  │    (EstimatedCACLow + EstimatedCACHigh) / 2.0 — ensuring spend is  │   ║
-- ║  │    anchored to the CAC ranges established in Script 01.             │   ║
-- ║  └─────────────────────────────────────────────────────────────────────┘   ║
-- ║                                                                             ║
-- ║  SPEND FORMULA                                                              ║
-- ║  CalcSpend = NewCustomers × CACMidpoint × SeasonIndex × Noise             ║
-- ║  where Noise = (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)           ║
-- ║  Noise range: 0.80–1.20 (±20%) uniform distribution.                      ║
-- ║                                                                             ║
-- ║  IMPRESSIONS DERIVATION                                                     ║
-- ║  Channels with CPM-based pricing (Paid Search, Social, Affiliate):         ║
-- ║    Impressions = (CalcSpend / CPM_rate) × 1000 × Noise(0.85–1.15)        ║
-- ║  Channels without paid impressions (Organic, Email, Referral, Direct):     ║
-- ║    Impressions = NewCustomers × AudienceMultiplier × Noise                ║
-- ║    (AudienceMultiplier estimates reach per acquired customer)              ║
-- ║                                                                             ║
-- ║  CLICKS DERIVATION                                                          ║
-- ║  Clicks = Impressions × CTR_rate × Noise(0.80–1.20)                       ║
-- ║  CTR rates calibrated for 2023–2025 era:                                  ║
-- ║    Organic Search: 3.5%  │  Paid Search: 3.5%  │  Social: 0.9%           ║
-- ║    Email: 3.5%            │  Referral: 5.0%     │  Affiliate: 1.5%        ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  1. The LEFT JOIN to SeasonalityIndex uses ISNULL(si.SeasonIndex, 1.0).   ║
-- ║     If a month in gen.CustomerAcquisition has no matching revenue record  ║
-- ║     in dbo.FactOnlineSales (e.g., a month with zero online sales), the    ║
-- ║     SeasonIndex defaults to 1.0 — average spend. This is the correct      ║
-- ║     fallback: do not penalise channels for data gaps in the source.       ║
-- ║  2. CACMidpoint for Direct (Key=5) is $0.00 by design. CalcSpend will     ║
-- ║     be $0.00 × anything = $0.00. This propagates correctly to Impressions ║
-- ║     and Clicks (both will be 0 for Direct rows).                          ║
-- ║  3. CHECKSUM(NEWID()) is called ONCE per Impressions derivation and ONCE  ║
-- ║     per Clicks derivation. These are independent random draws — the       ║
-- ║     noise on impressions and the noise on clicks are not correlated.      ║
-- ║     This is intentional and produces realistic variance.                  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- Leading semicolon: defensive T-SQL pattern. If a prior statement in the batch
-- was not terminated with a semicolon, this prevents a syntax error when the CTE
-- keyword WITH is encountered.

-- ────────────────────────────────────────────────────────────────────────────
-- CTE 1: MonthlyAcquisitions
-- Ground-truth customer counts per month per channel.
-- AcquisitionDate is a raw DATE (2007–2009 era, no temporal shift at this layer).
-- ────────────────────────────────────────────────────────────────────────────
-- ;with [MonthlyAcquisitions] AS (
--     SELECT
--         YEAR([ca].[AcquisitionDate]) * 100
--             + MONTH([ca].[AcquisitionDate])     AS [YearMonth],
--         -- Constructs the YYYYMM integer key: YEAR × 100 + MONTH.
--         -- Example: YEAR=2007, MONTH=3 → 2007 × 100 + 3 = 200703.
--         -- Preferred over FORMAT() or CONVERT() for integer output — no string overhead.
--         -- ⚠ BEST PRACTICE: Derive YYYYMM via arithmetic (YEAR*100+MONTH), not FORMAT(),
--         -- to avoid implicit string-to-int conversion cost on large tables.

--         [ca].[AcquisitionChannelKey],
--         -- The channel FK directly from gen.CustomerAcquisition — already an INT,
--         -- no conversion needed. The GROUP BY uses this for the per-channel split.

--         COUNT(*)                                AS [NewCustomers]
--         -- COUNT(*): counts all rows in the group — each row = one customer acquired.
--         -- This is the ground truth anchoring all spend calculations in CTE 4.
--     FROM  [gen].[CustomerAcquisition]  AS [ca]
--     -- Source: gen.CustomerAcquisition built by Script 02. Alias [ca] for brevity.
--     GROUP BY
--         YEAR([ca].[AcquisitionDate]) * 100 + MONTH([ca].[AcquisitionDate]),
--         -- GROUP BY must repeat the full YEAR/MONTH expression — cannot reference
--         -- the column alias [YearMonth] in the same SELECT's GROUP BY clause (T-SQL rule).
--         [ca].[AcquisitionChannelKey]
--         -- Two-column grouping = one row per (YYYYMM × channel) — the target grain.
-- ),

-- -- ────────────────────────────────────────────────────────────────────────────
-- -- CTE 2: MonthlyRevenue
-- -- Actual total revenue per calendar month from the Contoso source.
-- -- DateKey is a DATE column in dbo.FactOnlineSales.
-- -- ────────────────────────────────────────────────────────────────────────────
-- [MonthlyRevenue] AS (
--     SELECT
--         YEAR([f].[DateKey]) * 100
--             + MONTH([f].[DateKey])              AS [YearMonth],
--         -- Same YEAR*100+MONTH arithmetic as CTE 1 — ensures join keys are comparable
--         -- (both YearMonth columns are INT YYYYMM, no type mismatch on the join).

--         SUM([f].[SalesAmount])                  AS [MonthRevenue]
--         -- SUM of SalesAmount aggregated to the month level.
--         -- This monthly revenue figure drives the seasonality multiplier in CTE 3.
--         -- Higher revenue months → higher SeasonIndex → proportionally higher spend.
--     FROM  [dbo].[FactOnlineSales]  AS [f]
--     -- Source: Contoso source fact table (~13M rows). No WHERE filter — full scan
--     -- for a one-time aggregation. The @MinYear/@MaxYear optimisation from Script 03
--     -- is not needed here because this query runs only once per script execution.
--     GROUP BY
--         YEAR([f].[DateKey]) * 100 + MONTH([f].[DateKey])
--         -- Single-column grouping: one row per calendar month across all channels.
-- ),

-- -- ────────────────────────────────────────────────────────────────────────────
-- -- CTE 3: SeasonalityIndex
-- -- SeasonIndex = MonthRevenue / AvgMonthRevenue (1.0 = average).
-- -- Drives proportionally higher spend in peak revenue months (Q4).
-- -- ────────────────────────────────────────────────────────────────────────────
-- [SeasonalityIndex] AS (
--     SELECT
--         [YearMonth],
--         -- Pass-through: needed for the JOIN in CTE 4.

--         [MonthRevenue],
--         -- Pass-through: available for diagnostic queries (e.g., V3 verification).

--         [MonthRevenue]
--             / NULLIF(AVG([MonthRevenue]) OVER (), 0.0)  AS [SeasonIndex]
--         -- ⚠ BEST PRACTICE — NULLIF GUARD ON WINDOW FUNCTION DENOMINATOR:
--         -- AVG([MonthRevenue]) OVER () computes a single grand average across ALL rows
--         -- in MonthlyRevenue (no PARTITION BY = unbounded window).
--         -- NULLIF(..., 0.0): if the average is exactly 0 (all months had $0 revenue —
--         -- an impossible but defensive edge case), NULLIF returns NULL instead of
--         -- allowing a divide-by-zero error.
--         -- Result: 1.0 = average month, >1.0 = above average (Q4 peak),
--         --         <1.0 = below average (Q1–Q2 trough).
--     FROM  [MonthlyRevenue]
--     -- Reads directly from CTE 2 — no need to reference [dbo] again.
-- ),

-- -- ────────────────────────────────────────────────────────────────────────────
-- -- CTE 4: SpendCalculation
-- -- Derives spend, impressions, and clicks for every (month × channel) pair.
-- -- Joins to DimAcquisitionChannel to read CACMidpoint directly from the source.
-- -- ────────────────────────────────────────────────────────────────────────────
-- [SpendCalculation] AS (
--     SELECT
--         [ma].[YearMonth],
--         -- Carry forward the YYYYMM key — needed for the final INSERT and grain check.

--         -- MonthStartDateKey: YYYYMMDD first day of month (no +16 shift here)
--         [ma].[YearMonth] * 100 + 1                                  AS [MonthStartDateKey],
--         -- Constructs the YYYYMMDD date key for the first day of each month.
--         -- Example: YearMonth=200703 → 200703 × 100 + 1 = 20070301.
--         -- The +16 year temporal shift (→ 20230301) is applied later at the
--         -- fact.vMarketingSpend view layer — never at the physical [gen] table level.

--         [ma].[AcquisitionChannelKey],
--         -- FK to gen.DimAcquisitionChannel — carried through to the final INSERT.

--         [ma].[NewCustomers],
--         -- Ground-truth count from CTE 1 — also written as NewCustomersAcquired
--         -- in the target table column. Using the CTE alias here for clarity.

--         -- SeasonIndex: default 1.0 if month has no revenue record
--         ISNULL([si].[SeasonIndex], 1.0)                             AS [SeasonFactor],
--         -- ⚠ BEST PRACTICE — GRACEFUL FALLBACK ON LEFT JOIN NULL:
--         -- The LEFT JOIN to SeasonalityIndex means months with no revenue record
--         -- produce a NULL SeasonIndex. ISNULL defaults this to 1.0 (average spend)
--         -- rather than propagating NULL through the spend formula (NULL × anything = NULL).
--         -- This is the correct fallback: absence of revenue data ≠ zero spend.

--         -- CACMidpoint from DimAcquisitionChannel — canonical source of truth
--         ([ch].[EstimatedCACLow] + [ch].[EstimatedCACHigh]) / 2.0   AS [CACMidpoint],
--         -- ⚠ BEST PRACTICE — ANCHOR SYNTHETIC DATA TO THE DIMENSION TABLE:
--         -- Rather than hardcoding CAC midpoints ($32.50 for Paid Search etc.) inside
--         -- this script, the values are derived dynamically from gen.DimAcquisitionChannel.
--         -- This guarantees cross-table consistency: if the CAC ranges are ever updated
--         -- in Script 01, spend recalibrates automatically on the next run.
--         -- Direct (Key=5): EstimatedCACLow=0.00, EstimatedCACHigh=0.00 → CACMidpoint=$0.00.

--         -- CalcSpend = NewCustomers × CACMidpoint × SeasonIndex × RandomNoise(0.80–1.20)
--         CAST(
--             [ma].[NewCustomers]
--             * (([ch].[EstimatedCACLow] + [ch].[EstimatedCACHigh]) / 2.0)
--             -- CACMidpoint inline for the CAST expression: avoids referencing the alias
--             -- [CACMidpoint] defined in the same SELECT (T-SQL does not allow same-SELECT
--             -- alias references).
--             * ISNULL([si].[SeasonIndex], 1.0)
--             -- SeasonFactor inline: repeated for the same reason as CACMidpoint above.
--             * (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)
--             -- ⚠ BEST PRACTICE — STANDARD PROJECT NOISE PATTERN (±20%):
--             -- NEWID(): generates a unique GUID per row evaluation — true randomness.
--             -- CHECKSUM(): converts the GUID to a deterministic INT (positive or negative).
--             -- ABS(): ensures non-negative value before modulo.
--             -- % 400: maps the integer to the range 0–399.
--             -- / 1000.0: converts to 0.000–0.399.
--             -- + 0.80: shifts the range to 0.800–1.199 (±20% uniform distribution).
--             -- CAST AS MONEY: explicit type to prevent floating-point contamination.
--             -- Direct channel: 0 × $0.00 × anything = $0.00 MONEY — correct.
--         AS DECIMAL(19,4))                                                    AS [CalcSpend]

--     FROM       [MonthlyAcquisitions]          AS [ma]
--     INNER JOIN [gen].[DimAcquisitionChannel]  AS [ch]
--         ON [ma].[AcquisitionChannelKey] = [ch].[AcquisitionChannelKey]
--     -- INNER JOIN to DimAcquisitionChannel: every channel key in gen.CustomerAcquisition
--     -- must exist in gen.DimAcquisitionChannel (7 channels). Any orphan key is dropped —
--     -- the FK constraint on the target table would catch it anyway.
--     LEFT  JOIN [SeasonalityIndex]             AS [si]
--         ON [ma].[YearMonth] = [si].[YearMonth]
--     -- LEFT JOIN: preserves all (month × channel) pairs from MonthlyAcquisitions even
--     -- if a month has no revenue record in dbo.FactOnlineSales. The ISNULL fallback
--     -- above handles the NULL SeasonIndex that results from an unmatched LEFT JOIN row.
-- )

-- -- ────────────────────────────────────────────────────────────────────────────
-- -- FINAL INSERT — projects all 8 stored columns from SpendCalculation
-- -- Impressions and Clicks derived inline using channel-specific rates.
-- -- CASE keys: 1=Organic 2=PaidSearch 3=Social 4=Email 5=Direct 6=Referral 7=Affiliate
-- -- ────────────────────────────────────────────────────────────────────────────
-- INSERT INTO [gen].[FactMarketingSpend]
--     ([YearMonth], [MonthStartDateKey], [AcquisitionChannelKey],
--      [MonthlySpend], [Impressions], [Clicks], [NewCustomersAcquired])
-- -- Explicit column list: only the 7 stored columns are inserted.
-- -- MarketingSpendID is excluded — IDENTITY fills it automatically.
-- -- The 3 computed columns (CostPerClick, CostPerAcquisition, ClickThroughRate)
-- -- are also excluded — they are virtual and have no insertable storage slot.
-- SELECT
--     [sc].[YearMonth],
--     -- YYYYMM raw source date key — written directly to the table (no +16 shift).

--     [sc].[MonthStartDateKey],
--     -- YYYYMMDD first-of-month date key — also stored without +16 shift.
--     -- The temporal shift happens at the fact.vMarketingSpend view layer.

--     [sc].[AcquisitionChannelKey],
--     -- Integer FK — validated by the FK constraint defined on the table.

--     -- ── MonthlySpend ──────────────────────────────────────────────────────
--     -- Floor at 0 to handle any floating-point edge cases on $0 channels.
--    CASE WHEN [sc].[CalcSpend] < 0 THEN CAST(0 AS DECIMAL(19,4))
--     -- ⚠ BEST PRACTICE — DEFENSIVE FLOOR GUARD ON FINANCIAL COLUMNS:
--     -- DECIMAL(19,4) arithmetic should never produce a negative result here, but floating-point
--     -- rounding across DECIMAL(19,4) / FLOAT / INT chains can occasionally yield tiny negatives
--     -- (e.g., -0.0000001). CASE WHEN < 0 floors these to exactly $0.0000 DECIMAL(19,4).
--     -- This is the correct pattern for any synthetic financial column.
--          ELSE [sc].[CalcSpend]
--     END                                                              AS [MonthlySpend],

--     -- ── Impressions ───────────────────────────────────────────────────────
--     -- CPM channels: Impressions = (Spend / CPM) × 1000 × Noise(0.85–1.15)
--     -- Reach channels: Impressions = NewCustomers × AudienceMultiplier × Noise
--     -- ── Impressions (Materialized via CROSS APPLY) ─────────────────────────
--     [Imp].[Impressions],
--     -- ⚠ BEST PRACTICE — CROSS APPLY AS A RANDOM-DRAW MATERIALISATION LOCK:
--     -- If Impressions were computed inline in the SELECT, the SQL Server optimizer
--     -- might re-evaluate NEWID() multiple times for the same logical row, producing
--     -- different values each time Impressions is referenced (e.g., once for the
--     -- SELECT list and once in the Clicks CASE WHEN). The first CROSS APPLY forces
--     -- the engine to compute and LOCK the Impressions value once per row. The second
--     -- CROSS APPLY then reads [Imp].[Impressions] as a stable, already-resolved value.
--     -- This is the canonical T-SQL pattern for deterministic multi-stage random derivations.

--     -- ── Clicks (Derived strictly from materialized Impressions) ────────────
--     [Clk].[Clicks],
--     -- Reads from the second CROSS APPLY, which derives Clicks using the locked
--     -- [Imp].[Impressions] value. This ensures Clicks are always <= Impressions
--     -- (combined with the GREATEST() floor guard below).

--     [sc].[NewCustomers]                                              AS [NewCustomersAcquired]
--     -- Ground-truth count written as-is. This is NOT derived — it is the exact
--     -- number produced by Script 02 for this (month × channel) combination.

-- FROM [SpendCalculation] AS [sc]

-- -- ── First CROSS APPLY: Lock Impressions random draw ────────────────────────
-- -- 1. Materialize Impressions First to lock the random draw
-- CROSS APPLY (
-- -- ⚠ BEST PRACTICE — TWO-STAGE CROSS APPLY FOR DEPENDENT RANDOM DERIVATIONS:
-- -- Stage 1 (this APPLY) computes and materializes Impressions once per row.
-- -- Stage 2 (next APPLY) references [Imp].[Impressions] as a stable scalar —
-- -- Clicks are derived FROM impressions, not from a second independent impression draw.
-- -- This two-stage pattern guarantees: Clicks ≤ Impressions at all times.
--     SELECT
--         CASE [sc].[AcquisitionChannelKey]
--         -- Route each channel to its appropriate Impressions derivation method:

--             WHEN 1 THEN ABS([sc].[NewCustomers] * (80 + ABS(CHECKSUM(NEWID())) % 40))
--             -- Organic (1): reach-based. AudienceMultiplier = 80–119 readers per acquired customer.
--             -- ABS() outer wrap: ensures non-negative result if CHECKSUM produces INT_MIN edge.

--             WHEN 2 THEN CAST(ISNULL([sc].[CalcSpend], 0) / 40.0 * 1000.0 * (0.85 + (ABS(CHECKSUM(NEWID())) % 300) / 1000.0)  AS INT)
--             -- Paid Search (2): CPM-based. CPM=$40 → Impressions = Spend/40*1000.
--             -- ISNULL([sc].[CalcSpend], 0): defensive guard against NULL spend (Direct propagation).
--             -- Noise range: 0.85–1.15 (% 300 / 1000.0 → 0.000–0.299 + 0.85).
--             -- CAST AS INT: Impressions are always whole numbers.

--             WHEN 3 THEN CAST(ISNULL([sc].[CalcSpend], 0) / 12.0 * 1000.0 * (0.85 + (ABS(CHECKSUM(NEWID())) % 300) / 1000.0)  AS INT)
--             -- Social Media (3): CPM-based. CPM=$12 → higher impression volume per dollar than Paid Search.
--             -- Same noise range as Paid Search (0.85–1.15).

--             WHEN 4 THEN ABS([sc].[NewCustomers] * (35 + ABS(CHECKSUM(NEWID())) % 25))
--             -- Email Marketing (4): list-reach-based. AudienceMultiplier = 35–59 list members
--             -- per acquired customer (email list is a smaller, warmer audience than organic).

--             WHEN 5 THEN ABS([sc].[NewCustomers] * (3 + ABS(CHECKSUM(NEWID())) % 5))
--             -- Direct (5): minimal impressions — direct visitors see no ad, but the brand
--             -- web page and homepage count as "impressions" (3–7 per customer).
--             -- This produces small non-zero Impressions, not zero, which is intentional:
--             -- even Direct visitors interact with brand touchpoints.

--             WHEN 6 THEN ABS([sc].[NewCustomers] * (12 + ABS(CHECKSUM(NEWID())) % 13))
--             -- Referral (6): referral network reach. AudienceMultiplier = 12–24.
--             -- Smaller reach per customer than organic but warmer (referred traffic).

--             WHEN 7 THEN CAST(ISNULL([sc].[CalcSpend], 0) / 15.0 * 1000.0 * (0.85 + (ABS(CHECKSUM(NEWID())) % 300) / 1000.0)  AS INT)
--             -- Affiliate (7): CPM-based. CPM=$15 — between Social and Paid Search.
--             -- Same CPM formula pattern as channels 2 and 3.

--             ELSE 0
--             -- Defensive default: any future channel key not yet mapped returns 0 impressions.
--         END AS [Impressions]
-- ) AS [Imp]

-- -- ── Second CROSS APPLY: Derive Clicks from locked Impressions ──────────────
-- -- 2. Derive Clicks strictly from the locked Impressions value
-- CROSS APPLY (
-- -- Stage 2: uses [Imp].[Impressions] (already locked) as the base for click derivation.
-- -- Clicks = Impressions × CTR_rate × Noise(0.80–1.20).
-- -- ⚠ BEST PRACTICE — GREATEST() AS FUNNEL FLOOR GUARD:
-- -- A marketing funnel requires: Impressions ≥ Clicks ≥ NewCustomersAcquired.
-- -- GREATEST([sc].[NewCustomers], <click formula>) enforces the lower bound:
-- -- Clicks can never fall below the actual number of customers acquired.
-- -- This is a SQL Server 2022+ function (available in 2025). It eliminates
-- -- the need for a CASE WHEN comparison to enforce the business rule.
--     SELECT
--         GREATEST(
--             [sc].[NewCustomers],
--             -- Lower bound: clicks must be at least equal to actual acquisitions.
--             CASE [sc].[AcquisitionChannelKey]
--             -- Route each channel to its CTR rate:

--                 WHEN 1 THEN CAST([Imp].[Impressions] * 0.035 * (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)  AS INT)
--                 -- Organic (1): CTR = 3.5%. Reflects typical organic search click-through rate.
--                 -- Noise: standard ±20% pattern (% 400 / 1000.0 + 0.80).
--                 -- CAST AS INT: clicks are always whole numbers.

--                 WHEN 2 THEN CAST([Imp].[Impressions] * 0.035 * (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)  AS INT)
--                 -- Paid Search (2): CTR = 3.5%. Same rate as Organic — both are intent-driven channels
--                 -- with similar click behaviour (user is actively searching).

--                 WHEN 3 THEN CAST([Imp].[Impressions] * 0.009 * (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)  AS INT)
--                 -- Social Media (3): CTR = 0.9%. Much lower — social ads are interruption-based,
--                 -- not intent-based. High impression volume compensates for the low CTR.

--                 WHEN 4 THEN CAST([Imp].[Impressions] * 0.035 * (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)  AS INT)
--                 -- Email Marketing (4): CTR = 3.5%. Warm owned-list traffic has high engagement
--                 -- relative to paid channels — matches Organic/Paid Search rate.

--                 WHEN 5 THEN [sc].[NewCustomers]
--                 -- Direct (5): Clicks = NewCustomers exactly. Direct visitors navigated directly
--                 -- to the site — every visit counts as one click, and all of them converted.
--                 -- No noise applied: Direct has deterministic behaviour.

--                 WHEN 6 THEN CAST([Imp].[Impressions] * 0.050 * (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)  AS INT)
--                 -- Referral (6): CTR = 5.0%. Highest CTR — referred visitors are pre-qualified
--                 -- by a recommender (friend, review site, partner), producing warm high-intent clicks.

--                 WHEN 7 THEN CAST([Imp].[Impressions] * 0.015 * (0.80 + (ABS(CHECKSUM(NEWID())) % 400) / 1000.0)  AS INT)
--                 -- Affiliate (7): CTR = 1.5%. Between Social and Email — creator/affiliate
--                 -- audiences are engaged but less intent-driven than search.

--                 ELSE [sc].[NewCustomers]
--                 -- Defensive default: any unmapped channel uses NewCustomers as the
--                 -- click floor, ensuring the funnel constraint is never violated.
--             END
--         ) AS [Clicks]
-- ) AS [Clk];
-- -- Semicolon terminates the INSERT … SELECT … CTE statement.
-- GO

-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  MENTOR REVISION: OPTIMIZED DATA PIPELINE                                   ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║  Changes applied:                                                           ║
-- ║  1. Fixed Arithmetic Overflow bugs (ABS(CHECKSUM) boundary constraints).    ║
-- ║  2. Swapped MONEY for DECIMAL(19,4) for mathematical precision.             ║
-- ║  3. Extracted the 13M row FactOnlineSales scan into a #TempTable to        ║
-- ║     relieve memory grant pressure on the main CTE pipeline.                 ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── 1. PRE-CALCULATE SEASONALITY (PERFORMANCE FIX) ──────────────────────────
-- Instead of scanning 13 million rows inside the complex pipeline, we extract
-- the 36 distinct months into a tiny temp table.
DROP TABLE IF EXISTS #MonthlySeasonality;

-- ⚠ MENTOR FIX: Stop relying on SELECT INTO to guess the schema.
-- Explicitly define the Temp Table with the NOT NULL PRIMARY KEY constraint upfront.
-- This is the only 100% bulletproof way to guarantee the constraint in a single batch.
CREATE TABLE #MonthlySeasonality (
    [YearMonth]     INT             NOT NULL PRIMARY KEY,
    [MonthRevenue]  DECIMAL(19,4)   NULL,
    [SeasonIndex]   DECIMAL(19,4)   NULL
);

INSERT INTO #MonthlySeasonality ([YearMonth], [MonthRevenue], [SeasonIndex])
SELECT 
    [Agg].[YearMonth],
    [Agg].[MonthRevenue],
    [Agg].[MonthRevenue] / NULLIF(AVG([Agg].[MonthRevenue]) OVER (), 0.0) AS [SeasonIndex]
FROM (
    SELECT
        YEAR([DateKey]) * 100 + MONTH([DateKey]) AS [YearMonth],
        CAST(SUM([SalesAmount]) AS DECIMAL(19,4)) AS [MonthRevenue]
    FROM [dbo].[FactOnlineSales]
    GROUP BY YEAR([DateKey]) * 100 + MONTH([DateKey])
) AS [Agg];


-- ── 2. MAIN CTE PIPELINE ────────────────────────────────────────────────────
;WITH [MonthlyAcquisitions] AS (
    SELECT
        YEAR([ca].[AcquisitionDate]) * 100 + MONTH([ca].[AcquisitionDate]) AS [YearMonth],
        [ca].[AcquisitionChannelKey],
        COUNT(*) AS [NewCustomers]
    FROM  [gen].[CustomerAcquisition] AS [ca]
    GROUP BY
        YEAR([ca].[AcquisitionDate]) * 100 + MONTH([ca].[AcquisitionDate]),
        [ca].[AcquisitionChannelKey]
),

[SpendCalculation] AS (
    SELECT
        [ma].[YearMonth],
        [ma].[YearMonth] * 100 + 1 AS [MonthStartDateKey],
        [ma].[AcquisitionChannelKey],
        [ma].[NewCustomers],
        
        -- Default to 1.0 if month has no revenue record
        ISNULL([si].[SeasonIndex], 1.0) AS [SeasonFactor],

        -- CACMidpoint from DimAcquisitionChannel
        ([ch].[EstimatedCACLow] + [ch].[EstimatedCACHigh]) / 2.0 AS [CACMidpoint],

        -- Spend Formula (Using DECIMAL(19,4) instead of MONEY)
        -- SAFE RANDOMIZATION: ABS(CHECKSUM(NEWID()) % 400) - modulo inside ABS
        CAST(
            [ma].[NewCustomers]
            * (([ch].[EstimatedCACLow] + [ch].[EstimatedCACHigh]) / 2.0)
            * ISNULL([si].[SeasonIndex], 1.0)
            * (0.80 + ABS(CHECKSUM(NEWID()) % 400) / 1000.0)
        AS DECIMAL(19,4)) AS [CalcSpend]

    FROM       [MonthlyAcquisitions]          AS [ma]
    INNER JOIN [gen].[DimAcquisitionChannel]  AS [ch]
        ON [ma].[AcquisitionChannelKey] = [ch].[AcquisitionChannelKey]
    LEFT  JOIN #MonthlySeasonality            AS [si]
        ON [ma].[YearMonth] = [si].[YearMonth]
)

-- ── 3. INSERT & CROSS APPLY FUNNEL ──────────────────────────────────────────
INSERT INTO [gen].[FactMarketingSpend]
    ([YearMonth], [MonthStartDateKey], [AcquisitionChannelKey],
     [MonthlySpend], [Impressions], [Clicks], [NewCustomersAcquired])
SELECT
    [sc].[YearMonth],
    [sc].[MonthStartDateKey],
    [sc].[AcquisitionChannelKey],

    -- DECIMAL(19,4) alignment and floor guard
    CASE WHEN [sc].[CalcSpend] < 0 THEN CAST(0 AS DECIMAL(19,4)) 
         ELSE [sc].[CalcSpend] 
    END AS [MonthlySpend],

    [Imp].[Impressions],
    [Clk].[Clicks],
    [sc].[NewCustomers] AS [NewCustomersAcquired]

FROM [SpendCalculation] AS [sc]

-- STAGE 1: Lock Impressions random draw (Fixed Arithmetic Overflows)
CROSS APPLY (
    SELECT
        CASE [sc].[AcquisitionChannelKey]
            -- Modulo must be evaluated BEFORE ABS() to prevent INT_MIN overflow
            WHEN 1 THEN ABS([sc].[NewCustomers] * (80 + ABS(CHECKSUM(NEWID()) % 40)))
            WHEN 2 THEN CAST(ISNULL([sc].[CalcSpend], 0) / 40.0 * 1000.0 * (0.85 + ABS(CHECKSUM(NEWID()) % 300) / 1000.0) AS INT)
            WHEN 3 THEN CAST(ISNULL([sc].[CalcSpend], 0) / 12.0 * 1000.0 * (0.85 + ABS(CHECKSUM(NEWID()) % 300) / 1000.0) AS INT)
            WHEN 4 THEN ABS([sc].[NewCustomers] * (35 + ABS(CHECKSUM(NEWID()) % 25)))
            WHEN 5 THEN ABS([sc].[NewCustomers] * (3 + ABS(CHECKSUM(NEWID()) % 5)))
            WHEN 6 THEN ABS([sc].[NewCustomers] * (12 + ABS(CHECKSUM(NEWID()) % 13)))
            WHEN 7 THEN CAST(ISNULL([sc].[CalcSpend], 0) / 15.0 * 1000.0 * (0.85 + ABS(CHECKSUM(NEWID()) % 300) / 1000.0) AS INT)
            ELSE 0
        END AS [Impressions]
) AS [Imp]

-- STAGE 2: Derive Clicks from locked Impressions (Fixed Arithmetic Overflows)
CROSS APPLY (
    SELECT
        GREATEST(
            [sc].[NewCustomers],
            CASE [sc].[AcquisitionChannelKey]
                WHEN 1 THEN CAST([Imp].[Impressions] * 0.035 * (0.80 + ABS(CHECKSUM(NEWID()) % 400) / 1000.0) AS INT)
                WHEN 2 THEN CAST([Imp].[Impressions] * 0.035 * (0.80 + ABS(CHECKSUM(NEWID()) % 400) / 1000.0) AS INT)
                WHEN 3 THEN CAST([Imp].[Impressions] * 0.009 * (0.80 + ABS(CHECKSUM(NEWID()) % 400) / 1000.0) AS INT)
                WHEN 4 THEN CAST([Imp].[Impressions] * 0.035 * (0.80 + ABS(CHECKSUM(NEWID()) % 400) / 1000.0) AS INT)
                WHEN 5 THEN [sc].[NewCustomers]
                WHEN 6 THEN CAST([Imp].[Impressions] * 0.050 * (0.80 + ABS(CHECKSUM(NEWID()) % 400) / 1000.0) AS INT)
                WHEN 7 THEN CAST([Imp].[Impressions] * 0.015 * (0.80 + ABS(CHECKSUM(NEWID()) % 400) / 1000.0) AS INT)
                ELSE [sc].[NewCustomers]
            END
        ) AS [Clicks]
) AS [Clk];
GO

PRINT '  → [gen].[FactMarketingSpend] populated.';
-- Audit message confirming successful INSERT into the target table.
GO


-- ============================================================================
-- STEP 3: Performance index
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — STEP 3: PERFORMANCE INDEX                                   ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates a Non-Clustered Index on AcquisitionChannelKey to support the     ║
-- ║  most common access patterns against gen.FactMarketingSpend:               ║
-- ║  (1) GROUP BY AcquisitionChannelKey — channel-level spend reports          ║
-- ║  (2) JOIN to gen.DimAcquisitionChannel for channel attribute enrichment    ║
-- ║                                                                             ║
-- ║  WHY INCLUDE YearMonth AND MonthlySpend                                     ║
-- ║  Most spend queries filter by channel AND aggregate by month. Including    ║
-- ║  YearMonth and MonthlySpend in the index leaf pages eliminates a Key       ║
-- ║  Lookup back to the clustered index (IDENTITY PK) for these column         ║
-- ║  fetches — covering index pattern.                                         ║
-- ║                                                                             ║
-- ║  NOTE: With only ~252 rows this index has negligible performance impact    ║
-- ║  today. It is included because: (a) the view-layer queries may join this   ║
-- ║  table to FactOnlineSales (large), and (b) it demonstrates the correct    ║
-- ║  indexing pattern for student reference in the DEPI programme.             ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

CREATE NONCLUSTERED INDEX [IX_FactMarketingSpend_ChannelKey]
-- NONCLUSTERED: a secondary B-tree index — does not affect physical row storage order.
-- The clustered index (IDENTITY PK) governs physical ordering; this NCI adds a
-- separate lookup path for channel-based access patterns.
    ON [gen].[FactMarketingSpend] ([AcquisitionChannelKey])
    -- Index key: AcquisitionChannelKey — the most common JOIN and GROUP BY column
    -- in marketing analytics queries (e.g., spend by channel, CAC by channel).
    INCLUDE ([YearMonth], [MonthlySpend], [Impressions], [Clicks], [NewCustomersAcquired]);
    -- INCLUDE columns: appended to the index leaf pages without being key columns.
    -- Queries that filter by channel AND aggregate any of these metrics can be
    -- satisfied entirely from the NCI leaf pages — no Key Lookup to the clustered index.
    -- This is the "covering index" pattern for analytics queries on fact tables.
GO

PRINT '  → Index IX_FactMarketingSpend_ChannelKey created.';
GO


-- ============================================================================
-- RESET NOEXEC — ensures subsequent batches in the same session run normally
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — SET NOEXEC OFF RESET                                         ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  Resets the session execution state so that the verification queries       ║
-- ║  below always execute regardless of whether a pre-check triggered          ║
-- ║  SET NOEXEC ON earlier in this session.                                    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- Unconditionally re-enables execution for all subsequent batches.
-- Critical: without this reset, verification queries (V1–V5) would also be
-- skipped if a pre-check had fired SET NOEXEC ON.
-- Project-wide rule: SET NOEXEC OFF always appears after the last DDL/DML
-- block and before the verification suite.
GO


-- ============================================================================
-- VERIFICATION SUITE (V1 – V5)
-- Run all checks after STEP 2 completes. All "expect 0" rows must be 0.
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 6 — VERIFICATION SUITE (V1 – V5)                                ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  PURPOSE                                                                    ║
-- ║  Five verification queries confirm correctness at multiple levels:         ║
-- ║  V1: Row count and grain integrity (exact)                                 ║
-- ║  V2: Spend by channel — CAC hierarchy and Direct=$0 (directional)          ║
-- ║  V3: Monthly spend trend — must show seasonality alignment (directional)   ║
-- ║  V4: Digital funnel metrics — CTR hierarchy by channel (directional)       ║
-- ║  V5: Referential integrity — all must return 0 (exact)                    ║
-- ║                                                                             ║
-- ║  DETERMINISM NOTES                                                          ║
-- ║  V1 row counts and V5 integrity checks are EXACT.                          ║
-- ║  V2–V4 are APPROXIMATE because RandomNoise (CHECKSUM(NEWID())) varies      ║
-- ║  per run. Check direction of patterns and CAC hierarchy, not exact values. ║
-- ║                                                                             ║
-- ║  EXPECTED TOTAL ROWS                                                        ║
-- ║  7 channels × number of distinct calendar months in gen.CustomerAcquisition║
-- ║  Contoso DW spans January 2007 – December 2009 = 36 months.               ║
-- ║  Expected row count: 7 × 36 = 252 rows.                                   ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  gen.FactMarketingSpend — Verification Suite';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '';


-- ----------------------------------------------------------------------------
-- V1 — ROW COUNT & GRAIN INTEGRITY
-- Every (YearMonth × AcquisitionChannelKey) pair must appear exactly once.
-- Total rows = 7 channels × distinct months in gen.CustomerAcquisition.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — ROW COUNT & GRAIN INTEGRITY                                       │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — based on Contoso Retail DW):                  │
-- │  ┌──────────────────────────────────────────────────────────┬────────┐  │
-- │  │ Metric                                                   │ Value  │  │
-- │  ├──────────────────────────────────────────────────────────┼────────┤  │
-- │  │ TotalRows                                                │  252   │  │
-- │  │ DistinctChannels   (must = 7)                            │   7    │  │
-- │  │ DistinctMonths     (must = 36 for 2007–2009 full range)  │  36    │  │
-- │  │ DuplicateGrainRows (must = 0, enforced by UQ constraint) │   0    │  │
-- │  └──────────────────────────────────────────────────────────┴────────┘  │
-- │                                                                         │
-- │  ✗ TotalRows ≠ 252: some months may have 0 acquisitions for a channel  │
-- │    — check gen.CustomerAcquisition distribution first.                 │
-- │  ✗ DistinctChannels < 7: the INNER JOIN to DimAcquisitionChannel       │
-- │    is filtering out channels. Check Script 01 has 7 rows.              │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V1: Row count and grain integrity';

SELECT
    COUNT(*)                                                AS [TotalRows],
    -- Total rows: must equal 7 channels × distinct months in gen.CustomerAcquisition.

    COUNT(DISTINCT [AcquisitionChannelKey])                 AS [DistinctChannels],
    -- Must equal 7. Less than 7 means the CTE's INNER JOIN dropped a channel.

    COUNT(DISTINCT [YearMonth])                             AS [DistinctMonths],
    -- Must equal 36 for the full 2007–2009 Contoso DW range (Jan 2007 – Dec 2009).

    COUNT(*) - COUNT(DISTINCT CAST([YearMonth] AS BIGINT)
                   * 100 + [AcquisitionChannelKey])         AS [DuplicateGrainRows]
    -- ⚠ BEST PRACTICE — COMPOSITE GRAIN DUPLICATE DETECTION VIA BIGINT ARITHMETIC:
    -- Creates a single composite integer key: (YearMonth × 100) + ChannelKey.
    -- Example: YearMonth=200703, ChannelKey=2 → 20070300 + 2 = 20070302.
    -- CAST AS BIGINT first: prevents INT overflow when YearMonth (6 digits)
    -- is multiplied by 100 (result = 8 digits, within BIGINT range).
    -- COUNT(*) - COUNT(DISTINCT composite): zero means no duplicate (month, channel) pairs.
    -- This check is redundant with the UNIQUE constraint but explicitly surfaces
    -- grain violations in the verification output for student clarity.
FROM [gen].[FactMarketingSpend];


-- ----------------------------------------------------------------------------
-- V2 — SPEND BY CHANNEL (CAC Hierarchy)
-- Direct must show $0.00 spend. Paid Search should have the highest
-- total spend. CostPerAcquisition should broadly follow the CAC midpoints
-- established in gen.DimAcquisitionChannel.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — SPEND BY CHANNEL (CAC Hierarchy and Direct = $0)                 │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate — varies per run due to RandomNoise):     │
-- │  ┌──────────────────┬────────┬─────────────┬────────────┬─────────┐    │
-- │  │ Channel          │ Key    │ TotalSpend  │ AvgCAC(pa) │ MktType │    │
-- │  ├──────────────────┼────────┼─────────────┼────────────┼─────────┤    │
-- │  │ Paid Search      │   2    │ Highest     │ ~$30–35    │ Paid    │    │
-- │  │ Social Media     │   3    │ 2nd or 3rd  │ ~$22–28    │ Paid    │    │
-- │  │ Referral         │   6    │ Mid         │ ~$15–20    │ Organic │    │
-- │  │ Affiliate        │   7    │ Mid-Low     │ ~$11–15    │ Paid    │    │
-- │  │ Email Marketing  │   4    │ Low         │ ~$7–10     │ Organic │    │
-- │  │ Organic Search   │   1    │ Very Low    │ ~$2–3      │ Organic │    │
-- │  │ Direct           │   5    │ $0.00       │ NULL       │ Direct  │    │
-- │  └──────────────────┴────────┴─────────────┴────────────┴─────────┘    │
-- │  ✗ Direct TotalSpend ≠ $0.00: CACMidpoint or CalcSpend has a defect.  │
-- │  ✗ Paid Search is not the highest spender: check CAC midpoint logic.   │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V2: Spend by channel — CAC hierarchy (Paid Search highest, Direct = $0)';

SELECT
    [ch].[ChannelName],
    -- Dimension attribute from gen.DimAcquisitionChannel — human-readable channel name.

    [ch].[ChannelCategory],
    -- Paid / Organic / Direct — useful for spend mix analysis (V4 pattern).

    COUNT([ms].[MarketingSpendID])                          AS [MonthCount],
    -- Should be 36 for all channels (one row per month across the full date range).

    CAST(SUM([ms].[MonthlySpend])       AS DECIMAL(12,2))   AS [TotalSpend],
    -- Total spend across all months for the channel — cast for readable display.

    CAST(SUM([ms].[Impressions])        AS BIGINT)           AS [TotalImpressions],
    -- Aggregated impression volume — BIGINT to handle large CPM-channel totals.

    CAST(SUM([ms].[Clicks])             AS BIGINT)           AS [TotalClicks],
    -- Aggregated click volume — BIGINT for same reason as TotalImpressions.

    SUM([ms].[NewCustomersAcquired])                        AS [TotalCustomers],
    -- Total customers acquired via this channel across all months.

    CAST(
        CASE WHEN SUM([ms].[NewCustomersAcquired]) > 0
             THEN SUM([ms].[MonthlySpend]) / SUM([ms].[NewCustomersAcquired])
             -- Re-aggregated CAC: total spend / total customers (correct for period CAC).
             -- Not AVG(CostPerAcquisition) — which would be a mean of means (incorrect).
             ELSE NULL
        END AS DECIMAL(10,2))                               AS [AvgCostPerAcquisition],
    -- NULL for Direct (SUM(NewCustomers) > 0 but SUM(Spend) = 0 → result = 0, not NULL).
    -- The CASE guards against the edge case where all customers have been removed.

    CAST(
        CASE WHEN SUM([ms].[Clicks]) > 0
             THEN SUM([ms].[MonthlySpend]) / SUM([ms].[Clicks])
             ELSE NULL
        END AS DECIMAL(10,2))                               AS [AvgCostPerClick]
    -- Re-aggregated CPC: total spend / total clicks (same "mean of sums" principle).
FROM       [gen].[FactMarketingSpend]       AS [ms]
INNER JOIN [gen].[DimAcquisitionChannel]    AS [ch]
    ON [ms].[AcquisitionChannelKey] = [ch].[AcquisitionChannelKey]
-- JOIN to DimAcquisitionChannel: retrieves ChannelName and ChannelCategory for display.
GROUP BY   [ch].[ChannelName], [ch].[ChannelCategory]
ORDER BY   SUM([ms].[MonthlySpend]) DESC;
-- ORDER BY total spend descending: highest-cost channel first, Direct last ($0).


-- ----------------------------------------------------------------------------
-- V3 — MONTHLY SPEND TREND (Seasonality Alignment)
-- Total monthly spend should correlate with actual monthly revenue.
-- Q4 months (October–December) should show above-average spend.
-- Q1 months (January–March) should show below-average spend.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — MONTHLY SPEND TREND (Seasonality vs Revenue Alignment)           │
-- │                                                                         │
-- │  HOW TO INTERPRET                                                       │
-- │  Join this output to the V3 revenue query (below) by YearMonth.        │
-- │  SpendIndex and RevenueIndex should move in the same direction:         │
-- │  both high in Q4, both low in Q1–Q2.                                   │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERN:                                          │
-- │  ┌───────────────┬──────────────────────────────────────────────────┐   │
-- │  │ Period        │ Expected Total Spend Relative to Full Period Avg  │   │
-- │  ├───────────────┼──────────────────────────────────────────────────┤   │
-- │  │ Q4 months     │ Above average (SeasonIndex > 1.0)                │   │
-- │  │ Q1–Q2 months  │ Below average (SeasonIndex < 1.0)                │   │
-- │  │ Q3 months     │ Near average (back-to-school small lift)         │   │
-- │  └───────────────┴──────────────────────────────────────────────────┘   │
-- │  ✗ Flat spend across all months: SeasonalityIndex CTE has a defect.   │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V3: Monthly spend trend (Q4 should show above-average spend)';

SELECT
    [ms].[YearMonth],
    -- YYYYMM raw source period — use as the x-axis in trend analysis.

    CAST(SUM([ms].[MonthlySpend])     AS DECIMAL(12,2))     AS [TotalSpend],
    -- Total spend across all 7 channels for the month — the aggregated trend line.

    SUM([ms].[NewCustomersAcquired])                        AS [NewCustomers],
    -- Month-level acquisition total — compare against TotalSpend for CAC trend.

    SUM([ms].[Impressions])                                 AS [TotalImpressions],
    -- Month-level impression total — should follow TotalSpend for CPM channels.

    SUM([ms].[Clicks])                                      AS [TotalClicks],
    -- Month-level click total — useful for overall CTR trend analysis.

    -- SpendIndex: 1.0 = average monthly spend across the full period
    CAST(
        SUM([ms].[MonthlySpend])
        / NULLIF(AVG(SUM([ms].[MonthlySpend])) OVER (), 0)
    -- AVG(SUM(...)) OVER (): window aggregate over the grouped result set.
    -- Outer AVG: average of the per-month SUM totals (grand average month).
    -- NULLIF guard: defensive protection against zero average (impossible but safe).
    -- Result: values >1.0 indicate above-average spend months (expected in Q4).
    AS DECIMAL(5,2))                                        AS [SpendIndex]
FROM  [gen].[FactMarketingSpend]  AS [ms]
GROUP BY [ms].[YearMonth]
ORDER BY [ms].[YearMonth];
-- Ascending month order: displays the time-series trend chronologically.


-- ----------------------------------------------------------------------------
-- V4 — PAID vs ORGANIC SPEND SPLIT BY YEAR
-- Confirms the spend mix is aligned with channel category.
-- Paid channels (Paid Search, Social, Affiliate) should account for
-- the majority of spend (~75–85%). Organic should be a smaller share.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — PAID vs ORGANIC vs DIRECT SPEND BY YEAR                          │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERN (approximate):                            │
-- │  ┌───────────────────┬──────────────┬────────────────────────────────┐  │
-- │  │ ChannelCategory   │ Expected %   │ Notes                          │  │
-- │  ├───────────────────┼──────────────┼────────────────────────────────┤  │
-- │  │ Paid              │ ~75 – 85 %   │ Paid Search + Social dominant  │  │
-- │  │ Organic           │ ~15 – 25 %   │ Email + Referral moderate      │  │
-- │  │ Direct            │ ~0 %         │ Always $0.00 spend             │  │
-- │  └───────────────────┴──────────────┴────────────────────────────────┘  │
-- │                                                                         │
-- │  Year-over-year: Social Media % should be stable or growing slightly   │
-- │  as its customer acquisition volume holds strong in 2023–2025.         │
-- │  ✗ Organic > 40%: CAC calibration has a defect.                        │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V4: Paid vs Organic spend split by year';

SELECT
    [ms].[YearMonth] / 100                                  AS [SourceYear],
    -- Integer division by 100 extracts the year component from YYYYMM.
    -- Example: 200703 / 100 = 2007 (integer division truncates the month).

    [ch].[ChannelCategory],
    -- Paid / Organic / Direct — the category axis for the spend mix analysis.

    CAST(SUM([ms].[MonthlySpend]) AS DECIMAL(12,2))         AS [TotalSpend],
    -- Category-year level spend total — the numerator for PctOfYearSpend.

    CAST(
        SUM([ms].[MonthlySpend]) * 100.0
        / NULLIF(SUM(SUM([ms].[MonthlySpend])) OVER (
            PARTITION BY [ms].[YearMonth] / 100
        ), 0)
    -- ⚠ BEST PRACTICE — NESTED WINDOW AGGREGATE FOR PERCENTAGE-OF-TOTAL:
    -- SUM(SUM([ms].[MonthlySpend])) OVER (PARTITION BY year):
    --   Inner SUM: aggregates spend per GROUP BY (year, category) row.
    --   Outer SUM OVER (PARTITION BY year): sums all category rows within the year.
    -- This gives the year-total denominator for the percentage calculation.
    -- NULLIF(..., 0): guards against zero year-total spend (impossible, but defensive).
    -- * 100.0: produces a percentage value (0.00 – 100.00).
    AS DECIMAL(5,2))                                        AS [PctOfYearSpend]
    -- Percentage of total year spend attributed to this category.
    -- Three rows per year (Paid, Organic, Direct) that sum to 100.00%.
FROM       [gen].[FactMarketingSpend]       AS [ms]
INNER JOIN [gen].[DimAcquisitionChannel]    AS [ch]
    ON [ms].[AcquisitionChannelKey] = [ch].[AcquisitionChannelKey]
GROUP BY   [ms].[YearMonth] / 100, [ch].[ChannelCategory]
-- Two-column GROUP BY: (year, category) — one row per year-category combination.
ORDER BY   [SourceYear], [TotalSpend] DESC;
-- Year ascending, then highest spend category first within each year.


-- ----------------------------------------------------------------------------
-- V5 — REFERENTIAL INTEGRITY & DATA QUALITY
-- All six checks must return 0. Any non-zero value indicates a defect.
-- ----------------------------------------------------------------------------

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V5 — REFERENTIAL INTEGRITY & DATA QUALITY (all 6 checks must be 0)   │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — all zeros):                                   │
-- │  ┌────────────────────────────────────────────────────────┬──────────┐  │
-- │  │ Check                                                  │ Expected │  │
-- │  ├────────────────────────────────────────────────────────┼──────────┤  │
-- │  │ Orphan AcquisitionChannelKey                           │    0     │  │
-- │  │ Duplicate (YearMonth, ChannelKey) pairs                │    0     │  │
-- │  │ NULL MonthlySpend                                      │    0     │  │
-- │  │ Negative MonthlySpend                                  │    0     │  │
-- │  │ Impressions < Clicks (impossible — CTR can't exceed 1) │    0     │  │
-- │  │ Clicks < NewCustomersAcquired (impossible — funnel)    │    0     │  │
-- │  └────────────────────────────────────────────────────────┴──────────┘  │
-- │                                                                         │
-- │  NOTE: Direct channel (Key=5) rows are expected to have:               │
-- │  MonthlySpend=$0, Impressions≥0, Clicks=NewCustomersAcquired.          │
-- │  These are correct values, not data defects.                           │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '';
PRINT '  V5: Referential integrity and data quality (all expect 0)';

SELECT
    'Orphan AcquisitionChannelKey'          AS [Check],
    -- Tests whether any FactMarketingSpend row references a channel key that
    -- does not exist in gen.DimAcquisitionChannel. Should be 0 (FK constraint
    -- prevents orphans at INSERT time, but this confirms it explicitly).
    COUNT(*)                                AS [Value]
FROM [gen].[FactMarketingSpend] AS [ms]
WHERE NOT EXISTS (
    -- NOT EXISTS: returns TRUE for each [ms] row that has NO matching channel
    -- in the dimension table. Equivalent to a LEFT JOIN / IS NULL anti-join pattern
    -- but generally more readable and optimised by the SQL Server query planner.
    SELECT 1 FROM [gen].[DimAcquisitionChannel] AS [ch]
    WHERE [ch].[AcquisitionChannelKey] = [ms].[AcquisitionChannelKey]
)

UNION ALL
-- UNION ALL: combines multiple integrity check results into a single result set
-- without the overhead of DISTINCT de-duplication (each check has a unique label).

SELECT
    'Duplicate (YearMonth, ChannelKey) pairs',
    -- Confirms the UNIQUE constraint is working correctly. Should always be 0.
    COUNT(*) - COUNT(DISTINCT CAST([YearMonth] AS BIGINT) * 100 + [AcquisitionChannelKey])
    -- Same BIGINT composite key technique used in V1 — consistent pattern.
FROM [gen].[FactMarketingSpend]

UNION ALL

SELECT 'NULL MonthlySpend',
    -- MonthlySpend is NOT NULL in the DDL, but this check catches defects
    -- if the table is ever altered to allow NULLs in a future revision.
    SUM(CASE WHEN [MonthlySpend] IS NULL THEN 1 ELSE 0 END)
FROM [gen].[FactMarketingSpend]

UNION ALL

SELECT 'Negative MonthlySpend',
    -- The CASE WHEN < 0 floor guard in the INSERT should prevent this.
    -- A non-zero result means the floor guard has a defect.
    SUM(CASE WHEN [MonthlySpend] < 0 THEN 1 ELSE 0 END)
FROM [gen].[FactMarketingSpend]

UNION ALL

SELECT 'Impressions < Clicks (funnel violation)',
    -- ⚠ BEST PRACTICE — FUNNEL SEQUENCE INTEGRITY CHECK:
    -- A marketing funnel requires Impressions ≥ Clicks (you cannot click an ad
    -- you did not see). Any violation here means the two-stage CROSS APPLY
    -- randomisation produced an impossible sequence. The GREATEST() guard
    -- ensures Clicks ≥ NewCustomers, but does not directly bound Clicks ≤ Impressions.
    -- This check validates the CTR rates kept Clicks well below Impressions.
    SUM(CASE WHEN [Impressions] < [Clicks] THEN 1 ELSE 0 END)
FROM [gen].[FactMarketingSpend]

UNION ALL

SELECT 'Clicks < NewCustomersAcquired (funnel violation)',
    -- Validates the GREATEST([sc].[NewCustomers], ...) floor guard in the INSERT.
    -- Clicks must be ≥ NewCustomersAcquired: every acquired customer was a click.
    -- A non-zero result means GREATEST() was not applied or resolved incorrectly.
    SUM(CASE WHEN [Clicks] < [NewCustomersAcquired] THEN 1 ELSE 0 END)
FROM [gen].[FactMarketingSpend];
GO


PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 05 completed successfully.';
PRINT '  Table created:  [gen].[FactMarketingSpend]';
PRINT '  Index created:  [IX_FactMarketingSpend_ChannelKey]';
PRINT '';
PRINT '  Next steps:';
PRINT '    Script 06 → gen.FactCustomerSurvey    (depends on Script 01 only)';
PRINT '    Script 07 → gen.OnlineReturnEvents    (depends on Script 01 only)';
PRINT '    Script 08 → gen.PhysicalReturnEvents  (depends on Script 01 only)';
PRINT '════════════════════════════════════════════════════════════════';
GO
