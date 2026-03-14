/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║             SCRIPT 02: gen.CustomerAcquisition — CHANNEL ASSIGNMENT          ║
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
  This script generates gen.CustomerAcquisition — a one-row-per-customer table
  that assigns each customer in dbo.DimCustomer to exactly one acquisition
  channel from gen.DimAcquisitionChannel.

  The Contoso source has no concept of how a customer was first acquired.
  Without this table, the entire CMO acquisition analytics layer is dark:
  channel attribution, CAC benchmarking, paid/organic mix, and channel-to-CLV
  analysis are all impossible. This single table unlocks that entire domain.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Business Questions Unlocked                                            │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  CMO:  What is our customer acquisition cost by channel?                │
  │  CMO:  What is our paid vs. organic customer mix?                       │
  │  CMO:  Which channel acquires the highest lifetime-value customers?     │
  │  CMO:  How has our acquisition channel mix shifted over time?           │
  │  CFO:  What is the CAC-to-CLV ratio per channel?                        │
  │  CEO:  What is the payback period for customer acquisition spend?       │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  WHAT THIS SCRIPT DOES
--------------------------------------------------------------------------------
  It reads every qualifying customer from dbo.DimCustomer
  (this means customer type = 'person', birthdate is null, and firstdatepurchase is null
  , scores all seven
  acquisition channels for each customer using a multi-factor weighted model,
  and inserts the winning channel (highest score) as a single row into
  gen.CustomerAcquisition.

  The scoring model is intentionally designed to produce DISCOVERABLE PATTERNS
  — not perfectly uniform distributions. Students should be able to surface
  in their Power BI analysis that:
    - Younger customers were disproportionately acquired via Social Media
    - High-income customers skew toward Direct and Referral channels
    - European customers show stronger Organic/Email acquisition patterns
    - Asian customers show stronger Social Media acquisition
    - The channel mix shifts meaningfully across the 2023–2025 period

  These patterns are realistic signals embedded in the data, not noise.

--------------------------------------------------------------------------------
  GRAIN AND SCOPE
--------------------------------------------------------------------------------
  Grain    : One row per Person customer (CustomerType = 'Person')
  Scope    : All customers in dbo.DimCustomer where:
               - CustomerType = 'Person'        (company accounts excluded)
               - DateFirstPurchase IS NOT NULL   (acquisition date required)
               - BirthDate IS NOT NULL           (age factor requires DOB)

  ⚠  EXCLUSION NOTE — Company Accounts:
  Customers with CustomerType != 'Person' are intentionally excluded.
  Corporate accounts do not have a consumer acquisition channel —
  they are B2B relationships, not marketing-channel acquisitions.
  These customers will have no row in gen.CustomerAcquisition, and
  therefore no row in fact.vCustomerAcquisition. In DAX, this means
  they will return BLANK() on any acquisition channel measure.
  This is correct behaviour, not a data quality issue.

--------------------------------------------------------------------------------
  ACQUISITION DATE — NO TEMPORAL SHIFT APPLIED HERE
--------------------------------------------------------------------------------
  AcquisitionDate is stored as the raw DateFirstPurchase value from
  dbo.DimCustomer — the 2007–2009 era source date. No +16 year offset
  is applied at the [gen] layer.

  The +16 year temporal shift is applied EXCLUSIVELY at the [fact] view layer
  in fact.vCustomerAcquisition, consistent with the project's architectural
  principle that all temporal transformations happen at the semantic layer,
  never at the physical data layer.

  This ensures:
  1. The gen table stores the ground truth of the source
  2. A single transformation point (the view) — no double-shifting risk
  3. AcquisitionDate in fact.vCustomerAcquisition will appear as 2023–2025

--------------------------------------------------------------------------------
  SCORING MODEL — DESIGN RATIONALE
--------------------------------------------------------------------------------
  The assignment uses a competitive scoring system: each customer receives a
  score for every channel, and the channel with the highest score wins.
  Scores are computed as:

    FinalScore = BaseWeight × YearFactor × AgeFactor
                           × IncomeFactor × GeoFactor × RandomNoise

  This structure ensures:
  - BASE WEIGHT sets the prior probability of each channel being relevant
  - YEAR FACTOR encodes temporal evolution of the channel mix across 2023–2025
  - AGE FACTOR reflects generational differences in channel preference
  - INCOME FACTOR reflects socioeconomic patterns in discovery behaviour
  - GEO FACTOR reflects regional platform and channel preferences
  - RANDOM NOISE prevents deterministic ties and adds realistic variance

  Each factor is described in detail in the ChannelScores CTE below.

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Base Weights — Calibrated to 2023–2025 Era                            │
  ├──────────────────────┬────────────────────────────────────────────────┤
  │  Channel             │  Weight  Rationale                             │
  ├──────────────────────┼────────────────────────────────────────────────┤
  │  Paid Search (2)     │   22.0   Dominant intent channel — Google Ads  │
  │  Social Media (3)    │   20.0   Mature platform — Instagram/TikTok    │
  │  Organic Search (1)  │   18.0   Important but under pressure from AI  │
  │  Direct (5)          │   14.0   Brand-aware returning segment         │
  │  Email Marketing (4) │   12.0   Owned-list channel, strong ROI        │
  │  Referral (6)        │    9.0   Word-of-mouth / influencer            │
  │  Affiliate (7)       │    5.0   Smaller share in creator economy era  │
  └──────────────────────┴────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run on      : ContosoRetailDW (fresh instance)
  Run order   : Script 02 — Run after Script 01
  Dependencies: [gen] schema, gen.DimAcquisitionChannel, dbo.DimCustomer,
                dbo.DimGeography
  Impact      : Creates ONE new table in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — DROP IF EXISTS guard on the table.
  Can parallel : YES — Scripts 03 and 04 have no dependency on this script.
                 Script 05 (gen.FactMarketingSpend) MUST wait for this script.

================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECKS (3 checks)                  ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Runs three sequential dependency checks before any DDL executes:          ║
-- ║  (1) [gen] schema exists     → requires Script 00                          ║
-- ║  (2) gen.DimAcquisitionChannel exists → requires Script 01                 ║
-- ║  (3) dbo.DimCustomer exists  → requires ContosoRetailDW to be loaded       ║
-- ║                                                                             ║
-- ║  Each check is in its own GO-terminated batch. If check (1) fires          ║
-- ║  SET NOEXEC ON, checks (2) and (3) are also skipped — they are parsed      ║
-- ║  but not executed, so no second error fires.                               ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  Each check has an ELSE branch that prints ✓. Reading the Messages tab     ║
-- ║  should show exactly 3 green ticks before any DDL output appears. If you  ║
-- ║  see fewer, identify which check fired and resolve it before continuing.   ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT ON SUCCESS:                                               ║
-- ║  ✓ [gen] schema confirmed.                                                 ║
-- ║  ✓ [gen].[DimAcquisitionChannel] confirmed.                               ║
-- ║  ✓ [dbo].[DimCustomer] confirmed.                                          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- PRE-CHECKS: Confirm all dependencies exist before proceeding
-- ============================================================================
-- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gen')
IF SCHEMA_ID('gen') IS NULL
BEGIN
    
    DECLARE @ErrorMsg NVARCHAR(2048) = FORMATMESSAGE('ERROR: [gen] schema does not exist. Run Script 00 first.');
    
    -- 2. Raise a descriptive error message
    THROW 50001, @ErrorMsg, 1;
    
    -- TERMINATE ALL SUBSEQUENT BATCHES. Do not use RETURN.
    

END
ELSE
BEGIN
    PRINT '✓ Schema [gen] confirmed.';
    PRINT '';
END
GO

IF OBJECT_ID('[gen].[DimAcquisitionChannel]', 'U') IS NULL
BEGIN
    
    -- RAISERROR('ERROR: [gen].[DimAcquisitionChannel] does not exist. Run Script 01 first.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('ERROR: [gen].[DimAcquisitionChannel] does not exist. Run Script 01 first.');
    
    THROW 50001, @ErrorMessage, 1;

    
END
ELSE
BEGIN
    PRINT '✓ [gen].[DimAcquisitionChannel] confirmed.';
END
GO

IF OBJECT_ID('[dbo].[DimCustomer]', 'U') IS NULL
BEGIN
    
    -- RAISERROR('ERROR: [dbo].[DimCustomer] not found. Confirm ContosoRetailDW is loaded.', 16, 1);
    DECLARE @ErrorMessage NVARCHAR(2048) = FORMATMESSAGE('ERROR: [dbo].[DimCustomer] not found. Confirm ContosoRetailDW is loaded.');
    
    THROW 50001, @ErrorMessage, 1;

    
END
ELSE
BEGIN
    PRINT '✓ [dbo].[DimCustomer] confirmed.';
END
GO

PRINT '';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — STEP 1: IDEMPOTENT TABLE DROP                               ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Drops gen.CustomerAcquisition if it already exists, then allows the next  ║
-- ║  block to recreate it cleanly. If this is a first run, the IF block is     ║
-- ║  skipped silently.                                                          ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  DROP + CREATE (not ALTER TABLE) is the correct pattern for generation      ║
-- ║  scripts because the entire table content is regenerated on each run.      ║
-- ║  ALTER TABLE would accumulate stale rows from previous runs.               ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 1: Drop existing table (if re-running)
-- ============================================================================
DROP TABLE IF EXISTS [gen].[CustomerAcquisition];
PRINT '✓ Existing gen.CustomerAcquisition table dropped (if existed).';

GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — STEP 2: TARGET TABLE DEFINITION                             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates gen.CustomerAcquisition — a lean 3-column bridge table that       ║
-- ║  links each customer to their acquisition channel and date.                ║
-- ║                                                                             ║
-- ║  TABLE DESIGN DECISIONS                                                     ║
-- ║  • 3 columns only (CustomerKey, AcquisitionChannelKey, AcquisitionDate).   ║
-- ║    All channel attributes (ChannelName, CAC, IsOrganic) are resolved via   ║
-- ║    the FK join in dim.vAcquisitionChannel. Storing them here would         ║
-- ║    denormalise the gen layer unnecessarily.                                ║
-- ║  • AcquisitionDate is stored as DATE (not INT DateKey). The view layer     ║
-- ║    (fact.vCustomerAcquisition) computes the YYYYMMDD INT DateKey from this ║
-- ║    DATE column using the +16 year temporal shift. If we stored INT here    ║
-- ║    we would need to double-shift — a dangerous maintenance risk.           ║
-- ║  • Two FK constraints: one to dbo.DimCustomer, one to                      ║
-- ║    gen.DimAcquisitionChannel. This enforces referential integrity at the   ║
-- ║    database level — orphan records cannot be inserted.                     ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  CustomerKey is the PRIMARY KEY — exactly one row per customer. This is    ║
-- ║  the grain of this table. The scoring algorithm (Step 3) must guarantee    ║
-- ║  this uniqueness via ROW_NUMBER() PARTITION BY CustomerKey.                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 2: Create the target table
-- ============================================================================
-- Three columns only — this is a lean bridge table.
-- All channel attributes (ChannelName, CAC, IsOrganic) are resolved via the
-- FK relationship to gen.DimAcquisitionChannel in the view/Power BI layer.
-- Storing them here would denormalise the [gen] layer unnecessarily.
--
-- AcquisitionDate stored as DATE (not INT DateKey). The view layer
-- (fact.vCustomerAcquisition) computes the YYYYMMDD INT DateKey from this
-- DATE column using the +16 year temporal shift. If we stored INT here,
-- we would need to double-shift — a dangerous maintenance risk.
-- ============================================================================

CREATE TABLE [gen].[CustomerAcquisition] (
    CustomerKey             INT    NOT NULL,
    AcquisitionChannelKey   INT    NOT NULL,
    AcquisitionDate         DATE   NOT NULL,

    CONSTRAINT [PK_CustomerAcquisition]
        PRIMARY KEY (CustomerKey),

    CONSTRAINT [FK_CustAcq_Customer]
        FOREIGN KEY (CustomerKey)
        REFERENCES [dbo].[DimCustomer] (CustomerKey),

    CONSTRAINT [FK_CustAcq_Channel]
        FOREIGN KEY (AcquisitionChannelKey)
        REFERENCES [gen].[DimAcquisitionChannel] (AcquisitionChannelKey)
);
GO

PRINT '✓ [gen].[CustomerAcquisition] table created.';
PRINT '';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — STEP 3: COMPOSITE SCORING ALGORITHM                         ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Assigns exactly one acquisition channel to every qualifying customer      ║
-- ║  using a 3-CTE competitive scoring pipeline.                               ║
-- ║                                                                             ║
-- ║  ┌─────────────────────────────────────────────────────────────────────┐   ║
-- ║  │  ALGORITHM — 3-STAGE CTE PIPELINE                                   │   ║
-- ║  ├─────────────────────────────────────────────────────────────────────┤   ║
-- ║  │  CTE 1: CustomerProfile                                             │   ║
-- ║  │    Reads dbo.DimCustomer + dbo.DimGeography.                        │   ║
-- ║  │    Computes 4 scoring inputs per customer:                          │   ║
-- ║  │      AgeAtAcquisition  — birthday-corrected age (avoids off-by-one) │   ║
-- ║  │      YearlyIncome      — raw source value for income band mapping   │   ║
-- ║  │      Continent         — ISNULL-guarded geography join              │   ║
-- ║  │      YearProgress      — 0.0→1.0 position in dataset year range    │   ║
-- ║  │    Applies exclusion filters: Person only, non-NULL dates.          │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 2: ChannelScores                                               │   ║
-- ║  │    CROSS JOIN CustomerProfile × DimAcquisitionChannel (7 channels). │   ║
-- ║  │    Produces N_customers × 7 rows, each with a FinalScore:           │   ║
-- ║  │    FinalScore = BaseWeight × YearFactor × AgeFactor                 │   ║
-- ║  │                           × IncomeFactor × GeoFactor × RandomNoise │   ║
-- ║  │                                                                     │   ║
-- ║  │  CTE 3: RankedChannels                                              │   ║
-- ║  │    ROW_NUMBER() PARTITION BY CustomerKey ORDER BY FinalScore DESC.  │   ║
-- ║  │    Assigns Rank=1 to the winning channel per customer.              │   ║
-- ║  │    Only Rank=1 rows are inserted — guarantees one row per customer. │   ║
-- ║  └─────────────────────────────────────────────────────────────────────┘   ║
-- ║                                                                             ║
-- ║  PRE-CALCULATION PERFORMANCE PATTERN                                        ║
-- ║  @MinYear / @MaxYear / @YearRange are declared and computed BEFORE the      ║
-- ║  CTE begins. This avoids a Table Spool on dbo.DimCustomer if MIN/MAX were  ║
-- ║  computed inline inside the CTE body. Critical on large customer tables.   ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  1. CROSS JOIN is intentional here — we WANT all 7 channels evaluated for  ║
-- ║     every customer so scores are genuinely competitive.                    ║
-- ║  2. CHECKSUM(NEWID()) produces a different result on each execution.        ║
-- ║     Individual channel assignments will vary slightly between runs.        ║
-- ║     Population-level patterns (age/income/geo signals) remain consistent.  ║
-- ║  3. The birthday-correction CASE in AgeAtAcquisition is standard SQL        ║
-- ║     practice. DATEDIFF(YEAR,...) alone over-counts by 1 for people whose   ║
-- ║     birthday falls later in the year than the reference date.              ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 3: Generate channel assignments
-- ============================================================================
-- This is a single INSERT ... SELECT using three chained CTEs:
--
--   CTE 1 — CustomerProfile:
--     Reads dbo.DimCustomer + dbo.DimGeography and computes the four
--     customer attributes needed for scoring: age at acquisition,
--     income level, continent, and year progress (temporal position
--     within the dataset's date range). Applies exclusion filters here
--     so all downstream CTEs work on clean data only.
--
--   CTE 2 — ChannelScores:
--     CROSS JOINs CustomerProfile against gen.DimAcquisitionChannel to
--     produce one row per customer-channel combination (N_customers × 7).
--     For each combination, computes the composite FinalScore.
--     This is where all scoring logic lives.
--
--   CTE 3 — RankedChannels:
--     Applies ROW_NUMBER() OVER (PARTITION BY CustomerKey ORDER BY FinalScore DESC)
--     to rank channels per customer. The final INSERT selects WHERE Rank = 1
--     — the winning channel for each customer.
--
-- NOTE ON RANDOMNESS:
--   CHECKSUM(NEWID()) generates a fresh non-deterministic integer per row
--   on each execution. This means results will differ slightly between runs,
--   which is correct — we want realistic variance, not a fixed assignment.
--   The behavioural patterns (age/income/geo signals) will be consistent
--   across runs even though individual assignments may vary at the margin.
-- ============================================================================
DECLARE @MinYear INT;
DECLARE @MaxYear INT;
DECLARE @YearRange FLOAT;

SELECT 
    @MinYear = MIN(YEAR(DateFirstPurchase)),
    @MaxYear = MAX(YEAR(DateFirstPurchase))
FROM [dbo].[DimCustomer]
WHERE CustomerType = 'Person' 
  AND DateFirstPurchase IS NOT NULL;

-- Protect against division by zero
SET @YearRange = NULLIF(@MaxYear - @MinYear, 0);

;WITH CustomerProfile AS (

    SELECT
        c.CustomerKey,
        c.DateFirstPurchase                         AS AcquisitionDate,
        YEAR(c.DateFirstPurchase)                   AS AcquisitionYear,

        -- ── Age at time of first purchase (birthday-adjusted) ────────────────
        -- Standard project formula: avoids off-by-one error where someone whose
        -- birthday hasn't occurred yet in the current year gets overcounted.
        DATEDIFF(YEAR, c.BirthDate, c.DateFirstPurchase)
            - CASE
                WHEN DATEADD(
                    YEAR,
                    DATEDIFF(YEAR, c.BirthDate, c.DateFirstPurchase),
                    c.BirthDate
                ) > c.DateFirstPurchase
                THEN 1
                ELSE 0
              END                                   AS AgeAtAcquisition,

        c.YearlyIncome,
        ISNULL(g.ContinentName, 'Unknown')          AS Continent,

        -- ── YearProgress: temporal position within the dataset range ─────────
        -- 0.0 = earliest DateFirstPurchase year in the dataset
        -- 1.0 = latest  DateFirstPurchase year in the dataset
        -- Computed dynamically so the script adapts to any date range
        -- without hardcoded year values. NULLIF protects against a single-year
        -- dataset (denominator = 0 would produce division error).
       CAST(YEAR(c.DateFirstPurchase) - @MinYear AS FLOAT) 
        / @YearRange AS YearProgress

    FROM [dbo].[DimCustomer]   AS c
    LEFT JOIN [dbo].[DimGeography] AS g
        ON c.GeographyKey = g.GeographyKey

    WHERE c.CustomerType        = 'Person'      -- Exclude B2B / company accounts
      AND c.DateFirstPurchase   IS NOT NULL      -- AcquisitionDate required
      AND c.BirthDate           IS NOT NULL      -- AgeFactor requires DOB

),

ChannelScores AS (

    SELECT
        cp.CustomerKey,
        cp.AcquisitionDate,
        ch.AcquisitionChannelKey,

        -- ==============================================================
        -- COMPOSITE SCORE COMPUTATION
        -- FinalScore = BaseWeight × YearFactor × AgeFactor
        --                         × IncomeFactor × GeoFactor × RandomNoise
        -- ==============================================================

        CAST(

            -- ── FACTOR 1: Base Weight ────────────────────────────────────────
            -- Prior probability of each channel — calibrated to 2023–2025
            -- digital marketing landscape. Social Media and Paid Search share
            -- the top positions as fully matured channels. Organic Search is
            -- still strong but under increasing pressure from AI search.
            -- Affiliate is smallest — influencer economics favour owned reach.
            CASE ch.AcquisitionChannelKey
                WHEN 1 THEN 18.0   -- Organic Search:  important but AI-pressured
                WHEN 2 THEN 22.0   -- Paid Search:      dominant intent channel
                WHEN 3 THEN 20.0   -- Social Media:     mature, major channel
                WHEN 4 THEN 12.0   -- Email Marketing:  owned list, high ROI
                WHEN 5 THEN 14.0   -- Direct:           brand-aware segment
                WHEN 6 THEN  9.0   -- Referral:         word-of-mouth / influencer
                WHEN 7 THEN  10.0   -- Affiliate:        smaller creator economy share
            END

            -- ── FACTOR 2: Year Factor ────────────────────────────────────────
            -- Encodes how each channel's relevance evolves from 2023 to 2025.
            -- YearProgress: 0.0 = 2023 era, 1.0 = 2025 era.
            --
            -- Key 2023→2025 trends:
            --   Social Media: slight decline as ad costs rise + platform fatigue
            --   Paid Search:  slight growth — AI-enhanced targeting improves ROAS
            --   Organic:      declining — AI Overviews reduce organic click-through
            --   Affiliate:    growing — creator economy and influencer partnerships
            --   Email:        stable — owned list remains resilient
            --   Direct:       stable — brand loyalty channel
            --   Referral:     slight growth — trust economy growing
            * CASE ch.AcquisitionChannelKey
                WHEN 1 THEN 1.0 - (0.15 * cp.YearProgress)  -- Organic: 1.00 → 0.85
                WHEN 2 THEN 1.0 + (0.12 * cp.YearProgress)  -- Paid Srch: 1.00 → 1.12
                WHEN 3 THEN 1.0 - (0.10 * cp.YearProgress)  -- Social: 1.00 → 0.90
                WHEN 4 THEN 1.0 + (0.00 * cp.YearProgress)  -- Email: 1.00 (stable)
                WHEN 5 THEN 1.0 - (0.05 * cp.YearProgress)  -- Direct: 1.00 → 0.95
                WHEN 6 THEN 1.0 + (0.08 * cp.YearProgress)  -- Referral: 1.00 → 1.08
                WHEN 7 THEN 1.0 + (0.20 * cp.YearProgress)  -- Affiliate: 1.00 → 1.20
            END

            -- ── FACTOR 3: Age Factor ─────────────────────────────────────────
            -- Generational platform preferences. Younger customers discover via
            -- social and short-form video; older customers rely on search and
            -- brand familiarity (direct / email).
            --
            -- Age bands:
            --   Under 30:   TikTok/Instagram generation — Social dominant
            --   30–44:      Dual-platform — Paid Search + Social balanced
            --   45–59:      Search-first, email-responder demographic
            --   60+:        Brand-loyal direct navigators; low social adoption
            * CASE ch.AcquisitionChannelKey
                WHEN 1 THEN  -- Organic Search
                    CASE WHEN cp.AgeAtAcquisition <  30 THEN 0.8
                         WHEN cp.AgeAtAcquisition <  45 THEN 1.0
                         WHEN cp.AgeAtAcquisition <  60 THEN 1.2
                         ELSE 1.3 END
                WHEN 2 THEN  -- Paid Search
                    CASE WHEN cp.AgeAtAcquisition <  30 THEN 0.9
                         WHEN cp.AgeAtAcquisition <  45 THEN 1.2
                         WHEN cp.AgeAtAcquisition <  60 THEN 1.1
                         ELSE 0.9 END
                WHEN 3 THEN  -- Social Media
                    CASE WHEN cp.AgeAtAcquisition <  30 THEN 1.6
                         WHEN cp.AgeAtAcquisition <  45 THEN 1.2
                         WHEN cp.AgeAtAcquisition <  60 THEN 0.7
                         ELSE 0.4 END
                WHEN 4 THEN  -- Email Marketing
                    CASE WHEN cp.AgeAtAcquisition <  30 THEN 0.7
                         WHEN cp.AgeAtAcquisition <  45 THEN 1.0
                         WHEN cp.AgeAtAcquisition <  60 THEN 1.3
                         ELSE 1.4 END
                WHEN 5 THEN  -- Direct
                    CASE WHEN cp.AgeAtAcquisition <  30 THEN 0.6
                         WHEN cp.AgeAtAcquisition <  45 THEN 1.0
                         WHEN cp.AgeAtAcquisition <  60 THEN 1.2
                         ELSE 1.5 END
                WHEN 6 THEN  -- Referral
                    CASE WHEN cp.AgeAtAcquisition <  30 THEN 1.1
                         WHEN cp.AgeAtAcquisition <  45 THEN 1.1
                         WHEN cp.AgeAtAcquisition <  60 THEN 1.0
                         ELSE 0.9 END
                WHEN 7 THEN  -- Affiliate
                    CASE WHEN cp.AgeAtAcquisition <  30 THEN 1.8
                         WHEN cp.AgeAtAcquisition <  45 THEN 1.1
                         WHEN cp.AgeAtAcquisition <  60 THEN 0.6
                         ELSE 0.4 END
            END

            -- ── FACTOR 4: Income Factor ──────────────────────────────────────
            -- Socioeconomic patterns in digital discovery behaviour.
            -- Income thresholds match the IncomeGroup bands in dim.vCustomer:
            --   High          : $120,000+      → Direct, Referral (brand loyal)
            --   Upper-Middle  : $70,000–119,999 → Paid Search (conversion-ready)
            --   Lower-Middle  : $40,000–69,999  → Email, Organic (considered buyers)
            --   Low           : < $40,000       → Social, Affiliate (deal-seekers)
            --
            -- Note: YearlyIncome is the raw source value from dbo.DimCustomer.
            -- These thresholds match those used in dim.vCustomer's IncomeGroup.
            * CASE ch.AcquisitionChannelKey
                WHEN 1 THEN  -- Organic Search
                    CASE WHEN cp.YearlyIncome >= 120000 THEN 0.9
                         WHEN cp.YearlyIncome >=  70000 THEN 1.0
                         WHEN cp.YearlyIncome >=  40000 THEN 1.1
                         ELSE 1.1 END
                WHEN 2 THEN  -- Paid Search
                    CASE WHEN cp.YearlyIncome >= 120000 THEN 1.0
                         WHEN cp.YearlyIncome >=  70000 THEN 1.3
                         WHEN cp.YearlyIncome >=  40000 THEN 1.1
                         ELSE 0.8 END
                WHEN 3 THEN  -- Social Media
                    CASE WHEN cp.YearlyIncome >= 120000 THEN 0.7
                         WHEN cp.YearlyIncome >=  70000 THEN 0.9
                         WHEN cp.YearlyIncome >=  40000 THEN 1.2
                         ELSE 1.5 END
                WHEN 4 THEN  -- Email Marketing
                    CASE WHEN cp.YearlyIncome >= 120000 THEN 0.9
                         WHEN cp.YearlyIncome >=  70000 THEN 1.0
                         WHEN cp.YearlyIncome >=  40000 THEN 1.2
                         ELSE 1.1 END
                WHEN 5 THEN  -- Direct
                    CASE WHEN cp.YearlyIncome >= 120000 THEN 1.5
                         WHEN cp.YearlyIncome >=  70000 THEN 1.1
                         WHEN cp.YearlyIncome >=  40000 THEN 0.9
                         ELSE 0.7 END
                WHEN 6 THEN  -- Referral
                    CASE WHEN cp.YearlyIncome >= 120000 THEN 1.4
                         WHEN cp.YearlyIncome >=  70000 THEN 1.1
                         WHEN cp.YearlyIncome >=  40000 THEN 0.9
                         ELSE 0.8 END
                WHEN 7 THEN  -- Affiliate
                    CASE WHEN cp.YearlyIncome >= 120000 THEN 0.5
                         WHEN cp.YearlyIncome >=  70000 THEN 0.7
                         WHEN cp.YearlyIncome >=  40000 THEN 1.0
                         ELSE 1.9 END
            END

            -- ── FACTOR 5: Geographic Factor ──────────────────────────────────
            -- Regional platform and channel preferences in the 2023–2025 era.
            --
            --   North America : Google Ads dominant, strong Social (Meta/TikTok)
            --   Europe        : GDPR-conscious — owned channels preferred.
            --                   Strong Organic (brand trust) and Email (consent-based).
            --                   Paid Search present but constrained by privacy law.
            --   Asia          : Social Media dominant (WeChat, TikTok, Instagram).
            --                   Affiliate strong — influencer commerce ecosystem.
            --   Other/Unknown : Direct and Referral slightly elevated.
            * CASE ch.AcquisitionChannelKey
                WHEN 1 THEN  -- Organic Search
                    CASE cp.Continent
                        WHEN 'North America' THEN 1.0
                        WHEN 'Europe'        THEN 1.4
                        WHEN 'Asia'          THEN 0.7
                        ELSE 1.0 END
                WHEN 2 THEN  -- Paid Search
                    CASE cp.Continent
                        WHEN 'North America' THEN 1.4
                        WHEN 'Europe'        THEN 1.0
                        WHEN 'Asia'          THEN 0.8
                        ELSE 0.9 END
                WHEN 3 THEN  -- Social Media
                    CASE cp.Continent
                        WHEN 'North America' THEN 1.2
                        WHEN 'Europe'        THEN 0.9
                        WHEN 'Asia'          THEN 1.6
                        ELSE 1.1 END
                WHEN 4 THEN  -- Email Marketing
                    CASE cp.Continent
                        WHEN 'North America' THEN 1.0
                        WHEN 'Europe'        THEN 1.3
                        WHEN 'Asia'          THEN 0.8
                        ELSE 1.0 END
                WHEN 5 THEN  -- Direct
                    CASE cp.Continent
                        WHEN 'North America' THEN 1.1
                        WHEN 'Europe'        THEN 1.0
                        WHEN 'Asia'          THEN 0.9
                        ELSE 1.2 END
                WHEN 6 THEN  -- Referral
                    CASE cp.Continent
                        WHEN 'North America' THEN 1.0
                        WHEN 'Europe'        THEN 1.0
                        WHEN 'Asia'          THEN 1.0
                        ELSE 1.2 END
                WHEN 7 THEN  -- Affiliate
                    CASE cp.Continent
                        WHEN 'North America' THEN 0.8
                        WHEN 'Europe'        THEN 0.6
                        WHEN 'Asia'          THEN 1.9
                        ELSE 1.1 END
            END

            -- ── FACTOR 6: Random Noise ───────────────────────────────────────
            -- Adds realistic variance so not every customer of the same
            -- age/income/geo band gets the same channel.
            -- Range: 0.5 to 1.5 (±50% of the score at any given row).
            -- Uses CHECKSUM(NEWID()) per row for true non-determinism.
            -- The channel with the highest raw product wins, so the noise
            -- can tip borderline cases — which is exactly what we want.
            * (0.5 + (ABS(CHECKSUM(NEWID())) % 1000) / 1000.0)

        AS FLOAT) AS FinalScore

    FROM CustomerProfile          AS cp
    CROSS JOIN [gen].[DimAcquisitionChannel] AS ch
    -- CROSS JOIN produces 7 rows per customer (one per channel).
    -- All 7 rows get scored; only the top-scored row is inserted.

),

RankedChannels AS (

    SELECT
        CustomerKey,
        AcquisitionChannelKey,
        AcquisitionDate,
        FinalScore,
        ROW_NUMBER() OVER (
            PARTITION BY CustomerKey
            ORDER BY FinalScore DESC
        ) AS Rank
        -- Partition by customer → rank channels per customer independently.
        -- Ties are broken by SQL Server's internal order (effectively random).
        -- In practice ties are extremely rare given the noise factor.

    FROM ChannelScores

)

-- ── Final insert: winning channel only (Rank = 1 per customer) ─────────────
INSERT INTO [gen].[CustomerAcquisition]
    (CustomerKey, AcquisitionChannelKey, AcquisitionDate)
SELECT
    CustomerKey,
    AcquisitionChannelKey,
    AcquisitionDate
FROM RankedChannels
WHERE Rank = 1;

PRINT '✓ [gen].[CustomerAcquisition] populated.';
GO


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — STEP 4: PERFORMANCE INDEX                                   ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates a Non-Clustered Index on AcquisitionChannelKey to support the     ║
-- ║  most common join pattern: fact.vCustomerAcquisition joining to            ║
-- ║  dim.vAcquisitionChannel on AcquisitionChannelKey.                         ║
-- ║                                                                             ║
-- ║  WHY INCLUDE (AcquisitionDate)                                              ║
-- ║  fact.vCustomerAcquisition always SELECTs AcquisitionDate alongside the    ║
-- ║  key. Without the INCLUDE clause SQL Server would need a Key Lookup back   ║
-- ║  to the clustered index (CustomerKey PK) to fetch AcquisitionDate — a      ║
-- ║  separate IO operation per matched row. The INCLUDE makes this a covering  ║
-- ║  index: all required columns are in the index leaf pages, eliminating      ║
-- ║  the Key Lookup entirely.                                                  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- STEP 4: Performance index
-- ============================================================================
-- Non-clustered index on AcquisitionChannelKey supports the common join
-- pattern: fact.vCustomerAcquisition → dim.vAcquisitionChannel.
-- INCLUDE on AcquisitionDate avoids a key lookup for the date column,
-- which fact.vCustomerAcquisition always selects.
-- ============================================================================

CREATE NONCLUSTERED INDEX [IX_CustomerAcquisition_ChannelKey]
    ON [gen].[CustomerAcquisition] (AcquisitionChannelKey)
    INCLUDE (AcquisitionDate);

PRINT '✓ Index [IX_CustomerAcquisition_ChannelKey] created.';
PRINT '';
GO


-- ============================================================================
-- VERIFICATION QUERIES — V1 through V7
-- ============================================================================
-- Run immediately after execution. Expected results are documented inline.
-- ============================================================================

PRINT '════════════════════════════════════════════════════════════════';
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 6 — VERIFICATION SUITE (V1 – V7)                                ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  PURPOSE                                                                    ║
-- ║  Seven verification queries confirm correctness at multiple levels:        ║
-- ║  population completeness, channel distribution, temporal channel shift,    ║
-- ║  geographic patterns, age-based patterns, income-based patterns, and       ║
-- ║  referential integrity.                                                    ║
-- ║                                                                             ║
-- ║  IMPORTANT: V1 counts are EXACT (deterministic source data).               ║
-- ║  V2–V6 distributions are APPROXIMATE (randomness in scoring).              ║
-- ║  Check direction of patterns, not exact percentages.                       ║
-- ║                                                                             ║
-- ║  CONTOSO BASELINE (for reference):                                          ║
-- ║  dbo.DimCustomer contains approximately 18,484 total customers.           ║
-- ║  Of these, ~18,000 qualify as Person customers with non-NULL dates.        ║
-- ║  The remaining ~484 are company/corporate accounts (excluded).             ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
PRINT '  VERIFICATION — Script 02';
PRINT '════════════════════════════════════════════════════════════════';


-- ── V1: Row count and population completeness ────────────────────────────────
-- Confirm one row per qualifying Person customer.
-- Cross-check against dbo.DimCustomer to see how many were excluded
-- (company accounts, NULL dates). Both figures should be non-zero.
-- No NULLs should exist in any column.
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — ROW COUNT & COMPLETENESS                                          │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate — based on Contoso source):               │
-- │  ┌────────────────────────────────────────────────────────────────────┐ │
-- │  │ TotalAssigned : 18,484  (qualifying Person customers)             │ │
-- │  │ NullChannels  : 0        (every row must have a channel)           │ │
-- │  │ NullDates     : 0        (every row must have an acquisition date) │ │
-- │  │ EarliestAcqn  : 2001-07-01 (raw source 2007 era — no +16 shift yet) │ │
-- │  │ LatestAcqn    : 2004-07-31 (raw source range)                       │ │
-- │  └────────────────────────────────────────────────────────────────────┘ │
-- │  ✗ If NullChannels or NullDates > 0: scoring CTE has a defect.         │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V1: Row count and completeness';

SELECT
    COUNT(*)                                                    AS TotalAssigned,
    SUM(CASE WHEN AcquisitionChannelKey IS NULL THEN 1 ELSE 0 END) AS NullChannels,
    SUM(CASE WHEN AcquisitionDate       IS NULL THEN 1 ELSE 0 END) AS NullDates,
    MIN(AcquisitionDate)                                        AS EarliestAcquisition,
    MAX(AcquisitionDate)                                        AS LatestAcquisition
FROM [gen].[CustomerAcquisition];


-- ── V2: Population vs. source reconciliation ────────────────────────────────
-- Shows how many customers were assigned vs. excluded (company/NULL rows).
-- The sum of Assigned + Excluded should equal total dbo.DimCustomer rows.
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — POPULATION RECONCILIATION vs dbo.DimCustomer                     │
-- │                                                                         │
-- │  EXPECTED OUTPUT (approximate):                                         │
-- │  ┌──────────────────────────┬──────────────────────────────────────┐   │
-- │  │ QualifyingCustomers      │ 18,484 (Person + non-NULL dates)    │   │
-- │  │ ExcludedCustomers        │ 385    (company accounts or NULLs)  │   │
-- │  │ TotalInSource            │ 18,869 (all rows in DimCustomer)     │   │
-- │  └──────────────────────────┴──────────────────────────────────────┘   │
-- │  QualifyingCustomers must equal gen.CustomerAcquisition row count.     │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V2: Population reconciliation vs. dbo.DimCustomer';

SELECT
    SUM(CASE WHEN c.CustomerType = 'Person'
             AND c.DateFirstPurchase IS NOT NULL
             AND c.BirthDate IS NOT NULL THEN 1 ELSE 0 END)    AS QualifyingCustomers,
    SUM(CASE WHEN c.CustomerType != 'Person'
             OR  c.DateFirstPurchase IS NULL
             OR  c.BirthDate IS NULL THEN 1 ELSE 0 END)        AS ExcludedCustomers,
    COUNT(*)                                                    AS TotalInSource
FROM [dbo].[DimCustomer] AS c;


-- ── V3: Channel distribution — overall ──────────────────────────────────────
-- Shows the count and % share for each acquisition channel.
-- Expect a non-uniform distribution reflecting the weight model:
--   Paid Search and Social Media should be the top two channels.
--   Affiliate should be the smallest.
-- Any perfectly uniform distribution (each ~14.3%) would indicate a
-- scoring logic bug — all factors are cancelling each other out.
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — CHANNEL DISTRIBUTION OVERALL                                      │
-- │                                                                         │
-- │  EXPECTED PATTERN (approximate — varies slightly per run due to noise): │
-- │  ┌──────────────────────┬─────────────────────────────────────────┐    │
-- │  │ Channel              │ Expected % Range                        │    │
-- │  ├──────────────────────┼─────────────────────────────────────────┤    │
-- │  │ Paid Search          │ ~18 – 24 %  (highest or 2nd highest)   │    │
-- │  │ Social Media         │ ~16 – 22 %  (1st or 2nd)               │    │
-- │  │ Organic Search       │ ~14 – 20 %                              │    │
-- │  │ Direct               │ ~10 – 16 %                              │    │
-- │  │ Email Marketing      │ ~8  – 14 %                              │    │
-- │  │ Referral             │ ~6  – 12 %                              │    │
-- │  │ Affiliate            │ ~3  –  8 %  (always smallest)          │    │
-- │  └──────────────────────┴─────────────────────────────────────────┘    │
-- │  ✗ If all channels show ~14.3%: scoring factors are cancelling out.    │
-- │  ✗ If Affiliate > 15%: check that BaseWeight=5 is correct in CTE.     │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V3: Channel distribution — overall (non-uniform distribution expected)';

SELECT
    ch.AcquisitionChannelKey,
    ch.ChannelName,
    ch.ChannelCategory,
    COUNT(ca.CustomerKey)                                       AS CustomerCount,
    CAST(COUNT(ca.CustomerKey) * 100.0
         / SUM(COUNT(ca.CustomerKey)) OVER ()  AS DECIMAL(5, 2)) AS PctOfTotal,
    CAST(ch.EstimatedCACLow  AS DECIMAL(10, 2))                AS CACLow,
    CAST(ch.EstimatedCACHigh AS DECIMAL(10, 2))                AS CACHigh
FROM [gen].[CustomerAcquisition]       AS ca
JOIN [gen].[DimAcquisitionChannel]     AS ch
    ON ca.AcquisitionChannelKey = ch.AcquisitionChannelKey
GROUP BY
    ch.AcquisitionChannelKey,
    ch.ChannelName,
    ch.ChannelCategory,
    ch.EstimatedCACLow,
    ch.EstimatedCACHigh
ORDER BY CustomerCount DESC;


-- ── V4: Temporal channel mix shift ──────────────────────────────────────────
-- Shows the top 3 channels per year to confirm the YearFactor
-- is producing visible channel evolution across 2023–2025.
-- Affiliate should gain share over time; Organic should decline.
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — CHANNEL MIX BY YEAR (YearFactor Effect)                           │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERNS across source years 2007→2009:           │
-- │  • Affiliate %    should INCREASE year-over-year (YearFactor +20%)      │
-- │  • Organic Search % should DECREASE year-over-year (YearFactor -15%)   │
-- │  • Social Media % should DECREASE slightly (YearFactor -10%)           │
-- │  • Paid Search %  should INCREASE slightly  (YearFactor +12%)          │
-- │                                                                         │
-- │  The differences will be subtle (~1-3 pp per year) given RandomNoise.  │
-- │  Look for direction of change, not magnitude. Flat distributions        │
-- │  across years indicate the YearFactor CASE block has a defect.         │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V4: Channel mix by acquisition year (YearFactor effect)';

SELECT
    YEAR(ca.AcquisitionDate)    AS AcquisitionYear,
    ch.ChannelName,
    COUNT(*)                    AS CustomerCount,
    CAST(COUNT(*) * 100.0
         / SUM(COUNT(*)) OVER (
             PARTITION BY YEAR(ca.AcquisitionDate)
           ) AS DECIMAL(5, 2))  AS PctWithinYear
FROM [gen].[CustomerAcquisition]   AS ca
JOIN [gen].[DimAcquisitionChannel] AS ch
    ON ca.AcquisitionChannelKey = ch.AcquisitionChannelKey
GROUP BY YEAR(ca.AcquisitionDate), ch.ChannelName
ORDER BY AcquisitionYear, CustomerCount DESC;


-- ── V5: Age-based channel patterns ──────────────────────────────────────────
-- Confirms AgeFactor is producing expected generational signals.
-- Social Media should dominate the Under 30 segment.
-- Direct + Email should be strongest in 60+ segment.
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V5 — CHANNEL DISTRIBUTION BY AGE GROUP (AgeFactor Effect)              │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERNS (approximate):                           │
-- │  ┌──────────────┬──────────────────────────────────────────────────┐   │
-- │  │ Age Group    │ Expected Top Channels                            │   │
-- │  ├──────────────┼──────────────────────────────────────────────────┤   │
-- │  │ Under 30     │ Social Media should be #1 or #2 (AgeFactor=1.6) │   │
-- │  │ 30–44        │ Balanced — Paid Search and Social competitive    │   │
-- │  │ 45–59        │ Organic Search and Email stronger than average   │   │
-- │  │ 60+          │ Direct and Email should lead; Social lowest      │   │
-- │  └──────────────┴──────────────────────────────────────────────────┘   │
-- │  ✗ If Social Media % is uniform across age groups: AgeFactor CTE      │
-- │    has a defect.                                                        │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V5: Channel distribution by age group (AgeFactor effect)';

PRINT '  V5: Channel distribution by age group (AgeFactor effect)';

SELECT 
    Age.AgeGroup,
    ch.ChannelName,
    COUNT(*) AS CustomerCount,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY Age.AgeGroup) AS DECIMAL(5,2)) AS PctWithinAgeGroup
FROM [gen].[CustomerAcquisition] ca
JOIN [dbo].[DimCustomer] c ON ca.CustomerKey = c.CustomerKey
JOIN [gen].[DimAcquisitionChannel] ch ON ca.AcquisitionChannelKey = ch.AcquisitionChannelKey
CROSS APPLY (
    -- Compute Age once
    SELECT DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate) 
           - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate) > ca.AcquisitionDate THEN 1 ELSE 0 END AS ExactAge
) calc
CROSS APPLY (
    -- Compute Group once based on ExactAge
    SELECT CASE 
        WHEN calc.ExactAge < 30 THEN 'Under 30'
        WHEN calc.ExactAge < 45 THEN '30–44'
        WHEN calc.ExactAge < 60 THEN '45–59'
        ELSE '60+' 
    END AS AgeGroup
) Age
GROUP BY Age.AgeGroup, ch.ChannelName
ORDER BY Age.AgeGroup, CustomerCount DESC;

-- SELECT
--     CASE
--         WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--              - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                          > ca.AcquisitionDate THEN 1 ELSE 0 END < 30
--             THEN 'Under 30'
--         WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--              - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                          > ca.AcquisitionDate THEN 1 ELSE 0 END < 45
--             THEN '30–44'
--         WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--              - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                          > ca.AcquisitionDate THEN 1 ELSE 0 END < 60
--             THEN '45–59'
--         ELSE '60+'
--     END                         AS AgeGroup,
--     ch.ChannelName,
--     COUNT(*)                    AS CustomerCount,
--     CAST(COUNT(*) * 100.0
--          / SUM(COUNT(*)) OVER (
--              PARTITION BY
--                  CASE
--                      WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--                           - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                                       > ca.AcquisitionDate THEN 1 ELSE 0 END < 30
--                          THEN 'Under 30'
--                      WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--                           - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                                       > ca.AcquisitionDate THEN 1 ELSE 0 END < 45
--                          THEN '30–44'
--                      WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--                           - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                                       > ca.AcquisitionDate THEN 1 ELSE 0 END < 60
--                          THEN '45–59'
--                      ELSE '60+'
--                  END
--          ) AS DECIMAL(5, 2))    AS PctWithinAgeGroup
-- FROM [gen].[CustomerAcquisition]    AS ca
-- JOIN [dbo].[DimCustomer]            AS c
--     ON ca.CustomerKey = c.CustomerKey
-- JOIN [gen].[DimAcquisitionChannel]  AS ch
--     ON ca.AcquisitionChannelKey = ch.AcquisitionChannelKey
-- GROUP BY
--     CASE
--         WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--              - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                          > ca.AcquisitionDate THEN 1 ELSE 0 END < 30 THEN 'Under 30'
--         WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--              - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                          > ca.AcquisitionDate THEN 1 ELSE 0 END < 45 THEN '30–44'
--         WHEN DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate)
--              - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.BirthDate, ca.AcquisitionDate), c.BirthDate)
--                          > ca.AcquisitionDate THEN 1 ELSE 0 END < 60 THEN '45–59'
--         ELSE '60+'
--     END,
--     ch.ChannelName
-- ORDER BY AgeGroup, CustomerCount DESC;


-- ── V6: Income-based channel patterns ───────────────────────────────────────
-- Confirms IncomeFactor signals. High-income customers should skew to
-- Direct and Referral. Low-income customers should skew to Social/Affiliate.
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V6 — CHANNEL DISTRIBUTION BY INCOME GROUP (IncomeFactor Effect)        │
-- │                                                                         │
-- │  EXPECTED DIRECTIONAL PATTERNS (approximate):                           │
-- │  ┌───────────────────────────┬──────────────────────────────────────┐   │
-- │  │ Income Group              │ Expected Top Channels                │   │
-- │  ├───────────────────────────┼──────────────────────────────────────┤   │
-- │  │ High ($120K+)             │ Direct and Referral should over-index│   │
-- │  │ Upper-Middle ($70K–119K)  │ Paid Search should be strongest      │   │
-- │  │ Lower-Middle ($40K–69K)   │ Organic and Email competitive        │   │
-- │  │ Low (< $40K)              │ Social Media and Affiliate elevated  │   │
-- │  └───────────────────────────┴──────────────────────────────────────┘   │
-- │  ✗ If income groups show identical distributions: IncomeFactor CTE    │
-- │    has a defect.                                                        │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V6: Channel distribution by income group (IncomeFactor effect)';

SELECT 
    Income.IncomeGroup,
    ch.ChannelName,
    COUNT(*) AS CustomerCount,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY Income.IncomeGroup) AS DECIMAL(5,2)) AS PctWithinIncomeGroup
FROM [gen].[CustomerAcquisition] ca
JOIN [dbo].[DimCustomer] c ON ca.CustomerKey = c.CustomerKey
JOIN [gen].[DimAcquisitionChannel] ch ON ca.AcquisitionChannelKey = ch.AcquisitionChannelKey
CROSS APPLY (
    -- Compute Group once based on YearlyIncome
    SELECT CASE 
        WHEN c.YearlyIncome >= 120000 THEN 'High ($120K+)'
        WHEN c.YearlyIncome >=  70000 THEN 'Upper-Middle ($70K–119K)'
        WHEN c.YearlyIncome >=  40000 THEN 'Lower-Middle ($40K–69K)'
        ELSE 'Low (< $40K)' 
    END AS IncomeGroup
) Income
GROUP BY Income.IncomeGroup, ch.ChannelName
ORDER BY Income.IncomeGroup, CustomerCount DESC;

-- SELECT
--     CASE
--         WHEN c.YearlyIncome >= 120000 THEN 'High ($120K+)'
--         WHEN c.YearlyIncome >=  70000 THEN 'Upper-Middle ($70K–119K)'
--         WHEN c.YearlyIncome >=  40000 THEN 'Lower-Middle ($40K–69K)'
--         ELSE                               'Low (< $40K)'
--     END                         AS IncomeGroup,
--     ch.ChannelName,
--     COUNT(*)                    AS CustomerCount,
--     CAST(COUNT(*) * 100.0
--          / SUM(COUNT(*)) OVER (
--              PARTITION BY
--                  CASE
--                      WHEN c.YearlyIncome >= 120000 THEN 'High ($120K+)'
--                      WHEN c.YearlyIncome >=  70000 THEN 'Upper-Middle ($70K–119K)'
--                      WHEN c.YearlyIncome >=  40000 THEN 'Lower-Middle ($40K–69K)'
--                      ELSE                               'Low (< $40K)'
--                  END
--          ) AS DECIMAL(5, 2))    AS PctWithinIncomeGroup
-- FROM [gen].[CustomerAcquisition]    AS ca
-- JOIN [dbo].[DimCustomer]            AS c
--     ON ca.CustomerKey = c.CustomerKey
-- JOIN [gen].[DimAcquisitionChannel]  AS ch
--     ON ca.AcquisitionChannelKey = ch.AcquisitionChannelKey
-- GROUP BY
--     CASE
--         WHEN c.YearlyIncome >= 120000 THEN 'High ($120K+)'
--         WHEN c.YearlyIncome >=  70000 THEN 'Upper-Middle ($70K–119K)'
--         WHEN c.YearlyIncome >=  40000 THEN 'Lower-Middle ($40K–69K)'
--         ELSE                               'Low (< $40K)'
--     END,
--     ch.ChannelName
-- ORDER BY IncomeGroup, CustomerCount DESC;


-- ── V7: Referential integrity checks ────────────────────────────────────────
-- All CustomerKeys and ChannelKeys must resolve to valid source rows.
-- Both queries should return 0 orphans.
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V7 — REFERENTIAL INTEGRITY (all three checks must return 0)            │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — all zeros):                                   │
-- │  ┌────────────────────────────────────────────────────────┬───────┐    │
-- │  │ CheckName                                              │ Count │    │
-- │  ├────────────────────────────────────────────────────────┼───────┤    │
-- │  │ Orphan CustomerKeys                                    │   0   │    │
-- │  │ Orphan AcquisitionChannelKeys                          │   0   │    │
-- │  │ Duplicate CustomerKeys (must be 0 — PK guarantee)      │   0   │    │
-- │  └────────────────────────────────────────────────────────┴───────┘    │
-- │                                                                         │
-- │  ✗ Orphan CustomerKeys > 0: CTE exclusion filter has a bug.            │
-- │  ✗ Orphan ChannelKeys > 0: Script 01 was not run or has fewer rows.    │
-- │  ✗ Duplicate CustomerKeys > 0: ROW_NUMBER() filter (Rank=1) failed.    │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V7: Referential integrity (both counts must be 0)';

SELECT
    'Orphan CustomerKeys'       AS CheckName,
    COUNT(*)                    AS OrphanCount
FROM [gen].[CustomerAcquisition] AS ca
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[DimCustomer] AS c
    WHERE c.CustomerKey = ca.CustomerKey
)
UNION ALL
SELECT
    'Orphan AcquisitionChannelKeys',
    COUNT(*)
FROM [gen].[CustomerAcquisition] AS ca
WHERE NOT EXISTS (
    SELECT 1 FROM [gen].[DimAcquisitionChannel] AS ch
    WHERE ch.AcquisitionChannelKey = ca.AcquisitionChannelKey
)
UNION ALL
SELECT
    'Duplicate CustomerKeys (must be 0 — PK guarantee)',
    COUNT(*) - COUNT(DISTINCT CustomerKey)
FROM [gen].[CustomerAcquisition];


PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 02 completed successfully.';
PRINT '  Table created:  [gen].[CustomerAcquisition]';
PRINT '  Index created:  [IX_CustomerAcquisition_ChannelKey]';
PRINT '';
PRINT '  Next steps:';
PRINT '    Script 03 → gen.OrderPayment       (no dependency on this)';
PRINT '    Script 04 → gen.OrderFulfillment   (no dependency on this)';
PRINT '    Script 05 → gen.FactMarketingSpend (MUST run AFTER this)';
PRINT '════════════════════════════════════════════════════════════════';
GO
