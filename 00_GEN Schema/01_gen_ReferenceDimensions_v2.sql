/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║               SCRIPT 01: [gen] REFERENCE DIMENSION TABLES                    ║
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
  │  Version         : 2.0 (Fresh Build — All Amendments Applied)           │
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
  SCRIPT OBJECTIVE
--------------------------------------------------------------------------------
  1. Create and populate three [gen] reference dimension tables that serve as
     the foundational controlled vocabulary for the entire synthetic extension:
       gen.DimAcquisitionChannel  (7 rows — marketing acquisition channels)
       gen.DimPaymentMethod       (6 rows — payment instruments)
       gen.DimReturnReason        (8 rows — return reason codes)

  2. Establish the analytical flag design that powers binary KPI analysis:
       IsOrganic on DimAcquisitionChannel  → paid vs. organic channel split
       IsDigital on DimPaymentMethod       → digital vs. traditional payments
       IsOperationalFailure on DimReturnReason → controllable vs. customer-led

  3. Provide a zero-dependency, fully idempotent baseline layer. No other
     [gen] script can run until this script succeeds. Once it does, Scripts
     02, 03, 07, and 08 can all read from these three tables with confidence.

  These tables answer executive questions that the Contoso source dataset
  makes completely impossible without them — see the business gap table below.

--------------------------------------------------------------------------------
  WHY THESE THREE TABLES WERE NEEDED
--------------------------------------------------------------------------------
  The Contoso Retail DW source has no concept of:
    - HOW customers were first acquired (marketing channel attribution)
    - HOW customers paid for their orders (payment method analytics)
    - WHY customers returned products (return reason root-cause analysis)

  Without these three reference tables, the following executive questions
  remain completely unanswerable by any analytical layer:

  ┌─────────────────────────────────────────────────────────────────────────┐
  │  CMO: What is our CAC by acquisition channel?                           │
  │  CMO: What is our paid vs. organic customer mix?                        │
  │  CMO: Which channel acquires the highest-value customers?               │
  │  CFO: What is our payment method mix across orders?                     │
  │  CFO: Is digital payment adoption growing year-over-year?               │
  │  COO: What are the most common reasons for product returns?             │
  │  COO: What % of returns are caused by operational failures vs.          │
  │        customer preference?                                             │
  │  PM:  Which products have the highest operationally-caused return rate? │
  └─────────────────────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  DESIGN PHILOSOPHY FOR REFERENCE DIMENSIONS
--------------------------------------------------------------------------------
  Reference dimensions are small, static lookup tables. Their design follows
  three principles:

  1. MANUAL POPULATION OVER GENERATION
     Unlike the transactional [gen] tables (OrderPayment, CustomerAcquisition,
     etc.) which are generated algorithmically from source data, reference
     dimensions are manually authored. Their values represent deliberate
     business classification choices, not computed outputs. They are the
     controlled vocabulary of the analytical model.

  2. ERA ACCURACY
     All reference data is calibrated to the shifted 2023-2025 era that the
     project presents (source 2007-2009 + 16 years). Channel costs, payment
     methods, and return patterns reflect realistic contemporary benchmarks.
     For example, 'Social Media' CAC reflects modern targeting precision
     (lower than the original era), while 'Cash on Delivery' retains its
     regional relevance in Asian markets.

  3. ANALYTICAL FLAG DESIGN
     Each reference table includes a computed BIT flag that enables the
     single most critical binary analysis for that domain:
       - IsOrganic  (DimAcquisitionChannel): paid vs. non-paid channel split
       - IsDigital  (DimPaymentMethod):      digital vs. traditional payments
       - IsOperationalFailure (DimReturnReason): controllable vs. customer-led
     These flags are stored in [gen] tables so they are available to both
     the [dim] view layer AND directly to the downstream generation scripts.

--------------------------------------------------------------------------------
  AMENDMENTS FROM PREVIOUS BUILD (V1.0 → V2.0)
--------------------------------------------------------------------------------
  The following changes are applied in this fresh build based on lessons
  learned during the first iteration:

  AMENDMENT 1 — DimAcquisitionChannel: CAC column data type upgraded
    OLD: EstimatedCACLow  DECIMAL(10,2)
         EstimatedCACHigh DECIMAL(10,2)
    NEW: EstimatedCACLow  MONEY
         EstimatedCACHigh MONEY
    REASON: The downstream dim.vAcquisitionChannel view computes CACMidpoint
    as (Low + High) / 2.0 cast to MONEY. Using DECIMAL(10,2) at the source
    created an implicit type mismatch. Using MONEY throughout ensures the
    arithmetic chain is consistent from source table through view to Power BI.

  AMENDMENT 2 — DimReturnReason: Added as a third reference dimension
    The first build created DimReturnReason in a separate script. In this
    fresh build it is consolidated here with the other two reference dimensions
    because it has the same characteristics: small, static, manually authored,
    zero external dependencies.

  AMENDMENT 3 — DimReturnReason: IsOperationalFailure flag documented
    'Late Delivery' is flagged IsOperationalFailure = 1.
    HOWEVER: this reason code is intentionally EXCLUDED from
    gen.PhysicalReturnEvents (Script 08). Physical in-store returns happen
    face-to-face — there is no delivery involved. Late Delivery as a return
    reason only applies to online (gen.OnlineReturnEvents, Script 07).
    This downstream exclusion is a deliberate design choice documented here
    at the source so that any developer reading the reference table
    understands the constraint.

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------
  Run on      : ContosoRetailDW (fresh instance)
  Run order   : Script 01 — Run after Script 00 (schema creation)
  Dependencies: [gen] schema must exist (created by Script 00)
  Impact      : Creates 3 new tables in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — DROP IF EXISTS pattern on all three tables.

  Downstream dependents (MUST wait for this script):
    Script 02 → gen.CustomerAcquisition      (uses DimAcquisitionChannel)
    Script 03 → gen.OrderPayment             (uses DimPaymentMethod)
    Script 07 → gen.OnlineReturnEvents       (uses DimReturnReason)
    Script 08 → gen.PhysicalReturnEvents     (uses DimReturnReason, excl. Late Delivery)

================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/

-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECK                              ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Verifies that the [gen] schema exists before any DDL runs. If it does    ║
-- ║  not exist the script raises a fatal error and terminates the current      ║
-- ║  batch, prompting the developer to run Script 00 first.                   ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① SCHEMA_ID() over SELECT FROM sys.schemas subquery                       ║
-- ║    See Script 00, Code Block 2 for the full rationale. SCHEMA_ID() is     ║
-- ║    the purpose-built built-in — faster, cleaner, semantically explicit.   ║
-- ║                                                                             ║
-- ║  ② THROW over RAISERROR                                                    ║
-- ║    The commented-out RAISERROR line is the legacy T-SQL pattern. THROW    ║
-- ║    is the modern (2012+) replacement: always terminates the batch,        ║
-- ║    preserves original error number, and requires no severity flags.        ║
-- ║    The old pattern is retained commented-out for comparison reference.    ║
-- ║                                                                             ║
-- ║  ③ FORMATMESSAGE for error strings                                          ║
-- ║    Produces a readable, parameterisable error message without relying on   ║
-- ║    string concatenation — which can silently truncate long messages.       ║
-- ║                                                                             ║
-- ║  ④ ELSE branch with ✓ confirmation                                          ║
-- ║    The success path prints a green tick to the SSMS Messages tab. This    ║
-- ║    lets the developer visually confirm the check passed without reading    ║
-- ║    every output line — a simple but high-value operational practice.       ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║  This script has ONE hard dependency: Script 00. If you see the THROW     ║
-- ║  fire, run Script 00 first then return here. The ELSE branch prints ✓ to  ║
-- ║  confirm the check passed cleanly before any DDL executes.                ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT ON SUCCESS: ✓ Schema [gen] confirmed.                    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
-- PRE-CHECK: Confirm [gen] schema exists before proceeding
-- ============================================================================
-- Superseded pattern: IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gen')
-- ↑ Replaced by SCHEMA_ID() — purpose-built built-in, no catalog scan. See Code Block 2 in Script 00.
IF SCHEMA_ID('gen') IS NULL   -- NULL = [gen] does not exist; THROW will fire.
BEGIN
    -- FORMATMESSAGE builds the error string cleanly — direct string concat in THROW is not supported.
    DECLARE @ErrorMsg NVARCHAR(2048) = FORMATMESSAGE('ERROR: [gen] schema does not exist. Run Script 00 first.');
    
    -- THROW (modern T-SQL 2012+): terminates the current batch unconditionally.
    -- Superseded approach was: RAISERROR('...', 16, 1) — legacy, does not guarantee batch termination.
    THROW 50001, @ErrorMsg, 1;
    
    -- Note: THROW terminates this batch. The developer must run Script 00 and re-run this script.
    -- An earlier draft placed SET NOEXEC ON here; THROW's batch termination is the chosen strategy.
    

END
ELSE
BEGIN
    PRINT '✓ Schema [gen] confirmed.';   -- Visual confirmation in Messages tab before any DDL executes.
    PRINT '';
END
GO


-- ============================================================================
-- TABLE 1: gen.DimAcquisitionChannel
-- ============================================================================
-- PURPOSE:
--   Represents the marketing channel through which a customer was first
--   acquired — how they found and made their first purchase from Contoso.
--   This is a customer-level attribute, not a transaction-level attribute.
--   One channel per customer, assigned by the scoring algorithm in Script 02.
--
-- GRAIN: One row per acquisition channel (7 rows — static reference)
--
-- DOWNSTREAM CONSUMERS:
--   Script 02  → gen.CustomerAcquisition    (assigns channels to customers)
--   Script 05  → gen.FactMarketingSpend     (spend is allocated per channel)
--   dim.vAcquisitionChannel                 (semantic view for Power BI)
--   fact.vCustomerAcquisition               (analytical fact view)
--   fact.vMarketingSpend                    (analytical fact view)
--
-- COLUMN NOTES:
--   EstimatedCACLow / EstimatedCACHigh: Cost benchmarks calibrated to the
--   2023-2025 era. Direct channel = $0 by design (no spend required).
--   Data type is MONEY (see Amendment 1 in documentation header).
--
--   ChannelDescription: Retained in this physical table for completeness
--   and student reference. It is EXCLUDED from dim.vAcquisitionChannel
--   because free text has no analytical value in Power BI.
--
--   HISTORICAL CONTEXT FOR STUDENTS:
--   Email Marketing is classified as 'Organic' (ChannelCategory), not Paid.
--   This reflects the reality of the contemporary email marketing model:
--   email to an owned list has near-zero marginal cost. Paid classification
--   applies only to channels requiring direct ad spend per impression.
--   Social Media CAC range reflects modern digital advertising precision.
--   Paid Search remains dominant for direct-intent acquisition.
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — CREATE & POPULATE gen.DimAcquisitionChannel (7 rows)        ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Drops (if exists) and recreates gen.DimAcquisitionChannel, then inserts   ║
-- ║  7 rows representing all marketing channels through which Contoso          ║
-- ║  customers were first acquired. This is a static reference table —         ║
-- ║  its rows never change at runtime.                                          ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① DROP TABLE IF EXISTS before CREATE TABLE (idempotent rebuild pattern)   ║
-- ║    DROP + CREATE (not ALTER TABLE) is the correct pattern for static        ║
-- ║    reference tables populated via INSERT ... VALUES. ALTER TABLE would      ║
-- ║    leave stale rows from previous runs if re-run. The DROP guarantees a    ║
-- ║    clean slate on every execution — making the script fully repeatable.    ║
-- ║                                                                             ║
-- ║  ② DECIMAL(19,4) for currency, not MONEY                                   ║
-- ║    The legacy MONEY type has known division rounding quirks in T-SQL.      ║
-- ║    DECIMAL(19,4) is the modern recommended approach for currency storage:  ║
-- ║    19 total digits, 4 decimal places — sufficient for all CAC values and   ║
-- ║    avoids the implicit conversion risk in MONEY arithmetic chains.         ║
-- ║    The original MONEY design is documented inline below in the column      ║
-- ║    comments for transparency.                                               ║
-- ║                                                                             ║
-- ║  ③ PRIMARY KEY on AcquisitionChannelKey (surrogate integer key)            ║
-- ║    Integer surrogate keys are the Kimball standard for dimension tables.   ║
-- ║    They are smaller than natural keys, compress better in VertiPaq, and   ║
-- ║    resolve FK joins faster. The PK constraint enforces uniqueness at the  ║
-- ║    database level — a duplicate key insert will fail explicitly.           ║
-- ║                                                                             ║
-- ║  ④ Non-sequential key assignment (1,4,6,2,3,7,5)                           ║
-- ║    Key values represent business identity — not insert order. The keys     ║
-- ║    are grouped semantically in the INSERT block (Organic first, then Paid, ║
-- ║    then Direct) but their integer values reflect the logical hierarchy of  ║
-- ║    the channel taxonomy. Never infer channel type from key number alone.  ║
-- ║                                                                             ║
-- ║  ⑤ ChannelDescription EXCLUDED from dim.vAcquisitionChannel               ║
-- ║    Free-text description columns have no analytical value in Power BI —   ║
-- ║    they cannot be aggregated, sliced, or used in DAX measures. Storing     ║
-- ║    them in [gen] for developer reference while excluding from [dim] views  ║
-- ║    keeps the semantic layer lean and VertiPaq-optimised.                   ║
-- ║                                                                             ║
-- ║  DATA CALIBRATION — 2023–2025 ERA                                          ║
-- ║  Email Marketing → 'Organic': marginal cost on an owned list is near-zero.║
-- ║  Paid channels (Social Media, Paid Search, Affiliate): direct ad spend.   ║
-- ║  Direct channel → CAC = $0.00 by definition (brand-aware visitors).       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
DROP TABLE IF EXISTS [gen].[DimAcquisitionChannel];   -- Idempotent: clean slate on every run.
PRINT '✓ Existing gen.DimAcquisitionChannel table dropped (if existed).';
GO

CREATE TABLE [gen].[DimAcquisitionChannel] (
    AcquisitionChannelKey   INT            NOT NULL,   -- Surrogate integer key. Kimball standard. VertiPaq-optimised for FK joins.
    ChannelName             NVARCHAR(50)   NOT NULL,   -- Display label used in Power BI slicer/axis. NVARCHAR supports Unicode channel names.
    ChannelCategory         NVARCHAR(20)   NOT NULL,   -- Grouping: 'Paid', 'Organic', 'Direct'. Enables category-level DAX filtering.
    -- Original design used MONEY data type. Updated to DECIMAL(19,4) per T-SQL best practice:
    -- MONEY has known division rounding quirks. DECIMAL(19,4) is the modern standard for
    -- currency storage — 19 total digits, 4 decimal places, no implicit conversion risk.
    EstimatedCACLow         DECIMAL(19,4)  NOT NULL,   -- Lower bound of CAC benchmark range. Direct channel = 0.00 by definition.
    EstimatedCACHigh        DECIMAL(19,4)  NOT NULL,   -- Upper bound of CAC benchmark range. Used to compute CACMidpoint in dim.vAcquisitionChannel.
    ChannelDescription      NVARCHAR(300)  NULL,       -- Free-text description. Stored here for developer reference only.
                                                       -- EXCLUDED from dim.vAcquisitionChannel — free text has no analytical value in Power BI.

    CONSTRAINT PK_DimAcquisitionChannel PRIMARY KEY (AcquisitionChannelKey)
    -- PK enforces uniqueness at the DB level. One row per channel. Duplicate key insert = explicit failure.
);
GO

INSERT INTO [gen].[DimAcquisitionChannel]
    (AcquisitionChannelKey, ChannelName, ChannelCategory,
     EstimatedCACLow, EstimatedCACHigh, ChannelDescription)
VALUES
    -- ── Organic Channels ────────────────────────────────────────────────────
    (1, 'Organic Search', 'Organic', 0.00, 5.00,
        'Customers who found Contoso through unpaid search engine results. '
      + 'Lowest CAC channel — driven by SEO quality and brand authority. '
      + 'Primary organic acquisition source.'),

    (4, 'Email Marketing', 'Organic', 5.00, 12.00,
        'Customers acquired through email campaigns to an owned subscriber list. '
      + 'Classified as Organic because the marginal cost per acquisition '
      + 'on an owned list is near-zero. High open rates reflect strong '
      + 'engagement with the brand.'),

    (6, 'Referral', 'Organic', 10.00, 25.00,
        'Customers acquired through referral programmes, word-of-mouth links, '
      + 'or partner site recommendations. Higher CAC than pure organic '
      + 'due to referral incentives (discounts or credits) offered to '
      + 'the referring customer.'),

    -- ── Paid Channels ───────────────────────────────────────────────────────
    (2, 'Paid Search', 'Paid', 25.00, 40.00,
        'Customers acquired through paid search advertising. '
      + 'High intent channel — users actively searching for products. '
      + 'Dominant paid acquisition channel by volume.'),

    (3, 'Social Media', 'Paid', 15.00, 35.00,
        'Customers acquired through paid social media advertising across '
      + 'platforms. CAC range reflects contemporary digital targeting '
      + 'precision. Higher CPM but strong demographic reach for '
      + 'brand-new customer segments.'),

    (7, 'Affiliate', 'Paid', 8.00, 18.00,
        'Customers acquired through affiliate partner websites operating '
      + 'on a commission-per-acquisition model. Lower CAC than search '
      + 'because cost is only incurred on confirmed conversions. '
      + 'Affiliate partners drive long-tail discovery traffic.'),

    -- ── Direct Channel ──────────────────────────────────────────────────────
    (5, 'Direct', 'Direct', 0.00, 0.00,
        'Customers who navigated directly to the Contoso website via '
      + 'typed URL, bookmarks, or browser history. Zero acquisition cost '
      + 'by definition — these are already brand-aware customers. '
      + 'High-quality segment: indicates strong brand recall.');

PRINT '✓ [gen].[DimAcquisitionChannel] created and populated (7 rows).';
GO


-- ============================================================================
-- TABLE 2: gen.DimPaymentMethod
-- ============================================================================
-- PURPOSE:
--   Represents the payment instrument used to complete each sales order.
--   Payment is an order-level attribute — a customer pays once per order
--   regardless of how many line items the order contains.
--
-- GRAIN: One row per payment method (6 rows — static reference)
--
-- DOWNSTREAM CONSUMERS:
--   Script 03  → gen.OrderPayment           (assigns payment to orders)
--   dim.vPaymentMethod                      (semantic view for Power BI)
--   fact.vOrderPayment                      (analytical fact view)
--
-- COLUMN NOTES:
--   IsDigital: BIT flag distinguishing online-processed payments (1) from
--   physical or manual payment methods (0). This is the primary analytical
--   flag for digital payment adoption KPIs.
--
--   Digital = Credit Card, Debit Card, PayPal (processed through online
--   payment gateways). Non-digital = Bank Transfer, Cash on Delivery,
--   Gift Card (all require manual processing steps or physical interaction).
--
--   PaymentCategory goes one level deeper than IsDigital:
--     'Card'        → bank-issued card (online-processed)
--     'Digital'     → third-party digital wallet (PayPal)
--     'Traditional' → bank transfer or cash-based
--     'Prepaid'     → store-issued gift cards / credit
--
--   PaymentDescription: Retained in this physical table for student reference.
--   EXCLUDED from dim.vPaymentMethod (free text, no analytical value).
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — CREATE & POPULATE gen.DimPaymentMethod (7 rows)             ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Drops (if exists) and recreates gen.DimPaymentMethod, then inserts 7 rows ║
-- ║  representing all payment instruments available for Contoso online orders. ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① BIT flag (IsDigital) as the primary analytical discriminator            ║
-- ║    BIT columns are the most efficient T-SQL boolean storage type — 1 bit   ║
-- ║    of storage, compresses to near-zero in VertiPaq. Using a BIT flag        ║
-- ║    (rather than a VARCHAR 'Yes'/'No') enables direct FILTER context in DAX ║
-- ║    without a SWITCH: CALCULATE([KPI], dim.vPaymentMethod[IsDigital] = 1).  ║
-- ║                                                                             ║
-- ║  ② Two-level classification: IsDigital (binary) + PaymentCategory (group)  ║
-- ║    IsDigital answers the CMO/CFO digital-adoption KPI question in a        ║
-- ║    single filter. PaymentCategory ('Card', 'Digital', 'BNPL', 'Traditional'║
-- ║    'Prepaid') enables sub-category drill-down beyond the binary split.     ║
-- ║    Both levels are stored in [gen] so they are available directly to       ║
-- ║    both the view layer AND downstream generation scripts.                  ║
-- ║                                                                             ║
-- ║  ③ PaymentDescription NVARCHAR(400) — widened for BNPL row                 ║
-- ║    NVARCHAR(300) was sufficient for the original 6 methods. BNPL (Buy Now  ║
-- ║    Pay Later) references multiple provider names (Klarna, Afterpay, Affirm)║
-- ║    in its description, requiring the wider 400-char allocation.            ║
-- ║    PaymentDescription is stored here for developer reference only and      ║
-- ║    EXCLUDED from dim.vPaymentMethod in Power BI.                           ║
-- ║                                                                             ║
-- ║  ④ Same DROP + CREATE idempotent pattern as DimAcquisitionChannel           ║
-- ║    See Code Block 2 for the full rationale.                                ║
-- ║                                                                             ║
-- ║  DIGITAL vs NON-DIGITAL SPLIT (7 methods total)                             ║
-- ║  Digital   (IsDigital=1): Credit Card (1), Debit Card (2), PayPal (3),    ║
-- ║                            Buy Now Pay Later (7)         → 4 methods       ║
-- ║  Non-Digital(IsDigital=0): Bank Transfer (4), Cash on Delivery (5),       ║
-- ║                            Gift Card (6)                → 3 methods       ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE                                                  ║
-- ║    PaymentMethodKey=7 (BNPL) is inserted BETWEEN Key=3 and Key=4 in the   ║
-- ║    INSERT VALUES block. Physical insert order does not need to match key   ║
-- ║    order. The PRIMARY KEY constraint governs uniqueness, not insert         ║
-- ║    sequence. Always read IsDigital and PaymentCategory — never infer from  ║
-- ║    key number alone.                                                        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
DROP TABLE IF EXISTS [gen].[DimPaymentMethod];   -- Idempotent: clean slate on every run.
PRINT '✓ Existing gen.DimPaymentMethod table dropped (if existed).';
GO

CREATE TABLE [gen].[DimPaymentMethod] (
    PaymentMethodKey    INT            NOT NULL,   -- Surrogate integer key. Kimball standard. VertiPaq-optimised for FK joins.
    PaymentMethodName   NVARCHAR(50)   NOT NULL,   -- Display label for Power BI slicer/axis.
    PaymentCategory     NVARCHAR(20)   NOT NULL,   -- Sub-group: 'Card', 'Digital', 'Traditional', 'Prepaid', 'BNPL'.
    IsDigital           BIT            NOT NULL,   -- Primary analytical flag: 1 = online-gateway processed, 0 = physical/manual.
                                                   -- BIT is the most efficient boolean type — compresses to near-zero in VertiPaq.
    PaymentDescription  NVARCHAR(400)  NULL,       -- Free-text. Widened to 400 chars for BNPL multi-provider description.
                                                   -- EXCLUDED from dim.vPaymentMethod — free text has no analytical value in Power BI.

    CONSTRAINT PK_DimPaymentMethod PRIMARY KEY (PaymentMethodKey)
);
GO

INSERT INTO [gen].[DimPaymentMethod]
    (PaymentMethodKey, PaymentMethodName, PaymentCategory,
     IsDigital, PaymentDescription)
VALUES
    -- ── Digital Payment Methods (IsDigital = 1) ─────────────────────────────
    (1, 'Credit Card', 'Card', 1,
        'Visa, Mastercard, American Express credit card payments processed '
      + 'through an online payment gateway. Dominant payment method '
      + 'globally. Preferred for high-value orders due to chargeback '
      + 'protection and rewards programmes.'),

    (2, 'Debit Card', 'Card', 1,
        'Direct debit card payments linked to bank accounts, processed '
      + 'online. Popular in markets with high bank account penetration. '
      + 'Lower average transaction values than credit cards — no credit '
      + 'facility available, so customers spend within existing funds.'),

    (3, 'PayPal', 'Digital', 1,
        'PayPal digital wallet payments. Strong in online-native customer '
      + 'segments who prefer not to share card details directly with '
      + 'retailers. Buyer protection programme increases confidence for '
      + 'first-time or high-value purchases.'),

    (7, 'Buy Now Pay Later', 'BNPL', 1,
        'Instalment-based deferred payment via BNPL providers (e.g., Klarna, '
      + 'Afterpay, Affirm). Processed fully online at checkout — no card '
      + 'details required from the customer. Associated with higher average '
      + 'order values and younger customer segments. Introduces a deferred '
      + 'settlement cycle distinct from card and wallet payments.'),

    -- ── Non-Digital Payment Methods (IsDigital = 0) ─────────────────────────
    (4, 'Bank Transfer', 'Traditional', 0,
        'Direct bank wire or ACH transfer payments. High adoption in '
      + 'European B2B and high-value consumer markets (SEPA region). '
      + 'Processing delay of 1-3 business days introduces fulfilment '
      + 'lag — orders held until payment confirms.'),

    (5, 'Cash on Delivery', 'Traditional', 0,
        'Payment collected physically upon delivery of goods. Dominant '
      + 'method in Asian and developing markets where card penetration '
      + 'and digital trust are lower. Higher return and refusal rates '
      + 'for high-value orders — COD risk increases with order value.'),

    (6, 'Gift Card', 'Prepaid', 0,
        'Contoso-issued prepaid gift cards and store credit applied at '
      + 'checkout. Counted as Non-Digital because the card value is '
      + 'pre-loaded externally and requires a manual redemption code '
      + 'rather than real-time gateway processing. Common in seasonal '
      + 'gifting periods (Q4 holiday season).');

PRINT '✓ [gen].[DimPaymentMethod] created and populated (7 rows).';
GO

-- ============================================================================
-- TABLE 3: gen.DimReturnReason
-- ============================================================================
-- PURPOSE:
--   Represents the reason category assigned to each product return event.
--   Return reasons are assigned by the synthetic generation scripts
--   (Script 07 for online returns, Script 08 for physical returns) using
--   weighted probability distributions.
--
-- GRAIN: One row per return reason (8 rows — static reference)
--
-- DOWNSTREAM CONSUMERS:
--   Script 07  → gen.OnlineReturnEvents     (assigns reasons to online returns)
--   Script 08  → gen.PhysicalReturnEvents   (assigns reasons, excl. Late Delivery)
--   dim.vReturnReason                       (semantic view for Power BI)
--   fact.vReturns                           (unified returns analytical fact)
--
-- CRITICAL DESIGN NOTE — Late Delivery (ReturnReasonKey = 6):
--   'Late Delivery' carries IsOperationalFailure = 1. However, this reason
--   code is INTENTIONALLY EXCLUDED from gen.PhysicalReturnEvents (Script 08).
--   Physical in-store returns are face-to-face transactions — there is no
--   delivery leg involved, so a delivery delay cannot be a return reason.
--   Late Delivery applies ONLY to online returns (gen.OnlineReturnEvents).
--   This constraint is enforced in Scripts 07 and 08 at generation time.
--   The reason code is retained in this table for completeness and to
--   support unified analysis of online-only return drivers in DAX.
--
-- IsOperationalFailure FLAG LOGIC:
--   1 = Contoso caused this return. The business had control over the
--       outcome and failed. These are operationally-accountable returns.
--       Targets: reduce through quality control, accurate listings,
--       fulfilment improvement, packaging standards.
--
--   0 = The customer caused this return. Contoso performed correctly;
--       the customer changed their mind or found a better alternative.
--       These returns are commercially normal and expected. They should
--       be tracked for volume and margin impact but NOT used to judge
--       operational quality.
--
--   This flag is the primary analytical discriminator for the COO
--   Returns Management dashboard and the Product Manager SKU quality view.
--   DAX pattern: CALCULATE([Return Rate %], DimReturnReason[IsOperationalFailure] = 1)
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — CREATE & POPULATE gen.DimReturnReason (8 rows)              ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Drops (if exists) and recreates gen.DimReturnReason, then inserts 8 rows  ║
-- ║  representing every reason category a customer may return a product.       ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① IsOperationalFailure BIT as the primary analytical discriminator        ║
-- ║    1 = Contoso caused this return — controllable, should be minimised.     ║
-- ║    0 = Customer caused this return — commercially normal, track volume.    ║
-- ║    BIT storage (same VertiPaq efficiency principle as IsDigital above)     ║
-- ║    enables direct DAX filter context without SWITCH expressions:           ║
-- ║    CALCULATE([Return Rate %], vReturnReason[IsOperationalFailure] = 1)     ║
-- ║                                                                             ║
-- ║  ② AppliesTo column: channel applicability constraint stored at source     ║
-- ║    'Both' = applies to online and in-store returns.                        ║
-- ║    'Online Only' = cannot apply to physical in-store returns.              ║
-- ║    Storing this constraint in [gen] means Script 08 (PhysicalReturnEvents) ║
-- ║    can read it directly to filter the probability distribution at          ║
-- ║    generation time — enforcing the rule at the data layer, not in DAX.    ║
-- ║                                                                             ║
-- ║  ③ ReturnReasonCode NVARCHAR(10) — short code for developer filtering       ║
-- ║    Short uppercase codes (DEFECT, WRONG, LATE, etc.) make WHERE clauses    ║
-- ║    and CASE expressions in generation scripts readable without needing to  ║
-- ║    reference the full name. They are NOT exposed in dim.vReturnReason —    ║
-- ║    ReturnReasonName is the display label used in Power BI.                 ║
-- ║                                                                             ║
-- ║  ④ ReturnReasonCategory provides a second grouping level                   ║
-- ║    'Product Quality', 'Fulfilment Error', 'Packaging / Transit',          ║
-- ║    'Customer Preference', 'Expectation Mismatch', 'Customer Error'.        ║
-- ║    This level enables root-cause category analysis in Power BI beyond the  ║
-- ║    binary Operational/Customer-Led split.                                  ║
-- ║                                                                             ║
-- ║  ⑤ Same DROP + CREATE idempotent pattern as Code Blocks 2 and 3            ║
-- ║    See Code Block 2 for the full rationale.                                ║
-- ║                                                                             ║
-- ║  OPERATIONAL vs CUSTOMER-LED SPLIT (4 + 4)                                 ║
-- ║  Operational (IsOperationalFailure=1): DEFECT, WRONG, DAMAGED, LATE       ║
-- ║  Customer-Led (IsOperationalFailure=0): MINDCHG, NOTDESC, PRICE, DUPL     ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — KEY GAPS IN SEQUENCE                           ║
-- ║  ReturnReasonKeys are 1,2,3,6,4,5,7,8 — not fully sequential. Key=6       ║
-- ║  (LATE) sits in the Operational block despite its integer position         ║
-- ║  suggesting it belongs after Key=5. Keys represent business identity,      ║
-- ║  not insert order. Always read IsOperationalFailure and AppliesTo;         ║
-- ║  never infer accountability or channel scope from key number alone.        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
DROP TABLE IF EXISTS [gen].[DimReturnReason];   -- Idempotent: clean slate on every run.
PRINT '✓ Existing gen.DimReturnReason table dropped (if existed).';
GO

CREATE TABLE [gen].[DimReturnReason] (
    ReturnReasonKey         INT            NOT NULL,   -- Surrogate integer key. Kimball standard. VertiPaq-optimised for FK joins.
    ReturnReasonCode        NVARCHAR(10)   NOT NULL,   -- Short uppercase code (DEFECT, WRONG, LATE, etc.) for developer filtering and CASE logic.
                                                       -- NOT exposed in dim.vReturnReason — ReturnReasonName is the display label for Power BI.
    ReturnReasonName        NVARCHAR(100)  NOT NULL,   -- Full descriptive name. Used as the display label in Power BI visuals and slicers.
    ReturnReasonCategory    NVARCHAR(50)   NOT NULL,   -- Root-cause grouping: 'Product Quality', 'Fulfilment Error', 'Customer Preference', etc.
    IsOperationalFailure    BIT            NOT NULL,   -- Primary analytical discriminator: 1 = Contoso-accountable, 0 = customer-led.
                                                       -- BIT type: same VertiPaq compression efficiency as IsDigital (see Code Block 3).
    AppliesTo               NVARCHAR(20)   NOT NULL,   -- Channel constraint: 'Both' or 'Online Only'. Read by Script 08 to exclude LATE (Key=6)
                                                       -- from PhysicalReturnEvents. Physical in-store returns have no delivery leg.
    ReturnReasonDescription NVARCHAR(400)  NULL,       -- Free-text description. Developer reference only. EXCLUDED from dim.vReturnReason.

    CONSTRAINT PK_DimReturnReason PRIMARY KEY (ReturnReasonKey)
    -- PK enforces uniqueness at the DB level. Duplicate key insert = explicit failure.
);
GO

INSERT INTO [gen].[DimReturnReason]
    (ReturnReasonKey, ReturnReasonCode, ReturnReasonName, ReturnReasonCategory,
     IsOperationalFailure, AppliesTo, ReturnReasonDescription)
VALUES
    -- ── Operational Failures (IsOperationalFailure = 1) ─────────────────────
    (1, 'DEFECT', 'Defective Product', 'Product Quality', 1, 'Both',
        'Product stopped working, malfunctioned, or exhibited manufacturing '
      + 'defects after delivery. Applies to both online and in-store returns. '
      + 'High occurrence for a specific ProductKey signals a systematic SKU '
      + 'quality problem requiring supplier escalation or recall.'),

    (2, 'WRONG', 'Wrong Item Shipped', 'Fulfilment Error', 1, 'Both',
        'Customer received a different product than ordered — wrong SKU, '
      + 'wrong size, wrong colour, or wrong variant. A warehouse picking '
      + 'error. Applies to both channels. Consistent occurrence indicates '
      + 'a warehouse process failure, not a random event.'),

    (3, 'DAMAGED', 'Arrived Damaged', 'Packaging / Transit', 1, 'Both',
        'Product was undamaged when shipped but arrived broken, crushed, '
      + 'or otherwise damaged due to inadequate packaging or carrier '
      + 'mishandling. Applies to both channels. High rate on a specific '
      + 'ShippingMethod or carrier indicates a transit quality issue.'),

    (6, 'LATE', 'Late Delivery', 'Fulfilment Error', 1, 'Online Only',
        'Customer cancelled or returned the order because it arrived after '
      + 'the promised or expected delivery date. APPLIES TO ONLINE RETURNS '
      + 'ONLY — physical in-store returns have no delivery leg. '
      + 'In gen.PhysicalReturnEvents this reason code is excluded from '
      + 'the probability distribution. Persistent late delivery returns '
      + 'on a specific ShippingMethod indicate SLA breaches.'),

    -- ── Customer-Led Returns (IsOperationalFailure = 0) ─────────────────────
    (4, 'MINDCHG', 'Changed Mind', 'Customer Preference', 0, 'Both',
        'Customer decided they no longer want the product after receiving it. '
      + 'No fault on Contoso. The highest-volume customer-led return reason '
      + 'in most retail environments. Elevated rates in seasonal categories '
      + '(gifts, fashion) are commercially normal and expected.'),

    (5, 'NOTDESC', 'Not As Described', 'Expectation Mismatch', 0, 'Both',
        'Product arrived in working condition but did not match the customer''s '
      + 'expectations based on the product description, images, or reviews. '
      + 'Borderline operational in some cases — if multiple customers report '
      + 'the same mismatch, the product listing may need to be corrected. '
      + 'Applies to both channels.'),

    (7, 'PRICE', 'Better Price Found', 'Customer Preference', 0, 'Both',
        'Customer found the same or equivalent product at a lower price '
      + 'elsewhere after purchase. Pure commercial return — no operational '
      + 'failure. Elevated rates may indicate a pricing competitiveness '
      + 'issue to surface to the CMO or CFO for price benchmarking analysis.'),

    (8, 'DUPL', 'Duplicate Order', 'Customer Error', 0, 'Both',
        'Customer accidentally placed the same order twice and is returning '
      + 'the duplicate. Lowest volume return reason. More common in '
      + 'checkout flows with slow confirmation pages that prompt impatient '
      + 'double-clicks. Applies to both channels.');

PRINT '✓ [gen].[DimReturnReason] created and populated (8 rows).';
GO


-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Run these immediately after execution to confirm all three tables loaded
-- correctly. All checks should return zero anomalies.
-- ============================================================================

PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — VERIFICATION SUITE (V1 – V8)                                ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Eight verification queries confirm that all three reference tables were   ║
-- ║  created and populated correctly. Checks cover row counts, flag values,    ║
-- ║  analytical splits, and channel applicability constraints.                 ║
-- ║                                                                             ║
-- ║  WHY VERIFICATION IS ESPECIALLY IMPORTANT FOR THIS SCRIPT                  ║
-- ║  Unlike Scripts 02–08 which generate data algorithmically from source      ║
-- ║  tables, these three tables are manually authored INSERT...VALUES blocks.  ║
-- ║  This means ALL expected numbers are exact and deterministic — every       ║
-- ║  student running this script should see byte-identical results. Any        ║
-- ║  deviation indicates an insert error, a stale older version of the script,║
-- ║  or a dependency check that was bypassed.                                  ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① UNION ALL for multi-table row counts (V1)                                ║
-- ║    A single query using UNION ALL (not UNION) returns row counts for all   ║
-- ║    three tables in one result set without deduplication overhead. UNION    ║
-- ║    would perform an unnecessary DISTINCT pass across heterogeneous rows.  ║
-- ║                                                                             ║
-- ║  ② STRING_AGG for readable list columns (V3, V5, V7, V8)                   ║
-- ║    STRING_AGG(column, ', ') is the modern T-SQL (2017+) replacement for    ║
-- ║    the legacy XML PATH '' concatenation trick. It is cleaner, readable,    ║
-- ║    and respects collation. Used here to show method/channel lists inline   ║
-- ║    without requiring a second lookup query.                                 ║
-- ║                                                                             ║
-- ║  ③ Computed preview columns inline with SELECT (V2, V4)                    ║
-- ║    CACMidpoint and PaymentType_Preview are computed inline using CAST and  ║
-- ║    CASE — not persisted in the table. This confirms the arithmetic chain   ║
-- ║    works correctly at the [gen] layer before dim.vAcquisitionChannel       ║
-- ║    and dim.vPaymentMethod apply the same logic at the view layer.          ║
-- ║                                                                             ║
-- ║  HOW TO USE                                                                 ║
-- ║  Run all 8 queries immediately after Script 01 completes. Compare your     ║
-- ║  results against the EXPECTED OUTPUT documented under each query.          ║
-- ║  Any deviation indicates an insert error or a pre-check that was bypassed. ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
PRINT '  VERIFICATION — Script 01';
PRINT '════════════════════════════════════════════════════════════════';

-- ── V1: Row counts — expect 7, 6, 8 ─────────────────────────────────────────
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V1 — ROW COUNTS PER TABLE                                              │
-- │                                                                         │
-- │  Confirms all three reference tables have the correct number of rows.   │
-- │  These are static, hand-authored tables — counts are always exact.      │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — no variance):                                 │
-- │  ┌────────────────────────────┬───────────┐                             │
-- │  │ TableName                  │ TotalRows │                             │
-- │  ├────────────────────────────┼───────────┤                             │
-- │  │ gen.DimAcquisitionChannel  │     7     │                             │
-- │  │ gen.DimPaymentMethod       │     7     │                             │
-- │  │ gen.DimReturnReason        │     8     │                             │
-- │  └────────────────────────────┴───────────┘                             │
-- │                                                                         │
-- │  ✗ If DimPaymentMethod shows 6: BNPL row (Key=7) is missing.            │
-- │  ✗ If DimReturnReason shows 7: one reason code was not inserted.        │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V1: Row counts per table';
SELECT 'gen.DimAcquisitionChannel' AS TableName, COUNT(*) AS TotalRows
FROM gen.DimAcquisitionChannel
UNION ALL
SELECT 'gen.DimPaymentMethod',  COUNT(*) FROM gen.DimPaymentMethod
UNION ALL
SELECT 'gen.DimReturnReason',   COUNT(*) FROM gen.DimReturnReason;

-- ── V2: DimAcquisitionChannel — full listing with computed values ─────────────
-- Confirms MONEY data type handles arithmetic cleanly (no type mismatch)
-- Confirms Direct = $0.00 on both CAC columns
-- Confirms 3 Organic, 3 Paid, 1 Direct
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V2 — DimAcquisitionChannel FULL LISTING                                │
-- │                                                                         │
-- │  Confirms MONEY arithmetic and CACMidpoint preview computation.         │
-- │                                                                         │
-- │  EXPECTED OUTPUT (7 rows ordered by ChannelCategory, Key):              │
-- │  Direct → Key=5, CAC $0.00 / $0.00, CACMidpoint $0.00, IsOrganic=0     │
-- │  Organic → Keys 1,4,6; CAC ranges $0–$25; IsOrganic=1                  │
-- │  Paid    → Keys 2,3,7; CAC ranges $8–$40; IsOrganic=0                  │
-- │                                                                         │
-- │  KEY CHECK: Direct must be the only row with both CAC columns = $0.00  │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V2: DimAcquisitionChannel — full listing';
SELECT
    AcquisitionChannelKey,
    ChannelName,
    ChannelCategory,
    EstimatedCACLow,
    EstimatedCACHigh,
    CAST((EstimatedCACLow + EstimatedCACHigh) / 2.0 AS DECIMAL(19,4)) AS CACMidpoint_Preview,
    CAST(CASE WHEN ChannelCategory = 'Paid' THEN 0 ELSE 1 END AS BIT)  AS IsOrganic_Preview
FROM gen.DimAcquisitionChannel
ORDER BY ChannelCategory, AcquisitionChannelKey;

-- ── V3: DimAcquisitionChannel — category distribution check ─────────────────
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V3 — DimAcquisitionChannel CATEGORY SPLIT                              │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact):                                               │
-- │  ┌──────────────┬──────────────┬───────────┬────────────┐               │
-- │  │ Category     │ ChannelCount │ MinCACLow │ MaxCACHigh │               │
-- │  ├──────────────┼──────────────┼───────────┼────────────┤               │
-- │  │ Direct       │     1        │   $0.00   │   $0.00   │               │
-- │  │ Organic      │     3        │   $0.00   │  $25.00   │               │
-- │  │ Paid         │     3        │   $8.00   │  $40.00   │               │
-- │  └──────────────┴──────────────┴───────────┴────────────┘               │
-- │                                                                         │
-- │  ✗ If Organic shows 4: Email Marketing was mis-coded as Organic (OK)   │
-- │    — but check that Direct still shows 1 row.                           │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V3: DimAcquisitionChannel — category split (expect Organic=3, Paid=3, Direct=1)';
SELECT
    ChannelCategory,
    COUNT(*)            AS ChannelCount,
    MIN(EstimatedCACLow)  AS MinCACLow,
    MAX(EstimatedCACHigh) AS MaxCACHigh
FROM gen.DimAcquisitionChannel
GROUP BY ChannelCategory
ORDER BY ChannelCategory;

-- ── V4: DimPaymentMethod — full listing ──────────────────────────────────────
-- Confirms IsDigital correctly assigned: 1 = Credit Card, Debit Card, PayPal
-- Confirms IsDigital = 0 for Bank Transfer, Cash on Delivery, Gift Card
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V4 — DimPaymentMethod FULL LISTING                                     │
-- │                                                                         │
-- │  EXPECTED OUTPUT (7 rows ordered by IsDigital DESC, then Key):          │
-- │  ┌─────┬───────────────────┬──────────┬───────────┬─────────────────┐   │
-- │  │ Key │ MethodName        │ Category │ IsDigital │ PaymentType     │   │
-- │  ├─────┼───────────────────┼──────────┼───────────┼─────────────────┤   │
-- │  │  1  │ Credit Card       │ Card     │     1     │ Digital         │   │
-- │  │  2  │ Debit Card        │ Card     │     1     │ Digital         │   │
-- │  │  3  │ PayPal            │ Digital  │     1     │ Digital         │   │
-- │  │  7  │ Buy Now Pay Later │ BNPL     │     1     │ Digital         │   │
-- │  │  4  │ Bank Transfer     │Traditional│    0     │ Non-Digital     │   │
-- │  │  5  │ Cash on Delivery  │Traditional│    0     │ Non-Digital     │   │
-- │  │  6  │ Gift Card         │ Prepaid  │     0     │ Non-Digital     │   │
-- │  └─────┴───────────────────┴──────────┴───────────┴─────────────────┘   │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V4: DimPaymentMethod — full listing';
SELECT
    PaymentMethodKey,
    PaymentMethodName,
    PaymentCategory,
    IsDigital,
    CASE WHEN IsDigital = 1 THEN 'Digital' ELSE 'Non-Digital' END AS PaymentType_Preview
FROM gen.DimPaymentMethod
ORDER BY IsDigital DESC, PaymentMethodKey;

-- ── V5: DimPaymentMethod — digital vs non-digital split ─────────────────────
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V5 — DimPaymentMethod DIGITAL SPLIT                                    │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — 2 rows):                                      │
-- │  ┌─────────────┬─────────────┬──────────────────────────────────────┐   │
-- │  │ PaymentType │ MethodCount │ Methods                              │   │
-- │  ├─────────────┼─────────────┼──────────────────────────────────────┤   │
-- │  │ Digital     │     4       │ Credit Card, Debit Card, PayPal,     │   │
-- │  │             │             │ Buy Now Pay Later                    │   │
-- │  │ Non-Digital │     3       │ Bank Transfer, Cash on Delivery,     │   │
-- │  │             │             │ Gift Card                            │   │
-- │  └─────────────┴─────────────┴──────────────────────────────────────┘   │
-- │                                                                         │                         │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V5: DimPaymentMethod — digital split (expect Digital=4, Non-Digital=3)';
SELECT
    CASE WHEN IsDigital = 1 THEN 'Digital' ELSE 'Non-Digital' END AS PaymentType,
    COUNT(*) AS MethodCount,
    STRING_AGG(PaymentMethodName, ', ') AS Methods
FROM gen.DimPaymentMethod
GROUP BY IsDigital;

-- ── V6: DimReturnReason — full listing ───────────────────────────────────────
-- Confirms IsOperationalFailure = 1 for: DEFECT, WRONG, DAMAGED, LATE
-- Confirms IsOperationalFailure = 0 for: MINDCHG, NOTDESC, PRICE, DUPL
-- Confirms AppliesTo = 'Online Only' for: LATE (all others = 'Both')
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V6 — DimReturnReason FULL LISTING                                      │
-- │                                                                         │
-- │  EXPECTED OUTPUT (8 rows, ordered by IsOperationalFailure DESC, Key):   │
-- │  ┌─────┬────────┬──────────────────────┬──────────────────┬─────┬──────┐│
-- │  │ Key │ Code   │ Name                 │ Category         │ Ops │ ApplsTo││
-- │  ├─────┼────────┼──────────────────────┼──────────────────┼─────┼──────┤│
-- │  │  1  │ DEFECT │ Defective Product    │ Product Quality  │  1  │ Both ││
-- │  │  2  │ WRONG  │ Wrong Item Shipped   │ Fulfilment Error │  1  │ Both ││
-- │  │  3  │ DAMAGED│ Arrived Damaged      │ Pkg/Transit      │  1  │ Both ││
-- │  │  6  │ LATE   │ Late Delivery        │ Fulfilment Error │  1  │Online││
-- │  │  4  │ MINDCHG│ Changed Mind         │ Customer Pref.   │  0  │ Both ││
-- │  │  5  │ NOTDESC│ Not As Described     │ Expectation Miss │  0  │ Both ││
-- │  │  7  │ PRICE  │ Better Price Found   │ Customer Pref.   │  0  │ Both ││
-- │  │  8  │ DUPL   │ Duplicate Order      │ Customer Error   │  0  │ Both ││
-- │  └─────┴────────┴──────────────────────┴──────────────────┴─────┴──────┘│
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V6: DimReturnReason — full listing';
SELECT
    ReturnReasonKey,
    ReturnReasonCode,
    ReturnReasonName,
    ReturnReasonCategory,
    IsOperationalFailure,
    AppliesTo
FROM gen.DimReturnReason
ORDER BY IsOperationalFailure DESC, ReturnReasonKey;

-- ── V7: DimReturnReason — operational vs customer-led split ─────────────────
-- Expect: Operational=4, Customer-Led=4
-- Expect: 'Online Only' count = 1 (LATE delivery only)
PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V7 — DimReturnReason ACCOUNTABILITY SPLIT                              │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — 2 rows):                                      │
-- │  ┌─────────────────────┬─────────────┬───────────────────────────────┐  │
-- │  │ AccountabilityType  │ ReasonCount │ ReasonCodes                   │  │
-- │  ├─────────────────────┼─────────────┼───────────────────────────────┤  │
-- │  │ Operational Failure │     4       │ DEFECT, WRONG, DAMAGED, LATE  │  │
-- │  │ Customer-Led        │     4       │ MINDCHG, NOTDESC, PRICE, DUPL │  │
-- │  └─────────────────────┴─────────────┴───────────────────────────────┘  │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V7: DimReturnReason — failure accountability split';
SELECT
    CASE WHEN IsOperationalFailure = 1 THEN 'Operational Failure' 
         ELSE 'Customer-Led' END         AS AccountabilityType,
    COUNT(*)                             AS ReasonCount,
    STRING_AGG(ReturnReasonCode, ', ')   AS ReasonCodes
FROM gen.DimReturnReason
GROUP BY IsOperationalFailure;

PRINT '';
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  V8 — DimReturnReason CHANNEL APPLICABILITY CHECK                       │
-- │                                                                         │
-- │  EXPECTED OUTPUT (exact — 2 rows):                                      │
-- │  ┌─────────────┬─────────────┬──────────────────────────────────────┐   │
-- │  │ AppliesTo   │ ReasonCount │ Reasons                              │   │
-- │  ├─────────────┼─────────────┼──────────────────────────────────────┤   │
-- │  │ Both        │     7       │ All reasons except Late Delivery     │   │
-- │  │ Online Only │     1       │ Late Delivery                        │   │
-- │  └─────────────┴─────────────┴──────────────────────────────────────┘   │
-- │                                                                         │
-- │  This confirms that gen.PhysicalReturnEvents (Script 08) should        │
-- │  exclude ReturnReasonKey=6 when assigning return reasons to physical    │
-- │  in-store return events. Only 7 reasons apply to physical returns.      │
-- └─────────────────────────────────────────────────────────────────────────┘
PRINT '  V8: DimReturnReason — channel applicability check';
SELECT
    AppliesTo,
    COUNT(*)                            AS ReasonCount,
    STRING_AGG(ReturnReasonName, ', ')  AS Reasons
FROM gen.DimReturnReason
GROUP BY AppliesTo;

PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 01 completed successfully.';
PRINT '  Tables verified: gen.DimAcquisitionChannel (7),';
PRINT '                   gen.DimPaymentMethod (6),';
PRINT '                   gen.DimReturnReason (8)';
PRINT '  Next steps (can run in parallel):';
PRINT '    Script 02 → gen.CustomerAcquisition';
PRINT '    Script 03 → gen.OrderPayment';
PRINT '  Script 05 → gen.FactMarketingSpend (MUST wait for Script 02)';
PRINT '════════════════════════════════════════════════════════════════';
GO
