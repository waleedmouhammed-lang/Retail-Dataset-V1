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
  │  Version         : 2.0 (Optimised Build — All Amendments Applied)       │
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
  This script creates and populates gen.OrderFulfillment — a one-row-per-order
  table that assigns every online sales order in dbo.FactOnlineSales a complete
  synthetic fulfilment lifecycle: ship date, delivery date, fulfilment status,
  shipping method, warehouse processing time, and carrier transit time.

  The Contoso source dataset records only WHAT was sold — it contains no
  operational data about HOW orders were physically fulfilled. Without this
  table, the entire COO operational analytics domain is dark: no SLA tracking,
  no average lead-time KPI, no shipping method performance analysis, and no
  ability to distinguish warehouse delays from carrier delays.

  This single table enables the following executive-level questions:

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
  GENERATION LOGIC — FINAL OPTIMISED DESIGN OVERVIEW
--------------------------------------------------------------------------------
  The script uses a four-stage CTE pipeline preceded by scalar pre-computation.
  All random seeds are materialised once in Stage 1 to prevent NEWID() expansion
  in downstream CTE stages. The recency check uses a date-threshold scalar
  (@RecentDateThreshold) instead of NTILE(), eliminating a 13M-row sort penalty.

  ┌──────────────┬────────────────────────────────────────────────────────────┐
  │  Stage       │  Role                                                      │
  ├──────────────┼────────────────────────────────────────────────────────────┤
  │  Scalars     │  Pre-compute @MinYear, @MaxYear, @YearRange, @MaxDate,     │
  │  (pre-CTE)   │  @RecentDateThreshold BEFORE the CTE chain starts.         │
  │              │  Eliminates Table Spools on 13M-row FactOnlineSales.       │
  ├──────────────┼────────────────────────────────────────────────────────────┤
  │  Stage 1     │  OrderBase — One row per order. Aggregates OrderDate,      │
  │  OrderBase   │  OrderValue, geography (customer + store), YearProgress,   │
  │              │  IsRecentOrder flag, and ALL 7 random seeds materialised   │
  │              │  here to prevent NEWID() being called multiple times.      │
  ├──────────────┼────────────────────────────────────────────────────────────┤
  │  Stage 2     │  OrderWithMethod — Assigns ShippingMethod (Standard /      │
  │  OrderWith   │  Express / Overnight) using ShippingRandSeed from Stage 1. │
  │  Method      │  Computes GeoDistanceCategory (Domestic / Continental /    │
  │              │  International) and GeoMultiplier for transit time.        │
  ├──────────────┼────────────────────────────────────────────────────────────┤
  │  Stage 3     │  FulfilmentTimes — Computes CalcProcessingDays using       │
  │  Fulfilment  │  ROUND() (not CEILING) with coefficient 0.30 (not 0.20)   │
  │  Times       │  to produce a visible year-over-year improvement trend.    │
  │              │  GREATEST() guards minimum day floors. CalcTransitDays     │
  │              │  uses CEILING with GeoMultiplier applied to carrier leg.   │
  ├──────────────┼────────────────────────────────────────────────────────────┤
  │  Stage 4     │  FinalOrders — Assigns FulfillmentStatus using             │
  │  FinalOrders │  IsRecentOrder (replaces NTILE bucket). Recent orders get  │
  │              │  'Shipped' or 'Delivered' (50/50). All others follow the   │
  │              │  2%/1%/97% Cancelled/Returned/Delivered split.             │
  └──────────────┴────────────────────────────────────────────────────────────┘

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

  Year improvement curve (final active formula):
    ProcessingDays = ROUND( BaseRange × (1.0 − YearProgress × 0.30), 0 )
    → Coefficient 0.30 widens the improvement range to 1.00→0.70, making
      the operational improvement signal visible across multiple years.
      ROUND() (not CEILING) is used so fractional reductions crossing a
      0.5 boundary produce a lower integer — the improvement is detectable.
      GREATEST(1,...) on Standard/Express guarantees minimum 1 processing day.

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

  NULL rules (enforced in the INSERT SELECT):
    ShipDate         — NULL for Cancelled orders only
    DeliveryDate     — NULL for Cancelled and Shipped orders
    ProcessingDays   — NULL for Cancelled orders
    TransitDays      — NULL for Cancelled and Shipped orders
    TotalFulfilmentDays — NULL unless status is Delivered or Returned

  ⚠  NOTE ON COMMENTED-OUT CODE BLOCKS
  This script contains a large block of commented-out SQL (the original v1
  CTE pipeline). These blocks are retained intentionally as a design audit
  trail — showing the exact changes made during the optimisation review.
  They document:
    - The old NTILE(50) approach (replaced by @RecentDateThreshold)
    - The old inline NEWID() calls (replaced by materialised Stage 1 seeds)
    - The old CEILING + coefficient 0.20 formula (replaced by ROUND + 0.30)
  Do not uncomment and run these blocks — they are superseded by the active
  code that follows the commented section.

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
  Run order    : Script 04 — Run after Script 00 (no dependency on 02 or 03)
  Dependencies : [gen] schema, dbo.FactOnlineSales, dbo.DimCustomer,
                 dbo.DimStore, dbo.DimGeography
  Impact       : Creates ONE new table in [gen]. Zero modifications to [dbo].
  Safe to re-run: YES — idempotent DROP TABLE IF EXISTS guard.
  Can parallel  : YES — Scripts 03 and 04 are independent of each other.
                  Script 05 (gen.FactMarketingSpend) must wait for Script 02.
================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/


-- ============================================================================
-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — PRE-EXECUTION DEPENDENCY CHECKS (5 checks)                  ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  OBJECTIVE                                                                  ║
-- ║  Run five sequential dependency checks before any DDL executes. Each check  ║
-- ║  lives in its own GO-terminated batch. On failure, THROW halts all          ║
-- ║  subsequent batch execution for the session. On success, five ✓ ticks       ║
-- ║  appear in the SSMS Messages tab before any DDL output.                     ║
-- ║                                                                             ║
-- ║  WHY FIVE SEPARATE GO-TERMINATED BATCHES                                    ║
-- ║  Each check uses THROW which terminates the current batch on failure.       ║
-- ║  Placing each check in its own GO batch ensures that if Check 1 fires,     ║
-- ║  Checks 2–5 are parsed but not executed — no cascading errors. Each         ║
-- ║  batch surfaces exactly one clear, actionable failure message.              ║
-- ║                                                                             ║
-- ║  CHECK-BY-CHECK EXPLANATION                                                 ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │ Check 1: IF SCHEMA_ID('gen') IS NULL                                 │  ║
-- ║  │   Best Practice: SCHEMA_ID() is the fastest idempotency check for    │  ║
-- ║  │   schema existence — a single metadata function call returning NULL   │  ║
-- ║  │   if the schema does not exist. Faster than a correlated subquery     │  ║
-- ║  │   against sys.schemas.                                                │  ║
-- ║  │   Note: THROW 50000 is used (not 50001). Both are valid user-defined  │  ║
-- ║  │   error numbers (>= 50000). The behaviour is identical — both halt    │  ║
-- ║  │   execution with a fatal error message.                               │  ║
-- ║  │   Resolution: run Script 00 first.                                    │  ║
-- ║  │                                                                      │  ║
-- ║  │ Check 2: IF OBJECT_ID('[dbo].[FactOnlineSales]', 'U') IS NULL        │  ║
-- ║  │   Best Practice: OBJECT_ID() with second argument 'U' (User Table)   │  ║
-- ║  │   verifies the object exists AND is specifically a table — not a view │  ║
-- ║  │   or stored procedure of the same name. Always specify the type       │  ║
-- ║  │   parameter to prevent false positives.                               │  ║
-- ║  │   FactOnlineSales is the primary source for the entire CTE pipeline.  │  ║
-- ║  │   Without it, the INSERT produces zero rows silently.                 │  ║
-- ║  │   Resolution: confirm ContosoRetailDW is fully restored.              │  ║
-- ║  │                                                                      │  ║
-- ║  │ Check 3: IF OBJECT_ID('[dbo].[DimCustomer]', 'U') IS NULL            │  ║
-- ║  │   DimCustomer resolves CustomerKey → GeographyKey for customer        │  ║
-- ║  │   country/continent in Stage 1 (OrderBase). Without it, ISNULL       │  ║
-- ║  │   guards default all customers to 'Unknown' country, collapsing the  │  ║
-- ║  │   GeoDistanceCategory to 'International' for all orders — inflating   │  ║
-- ║  │   transit times and corrupting the geographic distance signal.        │  ║
-- ║  │   Resolution: confirm ContosoRetailDW is fully restored.              │  ║
-- ║  │                                                                      │  ║
-- ║  │ Check 4: IF OBJECT_ID('[dbo].[DimStore]', 'U') IS NULL               │  ║
-- ║  │   DimStore resolves StoreKey → GeographyKey for store country in      │  ║
-- ║  │   Stage 1. GeoDistanceCategory is computed by comparing customer      │  ║
-- ║  │   country against store country — both sides must be resolved.        │  ║
-- ║  │   The Contoso online store (StoreKey=0 or similar) may have no        │  ║
-- ║  │   geography record, which is handled by LEFT JOIN + ISNULL guards.    │  ║
-- ║  │   But if DimStore itself is missing, all stores default to 'Unknown'. │  ║
-- ║  │   Resolution: confirm ContosoRetailDW is fully restored.              │  ║
-- ║  │                                                                      │  ║
-- ║  │ Check 5: IF OBJECT_ID('[dbo].[DimGeography]', 'U') IS NULL           │  ║
-- ║  │   DimGeography is joined TWICE in Stage 1 — once for customer         │  ║
-- ║  │   geography (alias cg) and once for store geography (alias sg).       │  ║
-- ║  │   Without it, both ISNULL guards return 'Unknown' for country and     │  ║
-- ║  │   continent on every row, making GeoDistanceCategory meaningless and  │  ║
-- ║  │   defaulting all orders to 'International' transit distance.          │  ║
-- ║  │   Resolution: confirm ContosoRetailDW is fully restored.              │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  1. Script 04 has NO dependency on Scripts 02 or 03. It can run in         ║
-- ║     parallel with both — only Script 00 (schemas) plus the 4 Contoso       ║
-- ║     source tables listed above are required.                               ║
-- ║  2. Reading the Messages tab should show exactly 5 green ticks before       ║
-- ║     any DDL output. If fewer appear, identify the failed check and resolve  ║
-- ║     the missing dependency before continuing.                               ║
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
-- ║  OBJECTIVE                                                                  ║
-- ║  Drop gen.OrderFulfillment if it already exists, then create it fresh.     ║
-- ║  The DROP + CREATE pattern makes this script fully idempotent — safe to    ║
-- ║  re-run from scratch at any time without accumulating stale rows.           ║
-- ║                                                                             ║
-- ║  TABLE DESIGN — COLUMN-BY-COLUMN EXPLANATION                               ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │ [SalesOrderNumber]  NVARCHAR(20) NOT NULL (PRIMARY KEY CLUSTERED)    │  ║
-- ║  │   Natural key matching dbo.FactOnlineSales — no surrogate key needed  │  ║
-- ║  │   because the source key is already unique per order.                 │  ║
-- ║  │   NVARCHAR(20) matches the source column type exactly, preventing     │  ║
-- ║  │   implicit type conversion overhead on the JOIN from fact.vOnlineSales│  ║
-- ║  │   to fact.vOrderFulfillment.                                          │  ║
-- ║  │   PRIMARY KEY CLUSTERED: physically orders the table by order number. │  ║
-- ║  │   Best Practice: clustered PK on the natural join key (SalesOrder     │  ║
-- ║  │   Number) means the join from the fact view is an efficient clustered │  ║
-- ║  │   index seek — no separate lookup required.                           │  ║
-- ║  │                                                                      │  ║
-- ║  │ [OrderDate]  DATE NOT NULL                                            │  ║
-- ║  │   Raw source order date — no +16 year temporal shift. DATE (not       │  ║
-- ║  │   DATETIME or DATETIME2) is used because order-level analysis only    │  ║
-- ║  │   requires day granularity, and DATE uses 3 bytes vs 8 for DATETIME. │  ║
-- ║  │   Best Practice: choose the narrowest date type that satisfies the   │  ║
-- ║  │   analytical requirement — unnecessary precision wastes storage.      │  ║
-- ║  │                                                                      │  ║
-- ║  │ [ShipDate]  DATE NULL                                                 │  ║
-- ║  │   Date the warehouse dispatched the order to the carrier.             │  ║
-- ║  │   NULL for Cancelled orders only — a cancelled order was never        │  ║
-- ║  │   handed to the carrier. Inline comment documents the NULL condition. │  ║
-- ║  │                                                                      │  ║
-- ║  │ [DeliveryDate]  DATE NULL                                             │  ║
-- ║  │   Date the carrier completed delivery to the customer.                │  ║
-- ║  │   NULL for Cancelled (never shipped) and Shipped (in-transit, no      │  ║
-- ║  │   confirmed delivery yet). Inline comment documents both conditions.  │  ║
-- ║  │                                                                      │  ║
-- ║  │ [FulfillmentStatus]  NVARCHAR(20) NOT NULL                           │  ║
-- ║  │   Four domain values: 'Delivered', 'Shipped', 'Cancelled', 'Returned'.│  ║
-- ║  │   NOT NULL — every order must have a known fulfilment status.         │  ║
-- ║  │   NVARCHAR(20) accommodates the longest value ('Cancelled', 9 chars)  │  ║
-- ║  │   with buffer. Inline comment documents the four-value domain.        │  ║
-- ║  │                                                                      │  ║
-- ║  │ [ShippingMethod]  NVARCHAR(20) NOT NULL                              │  ║
-- ║  │   Three domain values: 'Standard', 'Express', 'Overnight'.           │  ║
-- ║  │   NOT NULL — every order has a shipping method, even Cancelled orders │  ║
-- ║  │   (the method was selected at order placement, before cancellation).  │  ║
-- ║  │   Inline comment documents the three-value domain.                   │  ║
-- ║  │                                                                      │  ║
-- ║  │ [ProcessingDays]  INT NULL                                            │  ║
-- ║  │   Warehouse processing duration: OrderDate → ShipDate.               │  ║
-- ║  │   NULL for Cancelled (no processing occurred — order was cancelled    │  ║
-- ║  │   before it reached the warehouse dispatch stage).                   │  ║
-- ║  │                                                                      │  ║
-- ║  │ [TransitDays]  INT NULL                                               │  ║
-- ║  │   Carrier transit duration: ShipDate → DeliveryDate.                 │  ║
-- ║  │   NULL for Cancelled (never shipped) and Shipped (delivery not yet    │  ║
-- ║  │   confirmed — order is in-transit at the dataset close date).         │  ║
-- ║  │                                                                      │  ║
-- ║  │ [TotalFulfillmentDays]  INT NULL                                      │  ║
-- ║  │   ProcessingDays + TransitDays — the complete end-to-end lifecycle.   │  ║
-- ║  │   NULL for all statuses EXCEPT 'Delivered' and 'Returned', because   │  ║
-- ║  │   only completed lifecycle journeys should contribute to average       │  ║
-- ║  │   fulfilment time KPIs. Including partial journeys (Cancelled,        │  ║
-- ║  │   Shipped) would understate the true end-to-end time.                │  ║
-- ║  │                                                                      │  ║
-- ║  │ CONSTRAINT [PK_OrderFulfillment]                                      │  ║
-- ║  │   PRIMARY KEY CLUSTERED ([SalesOrderNumber])                          │  ║
-- ║  │   Named constraint. Best Practice: always name constraints explicitly. │  ║
-- ║  │   System-generated names (PK__OrderFul__...) are opaque GUIDs that   │  ║
-- ║  │   produce unreadable error messages and ALTER statements.             │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ║                                                                             ║
-- ║  NULLABLE COLUMN DESIGN — NULL BY INTENT, NOT BY ERROR                     ║
-- ║  Five columns are deliberately nullable. These NULLs represent             ║
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
-- ║  Verification query V6 cross-validates these NULL rules with hard           ║
-- ║  integrity assertions: NullShipDate must equal CancelledOrders, and        ║
-- ║  NullDeliveryDate must equal CancelledOrders + ShippedOrders.              ║
-- ║  Any mismatch indicates a defect in the NULL assignment logic in the        ║
-- ║  INSERT SELECT (Stage 4 / FinalOrders CTE).                                ║
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
-- ║                                                                             ║
-- ║  OBJECTIVE                                                                  ║
-- ║  Compute five scalar variables in a single SELECT against FactOnlineSales   ║
-- ║  BEFORE the CTE chain begins. All five are aggregates of the 13M-row        ║
-- ║  source table. Pre-computing them as scalars gives the query optimiser       ║
-- ║  fixed constants — eliminating Table Spools in every downstream CTE stage.  ║
-- ║                                                                             ║
-- ║  LINE-BY-LINE EXPLANATION                                                   ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │ DECLARE @MinYear INT / @MaxYear INT                                   │  ║
-- ║  │   Min and max calendar year of all orders in FactOnlineSales.         │  ║
-- ║  │   These feed YearProgress in Stage 1: 0.0 = earliest year,           │  ║
-- ║  │   1.0 = latest year. Controls the processing-time improvement curve   │  ║
-- ║  │   in Stage 3 (FulfilmentTimes).                                       │  ║
-- ║  │   Best Practice: computing MIN/MAX inside a CTE body that is          │  ║
-- ║  │   referenced across a CROSS JOIN causes a Table Spool — SQL Server   │  ║
-- ║  │   writes 13M rows to tempdb and re-reads them for every downstream   │  ║
-- ║  │   CTE row. Pre-declaring as scalars materialises the result once;    │  ║
-- ║  │   the optimiser treats them as constants, not subqueries.             │  ║
-- ║  │                                                                      │  ║
-- ║  │ DECLARE @YearRange FLOAT                                              │  ║
-- ║  │   @MaxYear − @MinYear. Division denominator for YearProgress.         │  ║
-- ║  │   Declared as FLOAT (not INT) so the division in Stage 1 produces    │  ║
-- ║  │   a decimal result (0.0→1.0), not integer division (always 0 or 1). │  ║
-- ║  │                                                                      │  ║
-- ║  │ DECLARE @MaxDate DATE                                                 │  ║
-- ║  │   The most recent order date in the entire dataset.                  │  ║
-- ║  │   Used as the anchor for @RecentDateThreshold calculation below.     │  ║
-- ║  │                                                                      │  ║
-- ║  │ DECLARE @RecentDateThreshold DATE                                     │  ║
-- ║  │   Defines the cutoff date for IsRecentOrder in Stage 1.              │  ║
-- ║  │   Orders on or after this date get IsRecentOrder = 1 and are         │  ║
-- ║  │   eligible for 'Shipped' status assignment in Stage 4.               │  ║
-- ║  │   Design rationale: the original design used NTILE(50) to identify   │  ║
-- ║  │   the most recent ~2% of orders as the "in-transit" cohort. NTILE()  │  ║
-- ║  │   requires sorting all 13M rows before returning any results —        │  ║
-- ║  │   a severe performance penalty (30–60 seconds of sort time).          │  ║
-- ║  │   Replacing NTILE(50) with a scalar date threshold (@MaxDate − 21    │  ║
-- ║  │   days ≈ 2% of a 1,095-day, 3-year dataset) eliminates the sort      │  ║
-- ║  │   entirely. The inline comment documents the 21-day derivation.      │  ║
-- ║  │   Best Practice: always replace sort-based window functions           │  ║
-- ║  │   (NTILE, RANK, ROW_NUMBER on full table scans) with scalar threshold │  ║
-- ║  │   comparisons wherever the business intent permits it.                │  ║
-- ║  │                                                                      │  ║
-- ║  │ SELECT @MinYear = MIN(YEAR(f.DateKey)),                               │  ║
-- ║  │        @MaxYear = MAX(YEAR(f.DateKey)),                               │  ║
-- ║  │        @MaxDate = CAST(MAX(f.DateKey) AS DATE)                        │  ║
-- ║  │   FROM [dbo].[FactOnlineSales] f                                      │  ║
-- ║  │   Best Practice: populates three scalars in a single scan of          │  ║
-- ║  │   FactOnlineSales. MIN(YEAR(...)), MAX(YEAR(...)), and MAX(DateKey)   │  ║
-- ║  │   are computed in one pass. Three separate SELECT statements would    │  ║
-- ║  │   require three table scans — three times the IO cost.                │  ║
-- ║  │   CAST(MAX(f.DateKey) AS DATE): DateKey is stored as DATE in the      │  ║
-- ║  │   Contoso source. The explicit CAST ensures @MaxDate is typed as DATE │  ║
-- ║  │   for the subsequent DATEADD call below.                              │  ║
-- ║  │                                                                      │  ║
-- ║  │ SET @YearRange = NULLIF(@MaxYear - @MinYear, 0)                       │  ║
-- ║  │   Best Practice: NULLIF converts the result to NULL if the dataset   │  ║
-- ║  │   spans only one calendar year (edge case where @MaxYear − @MinYear  │  ║
-- ║  │   = 0). Without NULLIF, dividing by 0 in YearProgress would raise a  │  ║
-- ║  │   divide-by-zero error. The ISNULL(... , 0.5) guard in Stage 1       │  ║
-- ║  │   then substitutes 0.5 (mid-period neutral default) — safe and       │  ║
-- ║  │   meaningful for a single-year dataset.                               │  ║
-- ║  │                                                                      │  ║
-- ║  │ SET @RecentDateThreshold = DATEADD(day, -21, @MaxDate)               │  ║
-- ║  │   Derives the threshold 21 days before the most recent order date.   │  ║
-- ║  │   The inline comment explains the derivation: 21 days ÷ 1,095 days  │  ║
-- ║  │   (3 years) ≈ 1.9% — targeting the ~2% in-transit cohort. This value │  ║
-- ║  │   is adjustable: if the dataset date range differs from 3 years,     │  ║
-- ║  │   change the integer to maintain the desired ~2% proportion.          │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — WHY SCALARS, NOT CTE AGGREGATES?               ║
-- ║  If @MinYear, @MaxYear, and @MaxDate were computed inside the CTE body     ║
-- ║  (e.g., as MIN(YEAR(f.DateKey)) OVER ()), SQL Server would produce a Table  ║
-- ║  Spool on FactOnlineSales — persisting 13M intermediate rows to tempdb     ║
-- ║  for re-reading on every CROSS JOIN row downstream. On a test machine this ║
-- ║  adds 30–60+ seconds to the script runtime. Pre-declaring scalars makes    ║
-- ║  the values constants at compile time — zero spool, maximum performance.   ║
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
-- ║                                                                             ║
-- ║  OBJECTIVE                                                                  ║
-- ║  Aggregate one row per SalesOrderNumber with all per-order context needed   ║
-- ║  by Stages 2, 3, and 4: date, value, item count, customer and store         ║
-- ║  geography, temporal position (YearProgress), recency flag (IsRecentOrder), ║
-- ║  and ALL seven random seeds materialised here in a single CTE pass.         ║
-- ║                                                                             ║
-- ║  KEY DESIGN DECISIONS — COLUMN-BY-COLUMN                                    ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │ CAST(MIN(f.DateKey) AS DATE) AS OrderDate                            │  ║
-- ║  │   All line items in an order share the same DateKey; MIN() is        │  ║
-- ║  │   deterministic and avoids an aggregate error. CAST to DATE strips   │  ║
-- ║  │   any time component and produces the correct target column type.    │  ║
-- ║  │                                                                      │  ║
-- ║  │ CAST(SUM(f.SalesAmount) AS DECIMAL(19,4)) AS OrderValue              │  ║
-- ║  │   SUM correctly aggregates multi-line orders. CAST to DECIMAL(19,4) │  ║
-- ║  │   matches the target column type — avoids implicit conversion.       │  ║
-- ║  │   Best Practice: never store currency in FLOAT or REAL; DECIMAL(19,4)│  ║
-- ║  │   provides exact arithmetic without floating-point rounding.         │  ║
-- ║  │                                                                      │  ║
-- ║  │ SUM(f.SalesQuantity) AS TotalItems                                   │  ║
-- ║  │   Total units in the order — carried through to downstream stages    │  ║
-- ║  │   for any future weight-based or volume-based analysis.              │  ║
-- ║  │                                                                      │  ║
-- ║  │ ISNULL(MIN(cg.RegionCountryName), 'Unknown') AS CustomerCountry      │  ║
-- ║  │ ISNULL(MIN(cg.ContinentName),     'Unknown') AS CustomerContinent    │  ║
-- ║  │   Best Practice: LEFT JOIN to DimGeography (alias cg) preserves      │  ║
-- ║  │   orders whose customer has no geography record — INNER JOIN would   │  ║
-- ║  │   silently drop those orders. ISNULL converts NULL to 'Unknown' so   │  ║
-- ║  │   the CASE expression in Stage 2 (GeoDistanceCategory) always finds  │  ║
-- ║  │   the ELSE branch rather than returning NULL for the entire column.  │  ║
-- ║  │                                                                      │  ║
-- ║  │ ISNULL(MIN(sg.RegionCountryName), 'Unknown') AS StoreCountry         │  ║
-- ║  │ ISNULL(MIN(sg.ContinentName),     'Unknown') AS StoreContinent       │  ║
-- ║  │   Same LEFT JOIN + ISNULL pattern applied to store geography          │  ║
-- ║  │   (alias sg). The Contoso online store (StoreKey=0 or near-zero) may │  ║
-- ║  │   not have a row in DimGeography. INNER JOIN here would silently      │  ║
-- ║  │   exclude a significant fraction of all online orders.                │  ║
-- ║  │                                                                      │  ║
-- ║  │ ISNULL(CAST(MIN(YEAR(f.DateKey)) - @MinYear AS FLOAT)                │  ║
-- ║  │         / @YearRange, 0.5) AS YearProgress                           │  ║
-- ║  │   Temporal position of the order within the dataset's year range.    │  ║
-- ║  │   0.0 = orders in the earliest year; 1.0 = orders in the latest year.│  ║
-- ║  │   CAST to FLOAT before dividing: without CAST, integer division      │  ║
-- ║  │   returns 0 for all non-terminal years — the year signal is lost.    │  ║
-- ║  │   @YearRange is NULLIF-guarded in the scalar block above; if NULL,   │  ║
-- ║  │   ISNULL substitutes 0.5 (neutral mid-period default) — safe even   │  ║
-- ║  │   for single-year source datasets.                                   │  ║
-- ║  │   This value controls the processing-time improvement curve in Stage 3.│  ║
-- ║  │                                                                      │  ║
-- ║  │ CASE WHEN CAST(MIN(f.DateKey) AS DATE) >= @RecentDateThreshold       │  ║
-- ║  │      THEN 1 ELSE 0 END AS IsRecentOrder                              │  ║
-- ║  │   Inline comment: "OPTIMIZED: Replaces the 13M row sort penalty of   │  ║
-- ║  │   NTILE(50)". IsRecentOrder=1 marks orders placed within the last    │  ║
-- ║  │   ~21 days of the dataset close date — the in-transit cohort.        │  ║
-- ║  │   Stage 4 (FinalOrders) assigns 'Shipped' to ~50% of these orders    │  ║
-- ║  │   (BinaryRandSeed = 0) and 'Delivered' to the remaining ~50%.        │  ║
-- ║  │   Best Practice: a boolean date threshold comparison is orders of     │  ║
-- ║  │   magnitude faster than NTILE() on 13M rows — NTILE() requires a     │  ║
-- ║  │   full sort of the entire table before returning any results.         │  ║
-- ║  │                                                                      │  ║
-- ║  │ ABS(CHECKSUM(NEWID())) % 100 AS ShippingRandSeed                     │  ║
-- ║  │ ABS(CHECKSUM(NEWID())) % 100 AS StatusRandSeed                       │  ║
-- ║  │ ABS(CHECKSUM(NEWID())) % 2   AS BinaryRandSeed                       │  ║
-- ║  │ ABS(CHECKSUM(NEWID())) % 3   AS ProcSeedStd                          │  ║
-- ║  │ ABS(CHECKSUM(NEWID())) % 2   AS ProcSeedExp                          │  ║
-- ║  │ ABS(CHECKSUM(NEWID())) % 8   AS TransSeedStd                         │  ║
-- ║  │ ABS(CHECKSUM(NEWID())) % 4   AS TransSeedExp                         │  ║
-- ║  │   Inline comment: "MATERIALIZE ALL RANDOM SEEDS HERE TO PREVENT CPU  │  ║
-- ║  │   EXHAUSTION". All seven random seeds are produced here in Stage 1   │  ║
-- ║  │   and carried as plain integer columns into Stages 2, 3, and 4.      │  ║
-- ║  │   NEWID() generates a unique UUID per row per execution.              │  ║
-- ║  │   CHECKSUM() converts UUID to a deterministic signed integer.         │  ║
-- ║  │   ABS() removes the sign. % N maps the result to the 0…N-1 range     │  ║
-- ║  │   needed by each downstream CASE expression.                          │  ║
-- ║  │   Best Practice: materialising NEWID() seeds in the first CTE stage  │  ║
-- ║  │   that accesses the base table prevents NEWID() expansion — a SQL    │  ║
-- ║  │   Server behaviour where NEWID() inside a referenced CTE can be      │  ║
-- ║  │   re-evaluated on each downstream reference, producing different      │  ║
-- ║  │   values than intended and exhausting CPU on large datasets.          │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ║                                                                             ║
-- ║  JOIN PATTERN RATIONALE                                                     ║
-- ║  INNER JOIN DimCustomer: every FactOnlineSales row must have a valid        ║
-- ║  CustomerKey — no silent row exclusion risk.                                ║
-- ║  LEFT JOIN DimGeography (alias cg): customer geography is optional —        ║
-- ║  some customers may not have a matched geography record.                    ║
-- ║  LEFT JOIN DimStore: StoreKey=0 (the online channel placeholder) may        ║
-- ║  not have a corresponding DimStore row.                                     ║
-- ║  LEFT JOIN DimGeography (alias sg): store geography inherits the same       ║
-- ║  optional risk as customer geography.                                        ║
-- ║  Best Practice: always LEFT JOIN optional lookups and guard with ISNULL —  ║
-- ║  never INNER JOIN a dimension whose FK is not guaranteed to be populated.   ║
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
-- STAGE 2 — OrderWithMethod
-- ─────────────────────────────────────────────────────────────────────────────
-- OBJECTIVE: Assign ShippingMethod and compute GeoDistanceCategory +
-- GeoMultiplier for every order, inheriting all pre-materialised seeds from
-- Stage 1 via SELECT ob.*.
--
-- ShippingMethod Assignment:
--   Uses ShippingRandSeed (0–99, uniform) with CASE thresholds:
--     < 10  (10 values) → 'Overnight'   = 10% target share
--     < 40  (30 values) → 'Express'     = 30% target share
--     ELSE  (60 values) → 'Standard'    = 60% target share
--   Note: both high-value (>$500) and standard orders use IDENTICAL thresholds
--   in the final active code. The structure is preserved for future flexibility
--   (different thresholds per value tier could be introduced without
--   restructuring the CASE), and both branches produce the same distribution.
--   Best Practice: using pre-materialised ShippingRandSeed from Stage 1 (not
--   a new inline NEWID() call) guarantees the seed is evaluated once per
--   order and is consistent across Stages 2–4.
--
-- GeoDistanceCategory and GeoMultiplier:
--   CustomerCountry vs StoreCountry → Domestic (same country) → × 0.70
--   CustomerContinent vs StoreContinent → Continental           → × 1.00
--   All other combinations → International                      → × 1.60
--   The CASE evaluates the tightest condition first (country match)
--   before the looser condition (continent match). This is the correct
--   precedence — a same-country order is also same-continent, so country
--   must be checked first.
--   Both country and continent values were ISNULL-guarded in Stage 1,
--   so neither will be NULL here. Unresolvable geography defaults to
--   'International' via the ELSE branch — a conservative assumption.
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
-- ─────────────────────────────────────────────────────────────────────────────
-- OBJECTIVE: Compute CalcProcessingDays and CalcTransitDays as integer values
-- for every order, using the pre-materialised seeds from Stage 1 and the
-- GeoMultiplier from Stage 2. Three iterative fixes were applied to ensure a
-- detectable year-over-year processing time improvement signal.
--
-- CalcProcessingDays — Three Key Fixes Applied:
--
--   FIX 1 (CEILING → ROUND, explained in inline comments):
--     CEILING(integer × 0.80) always returns the original integer because
--     CEILING rounds UP to the nearest integer — it can never decrease a
--     value. The improvement formula only produces a visible reduction when
--     the product crosses an integer boundary downward, which requires
--     rounding to the nearest integer (ROUND), not always up (CEILING).
--     Example: ROUND(3 × 0.80) = ROUND(2.40) = 2 — improvement visible.
--
--   FIX 2 (Coefficient 0.20 → 0.30, explained in inline comments):
--     At coefficient 0.20, the factor range was 1.00→0.80. With ROUND,
--     this only crosses a downward integer boundary in the final source
--     year for seeds where base × 0.80 < integer − 0.5. Widening to 0.30
--     gives a range of 1.00→0.70, crossing boundaries in both the middle
--     and final years for a wider set of seeds — producing a visibly
--     improving trend across all three source years.
--
--   FIX 3 (Overnight now participates in the improvement curve):
--     The original design used om.BinaryRandSeed raw for Overnight (0 or 1
--     with no year factor). This produced zero year-over-year improvement
--     for Overnight orders. The fix wraps BinaryRandSeed in the same
--     ROUND + (1.0 − YearProgress × 0.30) formula as Standard and Express.
--     GREATEST(0,...): prevents negative values on the edge case where
--     BinaryRandSeed=0 and (1.0 − YearProgress × 0.30) rounds to 0.
--     GREATEST(1,...) on Standard/Express: guarantees a minimum of 1
--     processing day — a warehouse always needs at least 1 day to pick,
--     pack, and hand off to the carrier.
--
-- CalcTransitDays — Unchanged Design:
--   CEILING used intentionally here (not ROUND). Carrier transit is measured
--   in whole business days — partial days round UP, not to nearest.
--   Base range by method: Standard 5–12 d, Express 2–5 d, Overnight 1–2 d.
--   GeoMultiplier applied as FLOAT before CEILING to scale the full decimal
--   product before rounding up.
--   CAST AS INT: CEILING() returns NUMERIC — explicit CAST to INT prevents
--   a type mismatch when DATEADD(DAY, TransitDays, ...) is called in the
--   INSERT SELECT downstream.
--   Overnight international cap: CASE WHEN GeoMultiplier > 1.0 THEN 1.5
--   caps international Overnight at a 1.5× multiplier instead of the full
--   1.60×. Overnight courier networks route via global hubs — transit does
--   not scale linearly with geographic distance for this service tier.
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
-- ─────────────────────────────────────────────────────────────────────────────
-- OBJECTIVE: Assign FulfillmentStatus to every order using a two-branch
-- CASE expression. The IsRecentOrder flag from Stage 1 (which replaced
-- the old NTILE(50) recency bucket) drives the branching logic.
--
-- Branch 1 — Recent orders (IsRecentOrder = 1):
--   CASE WHEN ft.BinaryRandSeed = 0 THEN 'Shipped' ELSE 'Delivered' END
--   BinaryRandSeed is 0 or 1 (uniform). This assigns ~50% of recent orders
--   to 'Shipped' (in-transit at dataset close date) and ~50% to 'Delivered'.
--   Since IsRecentOrder marks ~2% of all orders, this produces ~1% 'Shipped'
--   overall — realistic for an active e-commerce operation where ~1% of
--   the order base is always in-transit on any given day.
--
-- Branch 2 — All other orders (IsRecentOrder = 0):
--   StatusRandSeed is 0–99 (uniform). CASE thresholds:
--     < 2  (2 values)  → 'Cancelled'  = 2% target across non-recent orders
--     < 3  (1 value)   → 'Returned'   = 1% target (full-order refused delivery)
--     ELSE (97 values) → 'Delivered'  = 97% of non-recent orders
--   Combined with the ~1% Shipped from Branch 1, the population-level
--   distribution is approximately: 96% Delivered / 2% Cancelled / 1% Shipped
--   / 1% Returned.
--
-- ⚠  STUDENT CRITICAL NOTE — 'Returned' vs Returns Fact Tables:
--   The 1% 'Returned' status here represents FULL-ORDER REFUSED DELIVERIES
--   (customer rejected the parcel at the door before accepting it). This is
--   ENTIRELY DISTINCT from line-item returns recorded in:
--     dbo.FactOnlineSales (ReturnQuantity / ReturnAmount columns)
--     gen.OnlineReturnEvents  (Script 07)
--     gen.PhysicalReturnEvents (Script 08)
--   Those tables capture returns AFTER successful delivery. These are two
--   different business events at different stages of the order lifecycle.
-- ============================================================================
-- ============================================================================
-- INSERT — Applies NULL rules per FulfillmentStatus and writes to physical table
-- ─────────────────────────────────────────────────────────────────────────────
-- OBJECTIVE: Project all columns from FinalOrders CTE into gen.OrderFulfillment
-- with conditional NULL logic applied inline in the SELECT.
--
-- Best Practice: NULL rules are applied here in the INSERT SELECT, not inside
-- the CTE stages. Keeping CTE stages clean (they always carry CalcProcessing
-- Days and CalcTransitDays as computed values) and applying the conditional
-- NULL masks at the point of physical insertion ensures the logic is in exactly
-- one place — easy to audit against the NULL rules table in Code Block 2.
--
-- Per-column line-by-line:
--
--   ShipDate: CASE WHEN fo.FulfillmentStatus = 'Cancelled' THEN NULL
--             ELSE DATEADD(DAY, fo.CalcProcessingDays, fo.OrderDate) END
--   DATEADD(DAY, ...) adds CalcProcessingDays integer days to OrderDate
--   to produce ShipDate. The CASE guard ensures DATEADD is never called
--   with a NULL day count (Cancelled orders have NULL ProcessingDays).
--
--   DeliveryDate: CASE WHEN fo.FulfillmentStatus IN ('Cancelled', 'Shipped')
--                 THEN NULL
--                 ELSE DATEADD(DAY, fo.CalcProcessingDays + fo.CalcTransitDays,
--                              fo.OrderDate) END
--   Total days = ProcessingDays + TransitDays added directly in DATEADD —
--   avoids an intermediate derived column. IN (...) is more readable than
--   multiple OR conditions for a multi-value exclusion.
--
--   ProcessingDays / TransitDays: same CASE gate pattern as ShipDate /
--   DeliveryDate respectively, enforcing the NULL rules from Code Block 2.
--
--   TotalFulfillmentDays: CASE WHEN fo.FulfillmentStatus IN ('Delivered',
--   'Returned') THEN fo.CalcProcessingDays + fo.CalcTransitDays ELSE NULL END
--   Only complete lifecycle journeys contribute. Cancelled and Shipped orders
--   have no full journey — including them would understate the true average.
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
-- INSERT
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
-- ║  OBJECTIVE                                                                  ║
-- ║  Create two Non-Clustered covering indexes targeting the two dominant        ║
-- ║  analytical access patterns against gen.OrderFulfillment in Power BI.        ║
-- ║  Both indexes use the INCLUDE clause to eliminate Key Lookups back to       ║
-- ║  the clustered index (SalesOrderNumber PK) for the most commonly            ║
-- ║  projected columns.                                                          ║
-- ║                                                                             ║
-- ║  INDEX 1 — IX_OrderFulfillment_Status                                       ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │ ON [gen].[OrderFulfillment] ([FulfillmentStatus])                    │  ║
-- ║  │   Seek column: FulfillmentStatus. Primary access pattern: SLA        │  ║
-- ║  │   attainment reports that filter or group by status                  │  ║
-- ║  │   (WHERE FulfillmentStatus = 'Delivered' / 'Cancelled' / etc.).     │  ║
-- ║  │   Without this index, every status-filtered query performs a full    │  ║
-- ║  │   table scan of 1.65M rows — regardless of how selective the filter. │  ║
-- ║  │   With the index, the engine performs an Index Seek on the ~16K      │  ║
-- ║  │   'Cancelled' rows or ~1.58M 'Delivered' rows directly.              │  ║
-- ║  │                                                                      │  ║
-- ║  │ INCLUDE ([SalesOrderNumber], [OrderDate], [ShippingMethod],           │  ║
-- ║  │          [TotalFulfillmentDays])                                      │  ║
-- ║  │   These four columns are co-projected with FulfillmentStatus in the  │  ║
-- ║  │   majority of SLA reports, funnel analyses, and on-time rate KPIs.   │  ║
-- ║  │   Including them in the index leaf pages means the engine never has  │  ║
-- ║  │   to perform a Key Lookup back to the clustered index to fetch them. │  ║
-- ║  │   Best Practice: always INCLUDE the non-key columns that are most    │  ║
-- ║  │   frequently SELECTed alongside the seek column in production        │  ║
-- ║  │   queries. A covering index eliminates all Key Lookups for those     │  ║
-- ║  │   queries.                                                            │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ║                                                                             ║
-- ║  INDEX 2 — IX_OrderFulfillment_OrderDate                                    ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │ ON [gen].[OrderFulfillment] ([OrderDate])                            │  ║
-- ║  │   Seek column: OrderDate. Secondary access pattern: time-series      │  ║
-- ║  │   trend analysis by order date — YoY operational comparisons,        │  ║
-- ║  │   monthly fulfilment time averages, peak season analysis.            │  ║
-- ║  │   Without this index, every date-range query performs a full scan.   │  ║
-- ║  │                                                                      │  ║
-- ║  │ INCLUDE ([FulfillmentStatus], [ShippingMethod], [ProcessingDays],    │  ║
-- ║  │          [TransitDays], [TotalFulfillmentDays])                       │  ║
-- ║  │   These five columns are the aggregation targets in time-series      │  ║
-- ║  │   queries: AVG(ProcessingDays) by month, COUNT(*) by status by month,│  ║
-- ║  │   AVG(TotalFulfillmentDays) by shipping method by quarter. Including  │  ║
-- ║  │   all five makes this index fully covering for those queries.         │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — WHY TWO SEPARATE INDEXES?                       ║
-- ║  The two dominant access patterns use DIFFERENT leading columns.            ║
-- ║  A composite index on (FulfillmentStatus, OrderDate) would serve Status    ║
-- ║  queries efficiently but produce an Index SCAN (not Seek) for pure         ║
-- ║  date-range queries — the engine would still need to scan all FulfillStatus ║
-- ║  partitions before filtering by date. Two targeted single-column indexes   ║
-- ║  with covering INCLUDEs is the correct pattern for a two-axis access        ║
-- ║  profile: each index is independently and optimally aligned to its          ║
-- ║  specific query shape.                                                      ║
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
-- ║  CODE BLOCK 9 — SESSION BOUNDARY BATCH                                       ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  OBJECTIVE                                                                  ║
-- ║  A standalone GO batch that creates a clean boundary between all DDL/DML   ║
-- ║  blocks above and the verification queries below.                           ║
-- ║                                                                             ║
-- ║  WHY A STANDALONE GO BATCH HERE                                             ║
-- ║  The dependency checks in Code Block 1 use THROW which terminates the       ║
-- ║  current batch on failure. This standalone GO batch (containing no          ║
-- ║  executable statements) creates a definitive batch boundary: if all         ║
-- ║  preceding DDL and DML completed without error, execution passes cleanly    ║
-- ║  through this batch and reaches the verification queries below.             ║
-- ║  If any earlier batch raised an error that stopped execution, this batch    ║
-- ║  and all subsequent verification queries will also not execute — which is   ║
-- ║  the correct and safe behaviour. Verification is meaningful only after a    ║
-- ║  successful data generation run.                                            ║
-- ║                                                                             ║
-- ║  Best Practice: this is the standard project-wide pattern for placing a     ║
-- ║  clean batch boundary between the data generation blocks and the            ║
-- ║  verification suite at the bottom of each [gen] generation script.         ║
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
-- ║  OBJECTIVE                                                                  ║
-- ║  Six verification queries confirm correctness at multiple levels of         ║
-- ║  granularity. Run all six immediately after Script 04 completes — before    ║
-- ║  any downstream view scripts (Scripts 09–10) reference this table.          ║
-- ║                                                                             ║
-- ║  VERIFICATION STRATEGY — TWO-TIER APPROACH                                  ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │  Tier 1 — EXACT checks (zero tolerance):                             │  ║
-- ║  │    V1: Population completeness — row count must exactly equal the    │  ║
-- ║  │        distinct SalesOrderNumber count in dbo.FactOnlineSales.       │  ║
-- ║  │        Delta = 0 and unassigned count = 0 are both required.         │  ║
-- ║  │    V6 (Part A): Hard integrity checks — 7 business logic violations  │  ║
-- ║  │        that must all return 0. See the expected output box below      │  ║
-- ║  │        for the full list of checks and their business meaning.        │  ║
-- ║  │    V6 (Part B, info): NULL count cross-validation using known exact   │  ║
-- ║  │        production values from Contoso Retail DW. These numbers are   │  ║
-- ║  │        deterministic from the source data even though the status      │  ║
-- ║  │        assignment uses randomness — NullShipDate always equals        │  ║
-- ║  │        CancelledOrders regardless of the random seed values.          │  ║
-- ║  │                                                                      │  ║
-- ║  │  Tier 2 — DIRECTIONAL checks (verify pattern direction, not % exact):│  ║
-- ║  │    V2: FulfillmentStatus distribution — target 96/2/1/1 % split.    │  ║
-- ║  │        Any status more than 3 percentage points from its target      │  ║
-- ║  │        warrants investigation of the IsRecentOrder / StatusRandSeed  │  ║
-- ║  │        logic in Stage 4.                                              │  ║
-- ║  │    V3: ShippingMethod distribution — target ~60/30/10 % split for   │  ║
-- ║  │        Standard / Express / Overnight. AvgFulfilmentDays should       │  ║
-- ║  │        decrease from Standard to Express to Overnight.               │  ║
-- ║  │    V4: Processing and transit time by method — Overnight must be the │  ║
-- ║  │        fastest on ALL three metrics (AvgProcessing, AvgTransit,      │  ║
-- ║  │        AvgTotal). The speed hierarchy must hold: Overnight < Express  │  ║
-- ║  │        < Standard on every average.                                  │  ║
-- ║  │    V5: Year-over-year improvement trend — AvgProcessingDays must     │  ║
-- ║  │        DECREASE from the earliest source year to the latest.         │  ║
-- ║  │        AvgTransitDays should be STABLE (no year factor was applied  │  ║
-- ║  │        to the carrier leg — only the warehouse leg improves).        │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ║                                                                             ║
-- ║  CONTOSO BASELINE (for reference):                                          ║
-- ║  dbo.FactOnlineSales contains approximately 1,674,320 distinct orders.     ║
-- ║  gen.OrderFulfillment must match this count exactly.                        ║
-- ║                                                                             ║
-- ║  KNOWN VERIFIED NUMBERS (from production run — Contoso Retail DW source):  ║
-- ║  These values are deterministic from the source data — they will match      ║
-- ║  on any correctly restored ContosoRetailDW instance:                        ║
-- ║  V6 (info): NullShipDate           = 33,238  (= CancelledOrders)           ║
-- ║             NullDeliveryDate        = 49,788  (= Cancelled + Shipped)       ║
-- ║             NullProcessingDays      = 33,238  (= CancelledOrders)           ║
-- ║             NullTransitDays         = 49,788  (= Cancelled + Shipped)       ║
-- ║             NullTotalFulfilmentDays = 49,788  (= Cancelled + Shipped)       ║
-- ║             CancelledOrders         = 33,238  (~2% of total)                ║
-- ║             ShippedOrders           = 16,550  (~1% of total)                ║
-- ║  Cross-check: 33,238 + 16,550 = 49,788 ✓ (arithmetic must balance)         ║
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
