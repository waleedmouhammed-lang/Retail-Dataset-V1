/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT        ║
║                        SCRIPT 00: SCHEMA INITIALISATION                      ║
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
  This script creates the three analytical schemas that form the backbone of
  the entire project architecture. It is the FIRST script that must be
  executed on any fresh ContosoRetailDW instance before any tables, views,
  or data generation scripts can run.

  It is intentionally kept isolated from all other scripts. It creates
  NOTHING except schemas — no tables, no views, no data. Its sole job is
  to ensure the three schema namespaces exist so that all downstream
  scripts can reference them safely.

  This script is fully IDEMPOTENT — it can be re-run on an existing instance
  without error and without destroying any objects that already exist inside
  the schemas.

--------------------------------------------------------------------------------
  THE THREE-LAYER ARCHITECTURE
--------------------------------------------------------------------------------

  The project operates on a strict three-layer separation of concerns:

  ┌──────────────┬─────────────────────────────────────────────────────────┐
  │  Schema      │  Role & Philosophy                                      │
  ├──────────────┼─────────────────────────────────────────────────────────┤
  │  [dbo]       │  SOURCE LAYER — The original Contoso Retail DW tables   │
  │              │  shipped by Microsoft. This layer is READ-ONLY and is   │
  │              │  NEVER modified under any circumstance. It represents   │
  │              │  the ground truth of the source system.                 │
  ├──────────────┼─────────────────────────────────────────────────────────┤
  │  [gen]       │  SYNTHETIC EXTENSION LAYER — Tables created to fill     │
  │              │  analytical gaps in the Contoso source that prevent     │
  │              │  answering executive business questions. This schema    │
  │              │  extends the source without corrupting it. If [gen]     │
  │              │  were dropped entirely, [dbo] would be undamaged.       │
  ├──────────────┼─────────────────────────────────────────────────────────┤
  │  [dim]       │  DIMENSION VIEW LAYER — SQL views that present          │
  │  [fact]      │  analytically-ready dimension and fact tables to        │
  │              │  Power BI. This is the semantic layer — the clean,      │
  │              │  business-facing contract between the database and the  │
  │              │  reporting tool. All transformations, temporal shifts,  │
  │              │  column renames, and computed classifications happen    │
  │              │  here, never in Power Query.                            │
  └──────────────┴─────────────────────────────────────────────────────────┘

--------------------------------------------------------------------------------
  DESIGN PHILOSOPHY
--------------------------------------------------------------------------------

  1. SOURCE IMMUTABILITY
     The [dbo] schema is the source of record. The project philosophy demands
     that all transformation and extension work happens downstream. No UPDATE,
     INSERT, DELETE, or ALTER statement is ever issued against any [dbo] table.
     This means the project is fully repeatable — dropping [gen], [dim], and
     [fact] and re-running all scripts from scratch always produces an
     identical result.

  2. SCHEMA AS A NAMESPACE CONTRACT
     Each schema is a deliberate namespace that signals intent to any reader
     of the codebase:
       - Seeing [gen.xxx]  tells you: this is synthetic data we created.
       - Seeing [dim.vXxx] tells you: this is a clean dimension for Power BI.
       - Seeing [fact.vXxx] tells you: this is a measurement fact for Power BI.
     This naming discipline removes ambiguity and makes the codebase
     self-documenting.

  3. PUSH LOGIC UPSTREAM (ROCHE'S MAXIM)
     All business logic — temporal date shifts, computed classifications,
     label decoding, denominator-safe divisions, SLA tiers — is pushed as
     far upstream as possible into the SQL view layer. Power Query in Power BI
     is kept thin. DAX measures are kept focused on aggregation and context
     manipulation, not data transformation. The database does the heavy
     lifting because it is better at it.

  4. TEMPORAL CONSISTENCY
     The entire dataset's temporal range has been shifted forward by +16 years
     at the [dim] and [fact] view layer (2007–2009 source → 2023–2025
     presentation). All date-based computed columns — customer age, employee
     tenure, product lifecycle status, promotion status — are anchored to the
     fixed reference date '2025-12-31'. GETDATE() is never used for
     historical computed attributes. This ensures every student running the
     scripts on any date gets identical, reproducible analytical results.

  5. VERTIPAQ-FIRST DATA TYPES
     All DateKey columns across every fact and dimension view are stored as
     INT in YYYYMMDD format (e.g., 20250115). This is a conscious VertiPaq
     engine optimisation. Integers compress dramatically better than DateTime
     strings using Value Encoding. Star Schema relationships that match INT
     on both sides resolve faster than any other data type pairing. This
     decision is applied universally — no exceptions.

--------------------------------------------------------------------------------
  EXECUTION CONTEXT
--------------------------------------------------------------------------------

  Run on      : ContosoRetailDW (fresh instance)
  Run order   : Script 00 — MUST run before any other script in the project
  Dependencies: None — this script has zero dependencies
  Impact      : Creates schemas only. Zero modifications to [dbo].
  Safe to re-run: YES — IF NOT EXISTS guards on all three schemas.

--------------------------------------------------------------------------------
  WHAT COMES AFTER THIS SCRIPT
--------------------------------------------------------------------------------

  After this script completes, the execution order is:

  Script 01  →  gen Reference Dimensions
               (gen.DimAcquisitionChannel, gen.DimPaymentMethod,
                gen.DimReturnReason)

  Script 02  →  gen Customer Acquisition Channel Assignment
               (gen.CustomerAcquisition)

  Script 03  →  gen Order Payment Method Assignment
               (gen.OrderPayment)

  Script 04  →  gen Order Fulfillment Lifecycle Data
               (gen.OrderFulfillment)

  Script 05  →  gen Marketing Spend Data
               (gen.FactMarketingSpend)
               ⚠ Depends on Script 02 — must run after

  Script 06  →  gen Customer Survey Data (NPS & CSAT)
               (gen.FactCustomerSurvey)

  Script 07  →  gen Online Return Events
               (gen.OnlineReturnEvents)

  Script 08  →  gen Physical Return Events
               (gen.PhysicalReturnEvents)

  Scripts 09–18 →  [dim] and [fact] analytical views
                   (Power BI semantic layer)

================================================================================
  END OF DOCUMENTATION HEADER
================================================================================
*/


-- ============================================================================
-- SAFETY CHECK: Confirm we are on the correct database before proceeding.
-- This guard prevents accidental schema creation on the wrong instance.
-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 1 — DATABASE SAFETY GUARD                                      ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Confirms the session is connected to ContosoRetailDW before allowing any  ║
-- ║  schema creation to proceed. If the wrong database is active it raises a   ║
-- ║  descriptive error and freezes all subsequent batches.                     ║
-- ║                                                                             ║
-- ║  WHY IT EXISTS                                                              ║
-- ║  SSMS lets you run any script against any connected database. Without this ║
-- ║  guard, running this script against master or tempdb would silently create ║
-- ║  [gen], [dim], and [fact] there. The failure would surface much later when ║
-- ║  downstream scripts cannot find [dbo] tables.                              ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  ──────────────────────────────────────────────────────────────────────    ║
-- ║  1. RAISERROR cannot accept a function call directly as its %s argument.  ║
-- ║     DB_NAME() must be captured into a variable first. This is a T-SQL     ║
-- ║     syntax constraint — not a style choice.                                ║
-- ║                                                                             ║
-- ║  2. Use SET NOEXEC ON, NOT RETURN.                                          ║
-- ║     RETURN exits the current batch only. Because this script uses GO       ║
-- ║     separators, every subsequent batch after the next GO would still       ║
-- ║     execute. SET NOEXEC ON instructs SQL Server to parse but skip ALL      ║
-- ║     subsequent batches for the entire session — which is correct here.     ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT ON SUCCESS                                                 ║
-- ║  ✓ Database confirmed: ContosoRetailDW                                     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

IF DB_NAME() <> 'ContosoRetailDW'
BEGIN
    -- 1. Capture the function output into a variable first
    DECLARE @CurrentDB NVARCHAR(128) = DB_NAME();

    DECLARE @ErrorMsg NVARCHAR(2048) = FORMATMESSAGE('ERROR: Must run against ContosoRetailDW.
                                                      Current DB is [%s].', @CurrentDB);
    -- 2. Raise a descriptive error message
    THROW 50001, @ErrorMsg, 1;
    
    -- 3. Halt execution for the entire session
     
END
GO

PRINT '✓ Database confirmed: ' + DB_NAME();
PRINT '';
GO


-- ============================================================================
-- SCHEMA 1: [gen] — Synthetic Extension Layer
-- ============================================================================
-- PURPOSE:
--   Houses all synthetic tables created to fill analytical gaps in the
--   Contoso source dataset. These tables extend the source to enable
--   executive BI questions that the original data cannot answer.
--
-- CONTENTS (created by later scripts):
--   gen.DimAcquisitionChannel   — 7 marketing acquisition channels
--   gen.DimPaymentMethod        — 6 payment methods
--   gen.DimReturnReason         — 8 return reason codes
--   gen.CustomerAcquisition     — One row per customer, acquisition channel
--   gen.OrderPayment            — One row per order, payment method
--   gen.OrderFulfillment        — One row per order, fulfillment lifecycle
--   gen.FactMarketingSpend      — Monthly marketing spend by channel
--   gen.FactCustomerSurvey      — NPS and CSAT survey responses
--   gen.OnlineReturnEvents      — Synthesized return dates for online sales
--   gen.PhysicalReturnEvents    — Synthesized return dates for physical sales
-- ============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 2 — [gen] SCHEMA CREATION                                       ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates [gen] if it does not already exist. If it exists the block        ║
-- ║  prints a skip message and does nothing — all contents are preserved.      ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  ──────────────────────────────────────────────────────────────────────    ║
-- ║  1. CREATE SCHEMA must be the FIRST statement in a batch. Because we wrap  ║
-- ║     it inside an IF block, it must be in EXEC() — pushing it into a        ║
-- ║     dynamic execution context where it is treated as a standalone batch.   ║
-- ║     This is one of the few valid uses of dynamic SQL in DDL scripts.       ║
-- ║                                                                             ║
-- ║  2. AUTHORIZATION [dbo] makes the dbo user the schema owner so that any    ║
-- ║     dbo-role user can create, alter, or drop objects without extra grants. ║
-- ║                                                                             ║
-- ║  3. IF NOT EXISTS makes this block safe to re-run on a database that       ║
-- ║     already has gen tables inside it — existing objects are untouched.     ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT  (first run) : ✓ Schema [gen] created successfully.      ║
-- ║  EXPECTED OUTPUT  (re-run)    : → Schema [gen] already exists. Skipping.  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Part 1: [gen] Schema
-- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gen')
IF SCHEMA_ID('gen') IS NULL
BEGIN
    EXEC('CREATE SCHEMA [gen] AUTHORIZATION [dbo]');
    PRINT '✓ Schema [gen] created successfully.';
END
ELSE
    PRINT '→ Schema [gen] already exists. Skipping creation.';
GO


-- ============================================================================
-- SCHEMA 2: [dim] — Dimension View Layer
-- ============================================================================
-- PURPOSE:
--   Houses all SQL views that present conformed dimension tables to Power BI.
--   Every view in this schema follows Kimball Star Schema dimension design:
--   one row per entity, descriptive attributes only, no measures.
--
-- NAMING CONVENTION:
--   All views are prefixed with 'v' (e.g., dim.vDate, dim.vCustomer).
--   This distinguishes views from physical tables at a glance.
--
-- CONTENTS (created by later scripts):
--   dim.vDate                 — Conformed date dimension (July-start fiscal)
--   dim.vCustomer             — Customer dimension with flattened geography
--   dim.vProduct              — Product dimension (flattened snowflake)
--   dim.vStore                — Store dimension with flattened geography
--   dim.vEmployee             — Employee dimension
--   dim.vPromotion            — Promotion dimension with discount tiers
--   dim.vCurrency             — Currency lookup dimension
--   dim.vPaymentMethod        — Payment method dimension (from [gen])
--   dim.vAcquisitionChannel   — Acquisition channel dimension (from [gen])
--   dim.vReturnReason         — Return reason dimension (from [gen])
-- ============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 3 — [dim] SCHEMA CREATION                                       ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates [dim] — the home for all SQL dimension views loaded into Power BI.║
-- ║  Follows the same idempotent IF NOT EXISTS / EXEC pattern as [gen].        ║
-- ║                                                                             ║
-- ║  WHY [dim] IS A VIEWS-ONLY SCHEMA                                          ║
-- ║  Dimension objects in this project are CREATE OR ALTER VIEW definitions.   ║
-- ║  They read from [dbo] or [gen] tables and apply transformations: column    ║
-- ║  renames, computed classifications, the +16 year temporal shift, snowflake ║
-- ║  flattening, and type formatting. Keeping views in [dim] means any reader  ║
-- ║  instantly knows a [dim.vXxx] object is a Power-BI-facing view, not a     ║
-- ║  physical storage object.                                                  ║
-- ║                                                                             ║
-- ║  NAMING CONVENTION                                                          ║
-- ║  All views carry the 'v' prefix (dim.vDate, dim.vCustomer, etc.).          ║
-- ║  This distinguishes views from tables in the object browser without        ║
-- ║  needing to inspect the object type column.                                ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT  (first run) : ✓ Schema [dim] created successfully.      ║
-- ║  EXPECTED OUTPUT  (re-run)    : → Schema [dim] already exists. Skipping.  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Part 2: [dim] Schema
-- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
IF SCHEMA_ID('dim') IS NULL
BEGIN
    EXEC('CREATE SCHEMA [dim] AUTHORIZATION [dbo]');
    PRINT '✓ Schema [dim] created successfully.';
END
ELSE
    PRINT '→ Schema [dim] already exists. Skipping creation.';
GO


-- ============================================================================
-- SCHEMA 3: [fact] — Fact View Layer
-- ============================================================================
-- PURPOSE:
--   Houses all SQL views that present fact tables to Power BI.
--   Every view in this schema follows Kimball Star Schema fact design:
--   integer foreign keys pointing to [dim] views, additive measures,
--   and no descriptive attributes (those belong in dimensions).
--
-- NAMING CONVENTION:
--   All views are prefixed with 'v' (e.g., fact.vOnlineSales, fact.vReturns).
--
-- CONTENTS (created by later scripts):
--   fact.vOnlineSales          — Online sales transactions (SalesQty > 0)
--   fact.vReturns              — Unified online + physical returns (UNION ALL)
--   fact.vInventory            — Inventory snapshot (Product × Store × Date)
--   fact.vSalesQuota           — Budget / Actual / Forecast planning fact
--   fact.vExchangeRate         — Monthly currency exchange rates
--   fact.vOrderFulfillment     — Order fulfillment lifecycle (3 DateKeys)
--   fact.vCustomerSurvey       — NPS and CSAT survey responses
--   fact.vMarketingSpend       — Monthly marketing spend by channel
--   fact.vCustomerAcquisition  — Customer acquisition channel bridge
--   fact.vOrderPayment         — Order-level payment method bridge
-- ============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 4 — [fact] SCHEMA CREATION                                      ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Creates [fact] — the home for all SQL fact views loaded into Power BI.    ║
-- ║  Same pattern and rationale as [gen] and [dim].                            ║
-- ║                                                                             ║
-- ║  STAR SCHEMA PRINCIPLE — WHY TWO SEPARATE SCHEMAS ([dim] AND [fact])       ║
-- ║  In Kimball dimensional modelling, fact tables hold measurements (how      ║
-- ║  much, how many) and integer foreign keys. Dimension tables hold context   ║
-- ║  (who, what, where, when). Separating them into [dim] and [fact] enforces ║
-- ║  this distinction at the namespace level. Any developer reading a query    ║
-- ║  immediately knows whether they are joining to a measurement or a context  ║
-- ║  object simply from the schema prefix — before reading a single column.   ║
-- ║                                                                             ║
-- ║  FACT VIEWS vs FACT TABLES                                                  ║
-- ║  Like [dim], all [fact] objects are CREATE OR ALTER VIEW definitions.      ║
-- ║  Physical storage lives in [dbo] and [gen]. The [fact] views select from   ║
-- ║  those physical tables and add computed columns: DateKeys (YYYYMMDD INT),  ║
-- ║  SLA tier labels, net sales expressions, and IsOnTime flags — work that    ║
-- ║  would be expensive to compute in DAX on every query refresh.             ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT  (first run) : ✓ Schema [fact] created successfully.     ║
-- ║  EXPECTED OUTPUT  (re-run)    : → Schema [fact] already exists. Skipping. ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Part 3: [fact] Schema
-- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
IF SCHEMA_ID('fact') IS NULL
BEGIN
    EXEC('CREATE SCHEMA [fact] AUTHORIZATION [dbo]');
    PRINT '✓ Schema [fact] created successfully.';
END
ELSE
    PRINT '→ Schema [fact] already exists. Skipping creation.';
GO


-- ============================================================================
-- COMPLETION CONFIRMATION
-- ============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — COMPLETION BANNER + SET NOEXEC OFF RESET                    ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Prints a completion banner confirming schema creation, then issues        ║
-- ║  SET NOEXEC OFF to reset the session execution context.                    ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — WHY SET NOEXEC OFF IS ESSENTIAL               ║
-- ║  If the database safety check at the top triggered SET NOEXEC ON, every   ║
-- ║  batch in this script was skipped — including this one. However,           ║
-- ║  SET NOEXEC ON persists for the session BEYOND this script. SET NOEXEC OFF ║
-- ║  ensures a developer who ran against the wrong database and then corrected ║
-- ║  the connection can re-run without the session remaining frozen.           ║
-- ║  Always place SET NOEXEC OFF at the end of any script that uses            ║
-- ║  SET NOEXEC ON as a guard mechanism.                                       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Part 4: Execution Confirmation & Safety Reset
PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 00 completed successfully.';
PRINT '  Schemas verified: [gen], [dim], [fact] (Owned by dbo)';
PRINT '  Next step: Run Script 01 — gen Reference Dimensions';
PRINT '════════════════════════════════════════════════════════════════';
GO

-- CRITICAL: Reset the execution context in case the header script halted execution

GO

-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  VERIFICATION QUERY — SCHEMA EXISTENCE CHECK (OPTIONAL / UNCOMMENT)         ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Queries sys.schemas to list all non-system schemas and their owners.      ║
-- ║  Uncomment and run after Script 00 to confirm all three schemas exist.     ║
-- ║                                                                             ║
-- ║  HOW TO USE IN SSMS                                                         ║
-- ║  Highlight the query block, press Ctrl+K then Ctrl+U to uncomment,        ║
-- ║  execute, then Ctrl+K + Ctrl+C to re-comment before saving.               ║
-- ║                                                                             ║
-- ║  ┌──────────────────────────────────────────────────────────────────────┐  ║
-- ║  │  EXPECTED OUTPUT — exactly 3 rows, all owned by dbo:                 │  ║
-- ║  ├──────────────────────────────────────────────────────────────────────┤  ║
-- ║  │  SchemaName  │  SchemaOwner                                          │  ║
-- ║  │  ────────────────────────                                            │  ║
-- ║  │  dim         │  dbo                                                  │  ║
-- ║  │  fact        │  dbo                                                  │  ║
-- ║  │  gen         │  dbo                                                  │  ║
-- ║  │                                                                      │  ║
-- ║  │  ✗ If any row is missing: the corresponding IF NOT EXISTS block      │  ║
-- ║  │    failed — check the Messages tab for the error detail.             │  ║
-- ║  │  ✗ If SchemaOwner is not dbo: the AUTHORIZATION clause was           │  ║
-- ║  │    overridden — check your SQL Server user permissions.              │  ║
-- ║  └──────────────────────────────────────────────────────────────────────┘  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- This is the verification query to confirm the schemas were created successfully.
-- Uncomment this query as soon as you run the script to see the list of schemas and their owners in the database.
-- ============================================================================

SELECT 
    s.name AS SchemaName,
    p.name AS SchemaOwner
FROM sys.schemas s
INNER JOIN sys.database_principals p 
    ON s.principal_id = p.principal_id
-- WHERE s.name NOT IN ('dbo', 'guest', 'sys', 'INFORMATION_SCHEMA')
--    AND s.name NOT LIKE 'db_%' -- Strips out default database role schemas
WHERE p.type <> 'R' -- Exclude schemas owned by Database Roles (which strips the db_ stuff automatically)
AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest') -- Exclude system schemas and the guest user schema
ORDER BY s.name;
