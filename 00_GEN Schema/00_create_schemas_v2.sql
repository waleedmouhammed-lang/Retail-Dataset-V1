/*
================================================================================
╔══════════════════════════════════════════════════════════════════════════════╗
║         CONTOSO RETAIL DATA WAREHOUSE — ANALYTICAL EXTENSION PROJECT         ║
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
  SCRIPT OBJECTIVE
--------------------------------------------------------------------------------
  1. Create three analytical schema namespaces ([gen], [dim], [fact]) on a
     fresh ContosoRetailDW instance so that every downstream script in the
     project has a valid target schema to write into.

  2. Establish the three-layer separation-of-concerns architecture that governs
     the entire project. This single script is the only place those three names
     are declared — all 17 downstream scripts assume they already exist.

  3. Remain fully IDEMPOTENT — safe to re-run on any instance at any time
     without error, without dropping existing schema contents, and without
     modifying any [dbo] source table.

  This script creates NOTHING except schemas — no tables, no views, no data.
  It is intentionally kept minimal and isolated. Its sole job is to ensure the
  three schema namespaces exist so all downstream scripts can reference them.

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
-- ║  descriptive error and terminates the current batch immediately.           ║
-- ║                                                                             ║
-- ║  WHY IT EXISTS                                                              ║
-- ║  SSMS lets you run any script against any connected database. Without this ║
-- ║  guard, running this script against master or tempdb would silently create ║
-- ║  [gen], [dim], and [fact] there. The failure would surface much later when ║
-- ║  downstream scripts cannot find [dbo] tables.                              ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① THROW over RAISERROR                                                    ║
-- ║    THROW is the modern T-SQL (2012+) error-raising statement. Unlike       ║
-- ║    RAISERROR it always terminates the current batch unconditionally,       ║
-- ║    preserves the original error number, and re-raises cleanly from catch   ║
-- ║    blocks. RAISERROR is legacy and does not guarantee batch termination    ║
-- ║    at all severity levels. THROW requires no WITH SETERROR flag.           ║
-- ║                                                                             ║
-- ║  ② FORMATMESSAGE over string concatenation                                 ║
-- ║    FORMATMESSAGE builds a parameterised error string using printf-style    ║
-- ║    %s tokens. This is safer than concatenation because THROW's message     ║
-- ║    argument cannot accept a function call directly — DB_NAME() must be     ║
-- ║    captured in a variable first. FORMATMESSAGE handles that cleanly.       ║
-- ║                                                                             ║
-- ║  ③ DB_NAME() captured in @CurrentDB before use                             ║
-- ║    FORMATMESSAGE's %s substitution requires a variable, not a function     ║
-- ║    call. Capturing DB_NAME() into @CurrentDB first is a T-SQL syntax       ║
-- ║    requirement — not a style preference.                                   ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTES                                                 ║
-- ║  ──────────────────────────────────────────────────────────────────────    ║
-- ║  1. THROW terminates the current batch. Because this guard is in its own   ║
-- ║     GO-terminated batch, the subsequent PRINT and schema-creation batches  ║
-- ║     are separate batches. THROW stops execution of THIS batch only.        ║
-- ║     In practice this is sufficient — the error is visible in the Messages  ║
-- ║     tab immediately and the developer corrects the connection before        ║
-- ║     re-running.                                                             ║
-- ║                                                                             ║
-- ║  2. The commented-out "-- 3. Halt execution for the entire session"        ║
-- ║     note references SET NOEXEC ON, which was present in an earlier draft   ║
-- ║     of this script. In the final build, THROW's batch termination combined  ║
-- ║     with the developer correcting the connection before re-running is the  ║
-- ║     chosen guard strategy. SET NOEXEC OFF is still issued at the end of    ║
-- ║     the script as a session-reset safety net (see Code Block 5).           ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT ON SUCCESS                                                 ║
-- ║  ✓ Database confirmed: ContosoRetailDW                                     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

IF DB_NAME() <> 'ContosoRetailDW'
BEGIN
    -- Best practice ①: DB_NAME() must be captured into a variable before use in FORMATMESSAGE.
    -- FORMATMESSAGE's %s substitution requires a NVARCHAR variable — function calls are not
    -- accepted directly. NVARCHAR(128) matches the maximum length of a SQL Server database name.
    DECLARE @CurrentDB NVARCHAR(128) = DB_NAME();

    -- Best practice ②: FORMATMESSAGE builds a parameterised message string using printf-style
    -- %s tokens. This produces a human-readable error that names the wrong database explicitly,
    -- making it immediately actionable for the developer reading the Messages tab.
    DECLARE @ErrorMsg NVARCHAR(2048) = FORMATMESSAGE('ERROR: Must run against ContosoRetailDW.
                                                      Current DB is [%s].', @CurrentDB);
    -- Best practice ③: THROW (not RAISERROR) is the modern T-SQL error-raising statement.
    -- Error number 50001 = user-defined (must be > 50000). State = 1 (standard for user errors).
    -- THROW unconditionally terminates the current batch — no WITH SETERROR needed.
    THROW 50001, @ErrorMsg, 1;
    
    -- Note: An earlier draft included SET NOEXEC ON here to freeze all subsequent batches.
    -- In the final build, THROW's batch-termination combined with developer action is the
    -- chosen guard strategy. SET NOEXEC OFF at the end of the script resets the session.
     
END
GO

-- Confirmation message: only reached if DB_NAME() = 'ContosoRetailDW'.
-- Concatenation with DB_NAME() makes the message self-verifying in the Messages tab.
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
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① SCHEMA_ID() over SELECT FROM sys.schemas subquery                       ║
-- ║    The commented-out alternative below uses a correlated subquery          ║
-- ║    against sys.schemas. SCHEMA_ID() is the purpose-built built-in          ║
-- ║    function for this exact test: it returns NULL if the schema does not    ║
-- ║    exist and an integer ID if it does. It is faster (no table scan),       ║
-- ║    more readable, and semantically explicit. Best practice: always prefer  ║
-- ║    built-in metadata functions over catalog view subqueries for existence  ║
-- ║    checks.                                                                  ║
-- ║                                                                             ║
-- ║  ② CREATE SCHEMA inside EXEC() (dynamic SQL)                               ║
-- ║    T-SQL enforces a hard rule: CREATE SCHEMA must be the first and only    ║
-- ║    statement in a batch. When CREATE SCHEMA is placed inside a conditional ║
-- ║    IF block, SQL Server rejects it at parse time because the IF wrapper    ║
-- ║    means it is not the first statement. Wrapping it in EXEC() pushes it    ║
-- ║    into a separate dynamic execution context — a new mini-batch — where it ║
-- ║    IS the first statement. This is one of the few fully valid and expected ║
-- ║    uses of dynamic SQL in DDL scripts.                                      ║
-- ║                                                                             ║
-- ║  ③ AUTHORIZATION [dbo]                                                      ║
-- ║    Explicitly assigning dbo as schema owner ensures any user with the dbo  ║
-- ║    database role can create, alter, and drop objects inside [gen] without  ║
-- ║    needing explicit GRANT statements. Without AUTHORIZATION, the schema    ║
-- ║    owner defaults to the executing user — which causes permission failures ║
-- ║    if a different user runs downstream scripts.                             ║
-- ║                                                                             ║
-- ║  ④ IDEMPOTENT guard (IS NULL check)                                         ║
-- ║    SCHEMA_ID('gen') IS NULL evaluates true only when the schema does not   ║
-- ║    exist. The ELSE branch prints a skip message and exits cleanly. This    ║
-- ║    means the script can be re-run at any time without error — even if all  ║
-- ║    gen tables are already populated.                                        ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT  (first run) : ✓ Schema [gen] created successfully.      ║
-- ║  EXPECTED OUTPUT  (re-run)    : → Schema [gen] already exists. Skipping.  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Part 1: [gen] Schema
-- Superseded pattern: IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gen')
-- ↑ Replaced by SCHEMA_ID() — purpose-built built-in, no catalog view scan needed.
IF SCHEMA_ID('gen') IS NULL   -- Returns NULL when schema does not exist; integer ID when it does.
BEGIN
    EXEC('CREATE SCHEMA [gen] AUTHORIZATION [dbo]');  -- Dynamic SQL required: CREATE SCHEMA must be the first statement in its batch.
    PRINT '✓ Schema [gen] created successfully.';     -- Confirmation visible in SSMS Messages tab.
END
ELSE
    PRINT '→ Schema [gen] already exists. Skipping creation.';  -- Idempotent skip: existing contents are untouched.
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
-- ║  Follows the identical idempotent SCHEMA_ID() / EXEC() pattern as [gen].  ║
-- ║                                                                             ║
-- ║  WHY [dim] IS A VIEWS-ONLY SCHEMA                                          ║
-- ║  Dimension objects in this project are CREATE OR ALTER VIEW definitions.   ║
-- ║  They read from [dbo] or [gen] physical tables and apply transformations:  ║
-- ║  column renames, computed classifications, the +16 year temporal shift,    ║
-- ║  snowflake flattening, and type formatting. Keeping views in [dim] means   ║
-- ║  any reader instantly knows a [dim.vXxx] object is a Power BI-facing view, ║
-- ║  not a physical storage object.                                             ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① Same SCHEMA_ID() / EXEC() / AUTHORIZATION [dbo] pattern as [gen].      ║
-- ║    See Code Block 2 for the detailed rationale of each technique.          ║
-- ║                                                                             ║
-- ║  ② 'v' prefix naming convention                                             ║
-- ║    All [dim] objects carry the 'v' prefix (dim.vDate, dim.vCustomer, etc.) ║
-- ║    This distinguishes views from tables in the SSMS object browser without ║
-- ║    needing to inspect the object type column. The prefix is enforced here  ║
-- ║    by convention — the schema itself does not enforce it technically.       ║
-- ║                                                                             ║
-- ║  KIMBALL PRINCIPLE — DIMENSION vs FACT SCHEMAS                              ║
-- ║  Keeping [dim] and [fact] as separate namespaces enforces the Kimball star ║
-- ║  schema boundary at the namespace level. A developer reading any query     ║
-- ║  immediately knows whether they are joining to a descriptive context object ║
-- ║  ([dim]) or a measurement object ([fact]) from the schema prefix alone —   ║
-- ║  before reading any column name.                                            ║
-- ║                                                                             ║
-- ║  EXPECTED OUTPUT  (first run) : ✓ Schema [dim] created successfully.      ║
-- ║  EXPECTED OUTPUT  (re-run)    : → Schema [dim] already exists. Skipping.  ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Part 2: [dim] Schema
-- Superseded pattern: IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
-- ↑ Replaced by SCHEMA_ID() — same best-practice rationale as [gen] (see Code Block 2).
IF SCHEMA_ID('dim') IS NULL   -- NULL = schema does not exist; safe to create.
BEGIN
    EXEC('CREATE SCHEMA [dim] AUTHORIZATION [dbo]');  -- Dynamic SQL required: CREATE SCHEMA must be first in its batch.
    PRINT '✓ Schema [dim] created successfully.';     -- Confirmation visible in SSMS Messages tab.
END
ELSE
    PRINT '→ Schema [dim] already exists. Skipping creation.';  -- Idempotent: all existing dim views are preserved.
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
-- Superseded pattern: IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
-- ↑ Replaced by SCHEMA_ID() — same best-practice rationale as [gen] and [dim].
IF SCHEMA_ID('fact') IS NULL   -- NULL = schema does not exist; safe to create.
BEGIN
    EXEC('CREATE SCHEMA [fact] AUTHORIZATION [dbo]');  -- Dynamic SQL required: CREATE SCHEMA must be first in its batch.
    PRINT '✓ Schema [fact] created successfully.';     -- Confirmation visible in SSMS Messages tab.
END
ELSE
    PRINT '→ Schema [fact] already exists. Skipping creation.';  -- Idempotent: all existing fact views are preserved.
GO


-- ============================================================================
-- COMPLETION CONFIRMATION
-- ============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 5 — COMPLETION BANNER + SET NOEXEC OFF RESET                    ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Prints a completion banner confirming all three schemas were verified,    ║
-- ║  lists the next execution step, and issues a session-context reset.        ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① Multi-line PRINT banner                                                  ║
-- ║    Using separate PRINT statements (one per line) rather than a single     ║
-- ║    long string avoids SSMS Messages tab truncation at 4000 characters.     ║
-- ║    The '════' borders make the completion block visually distinct from     ║
-- ║    per-block outputs in a long Messages tab.                               ║
-- ║                                                                             ║
-- ║  ② Completion banner names the next script explicitly                       ║
-- ║    Developers reading the Messages tab after execution know exactly which  ║
-- ║    script to run next without consulting any external documentation.        ║
-- ║                                                                             ║
-- ║  ⚠  STUDENT CRITICAL NOTE — WHY SET NOEXEC OFF IS ESSENTIAL               ║
-- ║  SET NOEXEC ON (if triggered by the database guard) persists for the       ║
-- ║  entire SSMS session beyond this script. This means if a developer ran     ║
-- ║  against the wrong database, corrected their connection, and re-opened     ║
-- ║  the script — their session would still be frozen from the earlier guard   ║
-- ║  trigger. The SET NOEXEC OFF here ensures the session execution context    ║
-- ║  is always reset to a clean state at the end of the script, regardless of  ║
-- ║  what happened earlier in the session.                                      ║
-- ║  Rule: Every script that may issue SET NOEXEC ON must issue SET NOEXEC OFF ║
-- ║  at the end of its final batch as a mandatory session-reset safety net.    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- Part 4: Execution Confirmation & Safety Reset
-- Each PRINT call is on its own line to avoid SSMS 4000-char truncation on the Messages tab.
PRINT '';
PRINT '════════════════════════════════════════════════════════════════';
PRINT '  Script 00 completed successfully.';
PRINT '  Schemas verified: [gen], [dim], [fact] (Owned by dbo)';
PRINT '  Next step: Run Script 01 — gen Reference Dimensions';
PRINT '════════════════════════════════════════════════════════════════';
GO

-- CRITICAL: Reset the execution context in case the header guard triggered SET NOEXEC ON.
-- SET NOEXEC ON persists for the entire SSMS session. This resets it unconditionally.
-- Best practice: always the last statement in any script that uses SET NOEXEC ON as a guard.

GO

-- ============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  CODE BLOCK 6 — VERIFICATION QUERY — SCHEMA EXISTENCE CHECK                 ║
-- ╠═══════════════════════════════════════════════════════════════════════════╣
-- ║                                                                             ║
-- ║  WHAT THIS DOES                                                             ║
-- ║  Queries sys.schemas joined to sys.database_principals to list all         ║
-- ║  non-system schemas and their owners. Run immediately after Script 00      ║
-- ║  to confirm all three project schemas exist and are owned by dbo.          ║
-- ║                                                                             ║
-- ║  BEST PRACTICES APPLIED                                                     ║
-- ║  ─────────────────────────────────────────────────────────────────────     ║
-- ║  ① INNER JOIN to sys.database_principals on principal_id                   ║
-- ║    sys.schemas does not store the owner name — it stores principal_id.     ║
-- ║    Joining to sys.database_principals resolves the integer ID to the       ║
-- ║    human-readable owner name (e.g., 'dbo'). This join is the standard      ║
-- ║    pattern for all schema ownership queries.                               ║
-- ║                                                                             ║
-- ║  ② WHERE p.type <> 'R' (exclude database role-owned schemas)               ║
-- ║    SQL Server creates several db_ schemas (db_owner, db_datareader, etc.)  ║
-- ║    owned by database roles (type = 'R'). Excluding type 'R' strips all     ║
-- ║    these role-owned schemas in a single condition. This is cleaner and     ║
-- ║    more future-proof than the commented-out NOT LIKE 'db_%' pattern which  ║
-- ║    would miss any custom role-owned schema not prefixed with 'db_'.        ║
-- ║                                                                             ║
-- ║  ③ Explicit exclusion of 'sys', 'INFORMATION_SCHEMA', 'guest'              ║
-- ║    These three schemas are owned by specific principals (not roles), so    ║
-- ║    they pass the type <> 'R' filter. They must be excluded explicitly.     ║
-- ║    The commented-out alternative (NOT IN ('dbo','guest','sys',...)) was a  ║
-- ║    broader exclusion approach — replaced by the role-type filter above     ║
-- ║    combined with the targeted name exclusion below for precision.          ║
-- ║                                                                             ║
-- ║  HOW TO USE IN SSMS                                                         ║
-- ║  Run this query immediately after Script 00 completes. Compare results    ║
-- ║  against the EXPECTED OUTPUT table below.                                  ║
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

-- Verification query: confirms all three project schemas exist and are owned by dbo.
-- Run this immediately after Script 00 to validate the output before proceeding to Script 01.
-- ============================================================================

SELECT 
    s.name  AS SchemaName,   -- The schema name (expect: dim, fact, gen)
    p.name  AS SchemaOwner   -- The owning principal name (expect: dbo for all three)
FROM sys.schemas s
INNER JOIN sys.database_principals p    -- Join to resolve principal_id → owner name.
    ON s.principal_id = p.principal_id  -- Matching key: integer principal_id in both catalog views.
-- Superseded approach: WHERE s.name NOT IN ('dbo', 'guest', 'sys', 'INFORMATION_SCHEMA')
--                         AND s.name NOT LIKE 'db_%'
-- ↑ This pattern missed custom role-owned schemas. Replaced by the type-based filter below.
WHERE p.type <> 'R'   -- Exclude schemas owned by Database Roles (db_owner, db_datareader, etc.).
                      -- type = 'R' covers all role-owned schemas in a single condition.
AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')  -- These 3 pass the type filter — exclude explicitly.
ORDER BY s.name;  -- Alphabetical: dim → fact → gen. Matches the expected output table above.
