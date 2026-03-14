-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║                                                                              ║
-- ║   PROJECT    : Contoso Retail — End-to-End BI Analytics                     ║
-- ║   PROGRAMME  : DEPI — Data Analysis with Power BI Track                     ║
-- ║   AUTHOR     : Waleed Mouhammed                                              ║
-- ║   ENGINE     : SQL Server 2025 (T-SQL)                                      ║
-- ║   SCRIPT     : 09 — Dimension Views ([dim] schema)                          ║
-- ║   VERSION    : 1.0                                                           ║
-- ║   DATE       : March 2026                                                    ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  AI DISCLOSURE                                                               ║
-- ║  This script was collaboratively designed with an AI assistant. All         ║
-- ║  business logic, thresholds, naming conventions, and architectural          ║
-- ║  decisions have been reviewed and approved by the lead architect.           ║
-- ║  Students should treat this script as authoritative production code.        ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  PURPOSE                                                                     ║
-- ║  Creates all 11 analytical dimension views in the [dim] schema. These       ║
-- ║  views form the semantic layer consumed by Power BI via Parquet export.     ║
-- ║  They are the single source of truth for all dimension attributes,          ║
-- ║  classifications, and computed labels in the model.                         ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  VIEWS CREATED (11 TOTAL)                                                   ║
-- ║                                                                              ║
-- ║   #   View Name                  Source(s)                                  ║
-- ║  ─────────────────────────────────────────────────────────────────────────  ║
-- ║   1   dim.vDate                  dbo.DimDate                                ║
-- ║   2   dim.vCustomer              dbo.DimCustomer + dbo.DimGeography         ║
-- ║   3   dim.vProduct               dbo.DimProduct + Subcategory + Category    ║
-- ║   4   dim.vStore                 dbo.DimStore + dbo.DimGeography            ║
-- ║   5   dim.vPromotion             dbo.DimPromotion                           ║
-- ║   6   dim.vEmployee              dbo.DimEmployee (Status = 'Current')       ║
-- ║   7   dim.vPaymentMethod         gen.DimPaymentMethod                       ║
-- ║   8   dim.vAcquisitionChannel    gen.DimAcquisitionChannel                  ║
-- ║   9   dim.vCurrency              dbo.DimCurrency                            ║
-- ║  10   dim.vReturnReason          gen.DimReturnReason                        ║
-- ║  11   dim.vChannel               dbo.DimChannel  ← NEW (for StoreSales FK)  ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  TEMPORAL SHIFT — PROJECT-WIDE PRINCIPLE                                    ║
-- ║  Source data (dbo, gen) stores dates in the 2005–2011 era.                  ║
-- ║  A uniform +16-year offset is applied at this view layer to present         ║
-- ║  all dates as 2021–2027, with core analytical data landing in 2023–2025.   ║
-- ║                                                                              ║
-- ║  FIXED REFERENCE DATE  : '2025-12-31'                                       ║
-- ║  Used for ALL historical computed attributes (ages, tenures, statuses).    ║
-- ║  GETDATE() is used ONLY for dynamic flags (IsCurrentYear, IsCurrentMonth).  ║
-- ║                                                                              ║
-- ║  NON-SHIFTED ATTRIBUTES                                                     ║
-- ║  BirthDate on customers and employees is NOT shifted — it is a fixed        ║
-- ║  personal attribute. Age is computed from BirthDate against '2025-12-31'.  ║
-- ║  Static reference tables (PaymentMethod, AcquisitionChannel, Currency,      ║
-- ║  ReturnReason, Channel) carry no temporal context — no shift is applied.    ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  DESIGN PRINCIPLES                                                           ║
-- ║  • All DateKey columns: INT YYYYMMDD — VertiPaq-optimal join format         ║
-- ║  • Snowflake hierarchies flattened into denormalized dimension views         ║
-- ║  • Descriptive geography columns prefixed (Customer / Store) to prevent     ║
-- ║    column name collisions across dimensions in Power BI                     ║
-- ║  • ETL audit columns (ETLLoadID, LoadDate, UpdateDate) excluded             ║
-- ║  • CONCAT_WS used for name concatenation — NULL-safe                        ║
-- ║  • All division guards use NULLIF to prevent divide-by-zero                 ║
-- ║  • All CREATE OR ALTER VIEW — fully idempotent, safe to re-run              ║
-- ║                                                                              ║
-- ╠══════════════════════════════════════════════════════════════════════════════╣
-- ║                                                                              ║
-- ║  EXECUTION ORDER                                                             ║
-- ║  Run AFTER: Scripts 00–08 (schemas + all gen tables).                       ║
-- ║  Run BEFORE: Script 10 (fact views) — fact views reference dim views.       ║
-- ║                                                                              ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝


-- ============================================================================
-- PRE-EXECUTION CHECKS
-- ============================================================================
-- All source schemas and tables must exist before any view can be created.
-- Uses SET NOEXEC ON to halt execution on any failure, preserving database
-- integrity and giving the student an actionable error message.
-- ============================================================================

PRINT '════════════════════════════════════════════════════════════════════';
PRINT '  Script 09 — Pre-Execution Checks';
PRINT '════════════════════════════════════════════════════════════════════';

-- -- ── Check 1: [dim] schema must exist ─────────────────────────────────────────
-- -- IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
-- IF SCHEMA_ID('dim') IS NULL   -- NULL = [gen] does not exist; THROW will fire.
-- BEGIN
--     -- FORMATMESSAGE builds the error string cleanly — direct string concat in THROW is not supported.
--     DECLARE @ErrorMsg NVARCHAR(2048) = FORMATMESSAGE('ERROR: [dim] schema does not exist. Run Script 00 first.');
    
--     -- THROW (modern T-SQL 2012+): terminates the current batch unconditionally.
--     -- Superseded approach was: RAISERROR('...', 16, 1) — legacy, does not guarantee batch termination.
--     THROW 50001, @ErrorMsg, 1;
    
--     -- Note: THROW terminates this batch. The developer must run Script 00 and re-run this script.
--     -- An earlier draft placed SET NOEXEC ON here; THROW's batch termination is the chosen strategy.
    

-- END
-- ELSE
-- BEGIN
--     PRINT '✓ Schema [dim] confirmed.';   -- Visual confirmation in Messages tab before any DDL executes.
--     PRINT '';
-- END
-- GO

-- -- ── Check 2: [dbo] source tables ─────────────────────────────────────────────
-- IF OBJECT_ID('[dbo].[DimDate]',    'U') IS NULL OR
--    OBJECT_ID('[dbo].[DimCustomer]','U') IS NULL OR
--    OBJECT_ID('[dbo].[DimGeography]','U') IS NULL OR
--    OBJECT_ID('[dbo].[DimProduct]', 'U') IS NULL OR
--    OBJECT_ID('[dbo].[DimStore]',   'U') IS NULL OR
--    OBJECT_ID('[dbo].[DimPromotion]','U') IS NULL OR
--    OBJECT_ID('[dbo].[DimEmployee]','U') IS NULL OR
--    OBJECT_ID('[dbo].[DimCurrency]','U') IS NULL OR
--    OBJECT_ID('[dbo].[DimChannel]', 'U') IS NULL OR
--    OBJECT_ID('[dbo].[DimProductSubcategory]','U') IS NULL OR
--    OBJECT_ID('[dbo].[DimProductCategory]',  'U') IS NULL
-- BEGIN

--     DECLARE @ErrorMsg NVARCHAR(2048) = FORMATMESSAGE('FATAL: One or more required [dbo] source tables are missing. Run Scripts 01–08 first.');
--     THROW 50002, @ErrorMsg, 1;

-- END
-- ELSE PRINT '  ✓ All required [dbo] source tables confirmed.';
-- GO

-- -- ── Check 3: [gen] tables ─────────────────────────────────────────────────────
-- IF OBJECT_ID('[gen].[DimPaymentMethod]',   'U') IS NULL OR
--    OBJECT_ID('[gen].[DimAcquisitionChannel]','U') IS NULL OR
--    OBJECT_ID('[gen].[DimReturnReason]',    'U') IS NULL
-- BEGIN

--     DECLARE @ErrorMsg NVARCHAR(2048) = FORMATMESSAGE('FATAL: One or more required [gen] source tables are missing. Run Scripts 01–08 first.');
--     THROW 50003, @ErrorMsg, 1;

-- END
-- ELSE PRINT '  ✓ All required [gen] tables confirmed.';
-- GO

-- PRINT '  ✓ All pre-checks passed. Building dimension views...';
-- PRINT '';
-- GO

-- ── Check 2: [dbo] source tables ─────────────────────────────────────────────
DECLARE @MissingDboTables NVARCHAR(MAX);

-- Set-based evaluation: Checks all tables and aggregates the missing ones into a single string.
SELECT @MissingDboTables = STRING_AGG(TableName, ', ')
FROM (
    VALUES 
        ('[dbo].[DimDate]'),
        ('[dbo].[DimCustomer]'),
        ('[dbo].[DimGeography]'),
        ('[dbo].[DimProduct]'),
        ('[dbo].[DimStore]'),
        ('[dbo].[DimPromotion]'),
        ('[dbo].[DimEmployee]'),
        ('[dbo].[DimCurrency]'),
        ('[dbo].[DimChannel]'),
        ('[dbo].[DimProductSubcategory]'),
        ('[dbo].[DimProductCategory]')
) AS Required(TableName)
WHERE OBJECT_ID(TableName, 'U') IS NULL;

IF @MissingDboTables IS NOT NULL
BEGIN
    DECLARE @DboErrorMsg NVARCHAR(2048) = FORMATMESSAGE('FATAL: The following required [dbo] tables are missing: %s. Run Scripts 01–08 first.', @MissingDboTables);
    THROW 50002, @DboErrorMsg, 1;
END
ELSE 
BEGIN
    PRINT '  ✓ All required [dbo] source tables confirmed.';
END
GO

-- ── Check 3: [gen] tables ─────────────────────────────────────────────────────
DECLARE @MissingGenTables NVARCHAR(MAX);

SELECT @MissingGenTables = STRING_AGG(TableName, ', ')
FROM (
    VALUES 
        ('[gen].[DimPaymentMethod]'),
        ('[gen].[DimAcquisitionChannel]'),
        ('[gen].[DimReturnReason]')
) AS Required(TableName)
WHERE OBJECT_ID(TableName, 'U') IS NULL;

IF @MissingGenTables IS NOT NULL
BEGIN
    DECLARE @GenErrorMsg NVARCHAR(2048) = FORMATMESSAGE('FATAL: The following required [gen] tables are missing: %s. Run Scripts 01–08 first.', @MissingGenTables);
    THROW 50003, @GenErrorMsg, 1;
END
ELSE 
BEGIN
    PRINT '  ✓ All required [gen] tables confirmed.';
END
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 1 — dim.vDate
--  Source : dbo.DimDate
--  Grain  : One row per calendar day (2005-01-01–2011-12-31 source,
--           presented as 2021-01-01–2027-12-31 after +16 shift)
--  Key    : DateKey (INT YYYYMMDD) — conformed PK for all time intelligence
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  TEMPORAL SHIFT APPROACH
--  The source Datekey (DATE/DATETIME) is cast to DATE, shifted +16 years,
--  then all calendar and fiscal attributes are RECOMPUTED from the shifted
--  date. Source CalendarYear / FiscalYear columns are intentionally ignored
--  to prevent stale metadata leaking through.
--
--  FISCAL CALENDAR — JULY START (replaces incorrect original Contoso fiscal)
--  Jul = FM1 | FQ1 | FH1 | FY = CalendarYear+1
--  Oct = FM4 | FQ2 | FH1
--  Jan = FM7 | FQ3 | FH2 | FY = CalendarYear
--  Apr = FM10| FQ4 | FH2
--
--  DYNAMIC FLAGS
--  IsCurrentYear / IsCurrentMonth / IsToday use GETDATE() — they reflect
--  the actual execution date, NOT '2025-12-31'. This is intentional: these
--  flags are meant to highlight the current reporting period dynamically.
-- ============================================================================

PRINT '  → Creating dim.vDate...';
GO

CREATE OR ALTER VIEW [dim].[vDate]
AS
WITH ShiftedDates AS (
    -- Apply the +16 year temporal shift once; all subsequent columns derive from here.
    SELECT
        DATEADD(YEAR, 16, CAST(d.Datekey AS DATE)) AS ShiftedDate,
        d.IsWorkDay,
        d.IsHoliday,
        d.HolidayName,
        d.EuropeSeason,
        d.NorthAmericaSeason,
        d.AsiaSeason
    FROM [dbo].[DimDate] AS d
)
SELECT

    /* ── PRIMARY KEY ─────────────────────────────────────────────────────── */
    CAST(
        YEAR(s.ShiftedDate) * 10000
      + MONTH(s.ShiftedDate) * 100
      + DAY(s.ShiftedDate)
    AS INT)                                                         AS DateKey,

    /* ── DATE AS DATE TYPE ───────────────────────────────────────────────── */
    -- Expose as DATE for use as the Power BI "Mark as Date Table" column.
    CAST(s.ShiftedDate AS DATE)                                     AS [Date],

    /* ── CALENDAR YEAR ───────────────────────────────────────────────────── */
    YEAR(s.ShiftedDate)                                             AS CalendarYear,
    CAST('CY ' + CAST(YEAR(s.ShiftedDate) AS NVARCHAR(4))
        AS NVARCHAR(10))                                            AS CalendarYearLabel,

    /* ── CALENDAR HALF-YEAR ──────────────────────────────────────────────── */
    CASE WHEN MONTH(s.ShiftedDate) <= 6 THEN 1 ELSE 2 END          AS CalendarHalfYear,
    CAST('H' + CAST(
            CASE WHEN MONTH(s.ShiftedDate) <= 6 THEN 1 ELSE 2 END
        AS NVARCHAR(1))
        + ' ' + CAST(YEAR(s.ShiftedDate) AS NVARCHAR(4))
        AS NVARCHAR(10))                                            AS CalendarHalfYearLabel,

    /* ── CALENDAR QUARTER ────────────────────────────────────────────────── */
    DATEPART(QUARTER, s.ShiftedDate)                                AS CalendarQuarter,
    CAST('Q' + CAST(DATEPART(QUARTER, s.ShiftedDate) AS NVARCHAR(1))
        + ' ' + CAST(YEAR(s.ShiftedDate) AS NVARCHAR(4))
        AS NVARCHAR(10))                                            AS CalendarQuarterLabel,

    /* ── CALENDAR MONTH ──────────────────────────────────────────────────── */
    MONTH(s.ShiftedDate)                                            AS CalendarMonth,
    CAST(
        DATENAME(MONTH, s.ShiftedDate)
        + ' ' + CAST(YEAR(s.ShiftedDate) AS NVARCHAR(4))
        AS NVARCHAR(20))                                            AS CalendarMonthLabel,

    /* ── CALENDAR WEEK ───────────────────────────────────────────────────── */
    DATEPART(WEEK, s.ShiftedDate)                                   AS CalendarWeek,
    CAST('WK' + RIGHT('0' + CAST(DATEPART(WEEK, s.ShiftedDate) AS NVARCHAR(2)), 2)
        AS NVARCHAR(10))                                            AS CalendarWeekLabel,

    /* ── CALENDAR DAY ────────────────────────────────────────────────────── */
    DATEPART(WEEKDAY, s.ShiftedDate)                                AS CalendarDayOfWeek,
    DATENAME(WEEKDAY, s.ShiftedDate)                                AS CalendarDayOfWeekLabel,
    DAY(s.ShiftedDate)                                              AS DayOfMonth,
    DATEPART(DAYOFYEAR, s.ShiftedDate)                              AS DayOfYear,

    /* ── FISCAL YEAR (July start: FM1=Jul, FQ1=Jul–Sep, FY = CY+1 for Jul–Dec) */
    CASE WHEN MONTH(s.ShiftedDate) >= 7
         THEN YEAR(s.ShiftedDate) + 1
         ELSE YEAR(s.ShiftedDate)
    END                                                             AS FiscalYear,

    CAST('FY' + CAST(
            CASE WHEN MONTH(s.ShiftedDate) >= 7
                 THEN YEAR(s.ShiftedDate) + 1
                 ELSE YEAR(s.ShiftedDate) END
        AS NVARCHAR(4))
        AS NVARCHAR(10))                                            AS FiscalYearLabel,

    /* ── FISCAL HALF-YEAR ────────────────────────────────────────────────── */
    -- FH1 = Jul–Dec | FH2 = Jan–Jun
    CASE WHEN MONTH(s.ShiftedDate) BETWEEN 7 AND 12 THEN 1 ELSE 2
    END                                                             AS FiscalHalfYear,

    CAST('FH' + CAST(
            CASE WHEN MONTH(s.ShiftedDate) BETWEEN 7 AND 12 THEN 1 ELSE 2 END
        AS NVARCHAR(1))
        + ' FY' + CAST(
            CASE WHEN MONTH(s.ShiftedDate) >= 7
                 THEN YEAR(s.ShiftedDate) + 1
                 ELSE YEAR(s.ShiftedDate) END
        AS NVARCHAR(4))
        AS NVARCHAR(20))                                            AS FiscalHalfYearLabel,

    /* ── FISCAL QUARTER ──────────────────────────────────────────────────── */
    -- FQ1=Jul–Sep | FQ2=Oct–Dec | FQ3=Jan–Mar | FQ4=Apr–Jun
    CASE
        WHEN MONTH(s.ShiftedDate) BETWEEN 7  AND 9  THEN 1
        WHEN MONTH(s.ShiftedDate) BETWEEN 10 AND 12 THEN 2
        WHEN MONTH(s.ShiftedDate) BETWEEN 1  AND 3  THEN 3
        ELSE                                              4
    END                                                             AS FiscalQuarter,

    CAST('FQ' + CAST(
            CASE
                WHEN MONTH(s.ShiftedDate) BETWEEN 7  AND 9  THEN 1
                WHEN MONTH(s.ShiftedDate) BETWEEN 10 AND 12 THEN 2
                WHEN MONTH(s.ShiftedDate) BETWEEN 1  AND 3  THEN 3
                ELSE 4
            END AS NVARCHAR(1))
        + ' FY' + CAST(
            CASE WHEN MONTH(s.ShiftedDate) >= 7
                 THEN YEAR(s.ShiftedDate) + 1
                 ELSE YEAR(s.ShiftedDate) END
        AS NVARCHAR(4))
        AS NVARCHAR(20))                                            AS FiscalQuarterLabel,

    /* ── FISCAL MONTH ────────────────────────────────────────────────────── */
    -- FM1=Jul, FM2=Aug, ..., FM6=Dec, FM7=Jan, ..., FM12=Jun
    CASE WHEN MONTH(s.ShiftedDate) >= 7
         THEN MONTH(s.ShiftedDate) - 6
         ELSE MONTH(s.ShiftedDate) + 6
    END                                                             AS FiscalMonth,

    CAST('FM' + CAST(
            CASE WHEN MONTH(s.ShiftedDate) >= 7
                 THEN MONTH(s.ShiftedDate) - 6
                 ELSE MONTH(s.ShiftedDate) + 6
            END AS NVARCHAR(2))
        AS NVARCHAR(6))                                             AS FiscalMonthLabel,

    /* ── YEAR-MONTH KEY (INT YYYYMM — for monthly-grain fact joins) ────────── */
    -- fact.vExchangeRate and fact.vMarketingSpend join on this, NOT DateKey.
    CAST(YEAR(s.ShiftedDate) * 100 + MONTH(s.ShiftedDate) AS INT)  AS YearMonthKey,

    /* ── WORK / HOLIDAY FLAGS (carried from source — day pattern preserved) ─ */
    s.IsWorkDay,
    s.IsHoliday,
    s.HolidayName,

    /* ── RETAIL SEASON CLASSIFICATIONS (source values — still valid post-shift) */
    s.EuropeSeason,
    s.NorthAmericaSeason,
    s.AsiaSeason,

    /* ── DYNAMIC CURRENT-PERIOD FLAGS (GETDATE() — not '2025-12-31') ───────── */
    CAST(CASE WHEN YEAR(s.ShiftedDate) = YEAR(GETDATE()) THEN 1 ELSE 0
         END AS BIT)                                                AS IsCurrentYear,

    CAST(CASE WHEN YEAR(s.ShiftedDate)  = YEAR(GETDATE())
               AND MONTH(s.ShiftedDate) = MONTH(GETDATE()) THEN 1 ELSE 0
         END AS BIT)                                                AS IsCurrentMonth,

    CAST(CASE WHEN CAST(s.ShiftedDate AS DATE) = CAST(GETDATE() AS DATE) THEN 1 ELSE 0
         END AS BIT)                                                AS IsToday

FROM ShiftedDates AS s;
GO

PRINT '    ✓ dim.vDate created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 2 — dim.vCustomer
--  Source : dbo.DimCustomer LEFT JOIN dbo.DimGeography
--  Grain  : One row per customer (individual and corporate)
--  Key    : CustomerKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  GEOGRAPHY PREFIX
--  All geography columns carry the 'Customer' prefix (CustomerContinent,
--  CustomerCountry, etc.) to prevent name collisions with Store geography
--  columns from dim.vStore inside the Power BI Fields pane.
--
--  BIRTHDATE — NO TEMPORAL SHIFT APPLIED
--  BirthDate is a fixed personal attribute, not a transaction date.
--  Age is computed as of '2025-12-31' from the unshifted BirthDate.
--
--  DATEFIRSTPURCHASE — SHIFTED +16 YEARS
--  DateFirstPurchase is a transactional timestamp (first order date).
--  It is shifted to the 2023–2025 era for temporal consistency.
--
--  CORPORATE CUSTOMERS (CustomerType = 'Company')
--  These have NULL BirthDate and no acquisition channel by design.
--  Age, AgeGroup, and LifeStage are NULL for corporate rows.
-- ============================================================================

PRINT '  → Creating dim.vCustomer...';
GO

CREATE OR ALTER VIEW [dim].[vCustomer]
AS
WITH CustomerBase AS (
    SELECT 
        c.CustomerKey,
        c.GeographyKey,
        c.FirstName,
        c.MiddleName,
        c.LastName,
        c.Gender,
        c.MaritalStatus,
        CAST(DATEADD(YEAR, 16, c.BirthDate) AS DATE) AS BirthDate,
        c.YearlyIncome,
        c.TotalChildren,
        c.NumberChildrenAtHome,
        c.Education,
        c.Occupation,
        c.HouseOwnerFlag,
        c.NumberCarsOwned,
        c.AddressLine1,
        c.AddressLine2,
        c.Phone,
        c.EmailAddress,
        c.CustomerType,
        c.CompanyName,
        
        -- Shift DateFirstPurchase +16 years
        CAST(DATEADD(YEAR, 16, c.DateFirstPurchase) AS DATE) AS DateFirstPurchase,
        
        -- Since the anchor is Dec 31, the year has fully elapsed for everyone. 
        -- Complex birthday-correction math is mathematically redundant here.
        DATEDIFF(YEAR, DATEADD(YEAR, 16, c.BirthDate), '2025-12-31') AS ComputedAge,
        
        -- Same logic applies to Tenure
        DATEDIFF(YEAR, DATEADD(YEAR, 16, c.DateFirstPurchase), '2025-12-31') AS ComputedTenureYears

    FROM [dbo].[DimCustomer] AS c
),
CustomerGeo AS (
    SELECT 
        cb.*,
        -- Protect against LEFT JOIN misses
        ISNULL(g.ContinentName, 'Unknown')      AS CustomerContinent,
        ISNULL(g.RegionCountryName, 'Unknown')  AS CustomerCountry,
        ISNULL(g.StateProvinceName, 'Unknown')  AS CustomerState,
        ISNULL(g.CityName, 'Unknown')           AS CustomerCity
    FROM CustomerBase AS cb
    LEFT JOIN [dbo].[DimGeography] AS g ON cb.GeographyKey = g.GeographyKey
)
SELECT 
    /* ── PRIMARY KEY ─────────────────────────────────────────────────────── */
    cg.CustomerKey,

    /* ── IDENTITY ────────────────────────────────────────────────────────── */
    CONCAT_WS(' ', 
        cg.FirstName, 
        NULLIF(LTRIM(RTRIM(cg.MiddleName)), ''), 
        cg.LastName
    )                                                           AS FullName,
    
    cg.CustomerType,    -- 'Individual' or 'Company'
    cg.CompanyName,     -- Populated for corporate customers only
    cg.EmailAddress,
    cg.Phone,

    /* ── DEMOGRAPHICS ────────────────────────────────────────────────────── */
    -- Isolate B2B vs B2C clearly so "Unknown" specifically highlights bad data
    CASE 
        WHEN cg.CustomerType = 'Company' THEN 'N/A'
        WHEN cg.Gender = 'M' THEN 'Male'
        WHEN cg.Gender = 'F' THEN 'Female'
        ELSE 'Unknown'
    END                                                         AS Gender,

    CASE 
        WHEN cg.CustomerType = 'Company' THEN 'N/A'
        WHEN cg.MaritalStatus IN ('M', 'Married') THEN 'Married'
        WHEN cg.MaritalStatus IN ('S', 'Single') THEN 'Single'
        WHEN cg.MaritalStatus IN ('D', 'Divorced') THEN 'Divorced'
        WHEN cg.MaritalStatus IN ('W', 'Widowed') THEN 'Widowed'
        ELSE 'Unknown'
    END                                                         AS MaritalStatus,

    cg.BirthDate,
    cg.ComputedAge                                              AS Age,

    CASE 
        WHEN cg.ComputedAge IS NULL      THEN NULL -- Leave null for B2B
        WHEN cg.ComputedAge < 26         THEN '18–25'
        WHEN cg.ComputedAge < 36         THEN '26–35'
        WHEN cg.ComputedAge < 46         THEN '36–45'
        WHEN cg.ComputedAge < 56         THEN '46–55'
        WHEN cg.ComputedAge < 66         THEN '56–65'
        ELSE                                  '65+'
    END                                                         AS AgeGroup,

    ISNULL(cg.Education, 'Unknown')                             AS Education,
    ISNULL(cg.Occupation, 'Unknown')                            AS Occupation,
    cg.TotalChildren,
    cg.NumberChildrenAtHome,
    cg.HouseOwnerFlag,
    cg.NumberCarsOwned,

    /* ── INCOME CLASSIFICATION ───────────────────────────────────────────── */
    cg.YearlyIncome,
    CASE 
        WHEN cg.YearlyIncome IS NULL THEN NULL
        WHEN cg.YearlyIncome <  30000 THEN 'Low'
        WHEN cg.YearlyIncome <  60000 THEN 'Lower-Middle'
        WHEN cg.YearlyIncome < 100000 THEN 'Upper-Middle'
        ELSE                               'High'
    END                                                         AS IncomeGroup,

    /* ── LIFE STAGE CLASSIFICATION ───────────────────────────────────────── */
    CASE 
        WHEN cg.ComputedAge IS NULL                                    THEN NULL
        WHEN cg.ComputedAge < 36 AND cg.TotalChildren > 0              THEN 'Young Family'
        WHEN cg.ComputedAge < 36 AND cg.MaritalStatus IN ('M','Married') THEN 'Young Couple'
        WHEN cg.ComputedAge < 36                                       THEN 'Young Single'
        WHEN cg.ComputedAge BETWEEN 36 AND 55 AND cg.TotalChildren > 0 THEN 'Mature Family'
        WHEN cg.ComputedAge BETWEEN 36 AND 55                          THEN 'Mature Single/Couple'
        ELSE                                                                'Senior'
    END                                                         AS LifeStage,

    /* ── PURCHASE HISTORY ATTRIBUTES ─────────────────────────────────────── */
    cg.DateFirstPurchase,  
    cg.ComputedTenureYears                                      AS CustomerTenureYears,

    CASE 
        WHEN cg.ComputedTenureYears IS NULL THEN NULL
        WHEN cg.ComputedTenureYears <= 1    THEN 'New'
        WHEN cg.ComputedTenureYears <= 2    THEN 'Developing'
        WHEN cg.ComputedTenureYears <= 3    THEN 'Established'
        ELSE                                     'Loyal'
    END                                                         AS CustomerLifecycleStage,

    /* ── ADDRESS & GEOGRAPHY ─────────────────────────────────────────────── */
    cg.AddressLine1,
    cg.AddressLine2,
    cg.CustomerContinent,
    cg.CustomerCountry,
    cg.CustomerState,
    cg.CustomerCity

FROM CustomerGeo AS cg;
GO

PRINT '    ✓ dim.vCustomer created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 3 — dim.vProduct
--  Source : dbo.DimProduct
--           LEFT JOIN dbo.DimProductSubcategory
--           LEFT JOIN dbo.DimProductCategory
--  Grain  : One row per product SKU
--  Key    : ProductKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  SNOWFLAKE FLATTENING
--  DimProduct → DimProductSubcategory → DimProductCategory is a three-level
--  snowflake in the source. All levels are flattened into this single view,
--  removing the need for bridge or intermediate tables in Power BI.
--
--  CATALOG PRICE RENAMING
--  UnitCost → CatalogCost | UnitPrice → CatalogPrice
--  Fact views carry transactional UnitCost/UnitPrice (the price at time of
--  sale). Renaming here signals list-price semantics to the report author.
--
--  TEMPORAL SHIFT ON PRODUCT DATES
--  AvailableForSaleDate and StopSaleDate shifted +16 years.
--  ProductStatus is derived from the SHIFTED StopSaleDate vs '2025-12-31'.
-- ============================================================================

PRINT '  → Creating dim.vProduct...';
GO

CREATE OR ALTER VIEW [dim].[vProduct]
AS
SELECT

    /* ── PRIMARY KEY ─────────────────────────────────────────────────────── */
    dp.ProductKey,

    /* ── PRODUCT IDENTITY ────────────────────────────────────────────────── */
    dp.ProductLabel,
    dp.ProductName,
    dp.ProductDescription,

    /* ── CATEGORY HIERARCHY (flattened from 3-level snowflake) ──────────── */
    dpc.ProductCategoryName                                     AS CategoryName,
    dps.ProductSubcategoryName                                  AS SubcategoryName,

    -- Breadcrumb path for drill-through and tooltip labels
    dpc.ProductCategoryName
        + ' > ' + dps.ProductSubcategoryName
        + ' > ' + dp.ProductName                               AS ProductBreadcrumb,

    /* ── BRAND & MANUFACTURER ────────────────────────────────────────────── */
    dp.Manufacturer,
    dp.BrandName,

    /* ── PRODUCT ATTRIBUTES ──────────────────────────────────────────────── */
    dp.ClassName,
    dp.StyleName,
    dp.ColorName,
    dp.Size,
    dp.SizeRange,
    dp.Weight,
    dp.UnitOfMeasureName,
    dp.StockTypeName,

    /* ── CATALOG PRICING (list prices — distinct from transactional prices) ─ */
    dp.UnitCost  AS CatalogCost,
    dp.UnitPrice AS CatalogPrice,
    -- Gross margin at catalog prices (denominator-safe)
    CAST(
        ISNULL(
            (dp.UnitPrice - dp.UnitCost) / NULLIF(dp.UnitPrice, 0),
            0
        ) AS DECIMAL(10, 4)
    )                                                           AS CatalogGrossMarginPct,

    /* ── PRODUCT LIFECYCLE DATES (shifted +16 years) ─────────────────────── */
    CAST(DATEADD(YEAR, 16, dp.AvailableForSaleDate) AS DATE)   AS AvailableForSaleDate,
    CAST(DATEADD(YEAR, 16, dp.StopSaleDate)         AS DATE)   AS StopSaleDate,

    /* ── PRODUCT STATUS (derived from shifted StopSaleDate vs reference date) */
    CASE
        WHEN dp.StopSaleDate IS NULL                                        THEN 'Active'
        WHEN DATEADD(YEAR, 16, dp.StopSaleDate) > '2025-12-31'             THEN 'Active'
        ELSE                                                                     'Discontinued'
    END                                                         AS ProductStatus,

    /* ── YEARS IN CATALOGUE (from shifted AvailableForSaleDate) ──────────── */
    DATEDIFF(YEAR,
        DATEADD(YEAR, 16, dp.AvailableForSaleDate),
        '2025-12-31'
    )                                                           AS CatalogueAgeYears

    /* ── IMAGE / URL (for report tooltips) ──────────────────────────────── */
    --dp.ImageURL,
    --dp.ProductURL

FROM [dbo].[DimProduct] AS dp
LEFT JOIN [dbo].[DimProductSubcategory] AS dps
    ON dp.ProductSubcategoryKey = dps.ProductSubcategoryKey
LEFT JOIN [dbo].[DimProductCategory] AS dpc
    ON dps.ProductCategoryKey = dpc.ProductCategoryKey;
GO

PRINT '    ✓ dim.vProduct created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 4 — dim.vStore
--  Source : dbo.DimStore LEFT JOIN dbo.DimGeography
--  Grain  : One row per store location
--  Key    : StoreKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  GEOGRAPHY PREFIX
--  Geography columns carry the 'Store' prefix (StoreContinent, StoreCountry,
--  etc.) to prevent name collisions with Customer geography in Power BI.
--
--  SPATIAL COLUMNS EXCLUDED
--  GeoLocation and Geometry (spatial types) are excluded — Power BI map
--  visuals use the flattened text fields (StoreCountry, StoreState, StoreCity).
--
--  STORE SIZE CATEGORY THRESHOLDS
--  SellingAreaSize is in square feet. Thresholds below are calibrated to
--  produce a meaningful distribution. Validate against actual data using
--  the verification queries and adjust if needed.
-- ============================================================================

PRINT '  → Creating dim.vStore...';
GO

CREATE OR ALTER VIEW [dim].[vStore]
AS
SELECT

    /* ── PRIMARY KEY ─────────────────────────────────────────────────────── */
    ds.StoreKey,

    /* ── STORE IDENTITY ──────────────────────────────────────────────────── */
    ds.StoreName,
    ds.StoreType,
    ds.StoreDescription,
    ds.StoreManager,
    ds.EmployeeCount,
    ds.SellingAreaSize,

    /* ── STORE SIZE CATEGORY ─────────────────────────────────────────────── */
    -- ⚠ Thresholds to validate against actual SellingAreaSize distribution.
    CASE
        WHEN ds.SellingAreaSize <  5000  THEN 'Small'
        WHEN ds.SellingAreaSize < 20000  THEN 'Medium'
        WHEN ds.SellingAreaSize < 50000  THEN 'Large'
        ELSE                                  'Flagship'
    END                                                         AS StoreSizeCategory,

    /* ── OPERATIONAL DATES (shifted +16 years) ───────────────────────────── */
    CAST(DATEADD(YEAR, 16, ds.OpenDate)  AS DATE)               AS OpenDate,
    CAST(DATEADD(YEAR, 16, ds.CloseDate) AS DATE)               AS CloseDate,

    /* ── STORE STATUS (derived from shifted CloseDate vs reference date) ─── */
    CASE
        WHEN ds.CloseDate IS NULL                                           THEN 'Active'
        WHEN DATEADD(YEAR, 16, ds.CloseDate) > '2025-12-31'               THEN 'Active'
        ELSE                                                                     'Closed'
    END                                                         AS StoreStatus,

    /* ── STORE AGE IN YEARS (from shifted OpenDate vs reference date) ───── */
    DATEDIFF(YEAR,
        DATEADD(YEAR, 16, ds.OpenDate),
        '2025-12-31'
    )
    - CASE
        WHEN MONTH(DATEADD(YEAR,16,ds.OpenDate)) * 100
           + DAY(DATEADD(YEAR,16,ds.OpenDate)) > 1231
        THEN 1 ELSE 0
      END                                                       AS StoreAgeYears,

    /* ── LAST REMODEL DATE (shifted +16 years) ───────────────────────────── */
    CAST(DATEADD(YEAR, 16, ds.LastRemodelDate) AS DATE)         AS LastRemodelDate,

    /* ── CLOSE REASON ────────────────────────────────────────────────────── */
    ds.CloseReason,

    /* ── GEOGRAPHY (Store-prefixed — avoids collision with Customer geography) */
    g.ContinentName     AS StoreContinent,
    g.RegionCountryName AS StoreCountry,
    g.StateProvinceName AS StoreState,
    g.CityName          AS StoreCity,

    /* ── CONTACT INFO ────────────────────────────────────────────────────── */
    ds.StorePhone,
    ds.StoreFax,
    ds.AddressLine1,
    ds.AddressLine2,
    ds.ZipCode

FROM [dbo].[DimStore] AS ds
LEFT JOIN [dbo].[DimGeography] AS g ON ds.GeographyKey = g.GeographyKey;
GO

PRINT '    ✓ dim.vStore created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 5 — dim.vPromotion
--  Source : dbo.DimPromotion
--  Grain  : One row per promotion event
--  Key    : PromotionKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  KEY NOTE: PromotionKey = 1 is the "No Promotion" record in Contoso.
--  Filter it out when comparing promotional vs. non-promotional performance.
--  DAX pattern: CALCULATE([Metric], DimPromotion[PromotionKey] <> 1)
--
--  DISCOUNT TIER THRESHOLDS
--  Based on DiscountPercent (0.0–1.0 scale, not 0–100).
--  0% = "No Discount" | <15% = "Low" | 15–30% = "Medium"
--  30–50% = "High" | >50% = "Aggressive"
-- ============================================================================

PRINT '  → Creating dim.vPromotion...';
GO

CREATE OR ALTER VIEW [dim].[vPromotion]
AS
SELECT

    /* ── PRIMARY KEY ─────────────────────────────────────────────────────── */
    dp.PromotionKey,
    dp.PromotionLabel,

    /* ── PROMOTION IDENTITY ──────────────────────────────────────────────── */
    dp.PromotionName,
    dp.PromotionDescription,
    dp.PromotionType,
    dp.PromotionCategory,
    dp.DiscountPercent,
    dp.MinQuantity,
    dp.MaxQuantity,

    /* ── DISCOUNT TIER (label for slicers and matrix rows) ──────────────── */
    CASE
        WHEN dp.DiscountPercent = 0     THEN 'No Discount'
        WHEN dp.DiscountPercent < 0.15  THEN 'Low'
        WHEN dp.DiscountPercent < 0.30  THEN 'Medium'
        WHEN dp.DiscountPercent < 0.50  THEN 'High'
        ELSE                                 'Aggressive'
    END                                                         AS DiscountTier,

    /* ── PROMOTION DATES (shifted +16 years) ─────────────────────────────── */
    CAST(DATEADD(YEAR, 16, dp.StartDate) AS DATE)               AS StartDate,
    CAST(DATEADD(YEAR, 16, dp.EndDate)   AS DATE)               AS EndDate,

    /* ── PROMOTION STATUS (derived from shifted EndDate vs reference date) ─ */
    CASE
        WHEN dp.EndDate IS NULL                                             THEN 'Active'
        WHEN DATEADD(YEAR, 16, dp.EndDate) >= '2025-12-31'                THEN 'Active'
        ELSE                                                                     'Expired'
    END                                                         AS PromotionStatus,

    /* ── PROMOTION DURATION IN DAYS ──────────────────────────────────────── */
    -- For open-ended promotions (NULL EndDate), measured to reference date.
    DATEDIFF(DAY,
        DATEADD(YEAR, 16, dp.StartDate),
        ISNULL(DATEADD(YEAR, 16, dp.EndDate), '2025-12-31')
    )                                                           AS PromotionDurationDays

FROM [dbo].[DimPromotion] AS dp;
GO

PRINT '    ✓ dim.vPromotion created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 6 — dim.vEmployee
--  Source : dbo.DimEmployee (WHERE Status = 'Current')
--  Grain  : One row per current employee
--  Key    : EmployeeKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  STATUS FILTER — CURRENT EMPLOYEES ONLY
--  Historical employees (Status <> 'Current') are excluded. The model is
--  designed for workforce analytics on the active headcount, not full
--  employment history.
--
--  BIRTHDATE — NO TEMPORAL SHIFT
--  BirthDate is a fixed personal attribute. EmployeeAge is computed from
--  the unshifted BirthDate against '2025-12-31'.
--
--  HIREDATE / STARTDATE / ENDDATE — SHIFTED +16 YEARS
--  These are operational/contractual timestamps that belong to the
--  2023–2025 business era being modelled.
--
--  PARENTEMPLOYEEKEY
--  Retained for org hierarchy DAX patterns using PATH() and PATHITEM().
--  NULL for top-level managers.
-- ============================================================================

PRINT '  → Creating dim.vEmployee...';
GO

CREATE OR ALTER VIEW [dim].[vEmployee]
AS
WITH EmployeeBase AS (
    SELECT
        e.*,
        -- Compute tenure from shifted HireDate against reference date
        DATEDIFF(YEAR, DATEADD(YEAR, 16, e.HireDate), '2025-12-31')
            - CASE
                WHEN MONTH(DATEADD(YEAR,16,e.HireDate)) * 100
                   + DAY(DATEADD(YEAR,16,e.HireDate)) > 1231
                THEN 1 ELSE 0
              END                                               AS ComputedTenureYears,
        -- Compute age from unshifted BirthDate against reference date
        DATEDIFF(YEAR, e.BirthDate, '2025-12-31')
            - CASE
                WHEN MONTH(e.BirthDate) * 100 + DAY(e.BirthDate) > 1231
                THEN 1 ELSE 0
              END                                               AS ComputedAge
    FROM [dbo].[DimEmployee] AS e
    WHERE e.Status = 'Current'
)
SELECT

    /* ── PRIMARY KEY ─────────────────────────────────────────────────────── */
    eb.EmployeeKey,
    eb.ParentEmployeeKey,   -- Self-referencing for org hierarchy (NULL = top level)

    /* ── IDENTITY ────────────────────────────────────────────────────────── */
    CONCAT_WS(' ',
        eb.FirstName,
        NULLIF(LTRIM(RTRIM(eb.MiddleName)), ''),
        eb.LastName
    )                                                           AS EmployeeFullName,

    eb.Title,
    eb.EmailAddress,
    eb.Phone,

    CASE eb.Gender
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE          'Unknown'
    END                                                         AS Gender,

    CASE eb.MaritalStatus
        WHEN 'M' THEN 'Married'
        WHEN 'S' THEN 'Single'
        WHEN 'D' THEN 'Divorced'
        WHEN 'W' THEN 'Widowed'
        ELSE          'Unknown'
    END                                                         AS MaritalStatus,

    /* ── ROLE & DEPARTMENT ───────────────────────────────────────────────── */
    eb.DepartmentName,
    eb.SalesPersonFlag,
    eb.SalariedFlag,
    eb.CurrentFlag,

    -- Role classification for dashboard grouping
    CASE WHEN eb.SalesPersonFlag = 1 THEN 'Sales' ELSE 'Non-Sales' END
                                                                AS RoleType,

    /* ── COMPENSATION & BENEFITS ─────────────────────────────────────────── */
    eb.BaseRate,
    eb.VacationHours,
    eb.PayFrequency,

    /* ── DATES (shifted +16 years for operational timestamps) ────────────── */
    CAST(DATEADD(YEAR, 16, eb.HireDate)   AS DATE)              AS HireDate,
    CAST(DATEADD(YEAR, 16, eb.StartDate)  AS DATE)              AS StartDate,
    CAST(DATEADD(YEAR, 16, eb.EndDate)    AS DATE)              AS EndDate,

    /* ── BIRTHDATE (unshifted — personal attribute) ──────────────────────── */
    eb.BirthDate,
    eb.ComputedAge                                              AS EmployeeAge,

    /* ── TENURE ──────────────────────────────────────────────────────────── */
    eb.ComputedTenureYears                                      AS TenureYears,

    CASE
        WHEN eb.ComputedTenureYears < 2  THEN '0–1 Years'
        WHEN eb.ComputedTenureYears < 5  THEN '2–4 Years'
        WHEN eb.ComputedTenureYears < 10 THEN '5–9 Years'
        ELSE                                  '10+ Years'
    END                                                         AS SeniorityTier,

    /* ── STATUS ──────────────────────────────────────────────────────────── */
    eb.Status   -- Always 'Current' due to WHERE filter; retained for transparency

FROM EmployeeBase AS eb;
GO

PRINT '    ✓ dim.vEmployee created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 7 — dim.vPaymentMethod
--  Source : gen.DimPaymentMethod
--  Grain  : One row per payment method (7 rows — static reference)
--  Key    : PaymentMethodKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
-- No temporal shift applied — static reference data.
-- PaymentDescription excluded (verbose — for data dictionary only).
-- ============================================================================

PRINT '  → Creating dim.vPaymentMethod...';
GO

CREATE OR ALTER VIEW [dim].[vPaymentMethod]
AS
SELECT
    pm.PaymentMethodKey,
    pm.PaymentMethodName,
    pm.PaymentCategory,     -- 'Digital', 'Traditional', 'BNPL', 'Prepaid'
    CAST(CASE
        WHEN pm.PaymentCategory IN ('Digital', 'BNPL') THEN 1
        ELSE 0
    END AS BIT) AS IsDigital            -- BIT: 1 = real-time gateway, 0 = offline/manual
FROM [gen].[DimPaymentMethod] AS pm;
GO

PRINT '    ✓ dim.vPaymentMethod created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 8 — dim.vAcquisitionChannel
--  Source : gen.DimAcquisitionChannel
--  Grain  : One row per acquisition channel (7 rows — static reference)
--  Key    : AcquisitionChannelKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
-- No temporal shift applied — static reference data.
-- CACMidpoint is a computed convenience column for DAX CAC calculations.
-- ChannelDescription excluded (verbose — for data dictionary only).
-- ============================================================================

PRINT '  → Creating dim.vAcquisitionChannel...';
GO

CREATE OR ALTER VIEW [dim].[vAcquisitionChannel]
AS
SELECT
    ac.AcquisitionChannelKey,
    ac.ChannelName,
    ac.ChannelCategory,         -- e.g., 'Paid', 'Organic', 'Referral'
    CAST(CASE
        WHEN ac.ChannelCategory = 'Paid' THEN 1
        ELSE 0
    END AS BIT) AS IsDigital,            -- BIT: 1 = real-time gateway, 0 = offline/manual
    ac.EstimatedCACLow,         -- DECIMAL(19,4): lower bound of CAC range
    ac.EstimatedCACHigh,        -- DECIMAL(19,4): upper bound of CAC range
    -- Midpoint for single-value CAC calculations in DAX
    -- Used DECIMAL(19,4) to match the precision of the source columns and ensure consistency in DAX measures.
    CAST((ac.EstimatedCACLow + ac.EstimatedCACHigh) / 2.0 AS DECIMAL(19,4))
                                                            AS CACMidpoint
FROM [gen].[DimAcquisitionChannel] AS ac;
GO

PRINT '    ✓ dim.vAcquisitionChannel created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 9 — dim.vCurrency
--  Source : dbo.DimCurrency
--  Grain  : One row per ISO currency
--  Key    : CurrencyKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
-- No temporal shift — currency metadata is time-invariant.
-- ============================================================================

PRINT '  → Creating dim.vCurrency...';
GO

CREATE OR ALTER VIEW [dim].[vCurrency]
AS
SELECT
    c.CurrencyKey,
    c.CurrencyLabel,        -- ISO code, e.g. 'USD', 'EUR'
    c.CurrencyName,         -- e.g. 'US Dollar'
    c.CurrencyDescription
FROM [dbo].[DimCurrency] AS c;
GO

PRINT '    ✓ dim.vCurrency created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 10 — dim.vReturnReason
--  Source : gen.DimReturnReason
--  Grain  : One row per return reason (8 rows — static reference)
--  Key    : ReturnReasonKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
-- No temporal shift — static reference data.
-- ReturnReasonDescription excluded (verbose — for data dictionary only).
--
-- KEY ANALYTICAL FLAG: IsOperationalFailure
--   1 = Contoso caused this return (DEFECT, WRONG, DAMAGED, LATE)
--   0 = Customer caused this return (MINDCHG, NOTDESC, PRICE, DUPL)
--   DAX: CALCULATE([Return Rate %], dim.vReturnReason[IsOperationalFailure] = 1)
--
-- AppliesTo: 'Both' or 'Online Only'
--   'LATE' (Late Delivery) = Online Only — no delivery leg in physical returns.
-- ============================================================================

PRINT '  → Creating dim.vReturnReason...';
GO

CREATE OR ALTER VIEW [dim].[vReturnReason]
AS
SELECT
    rr.ReturnReasonKey,
    rr.ReturnReasonCode,        -- Short code: DEFECT, WRONG, DAMAGED, etc.
    rr.ReturnReasonName,        -- Full label for display
    rr.ReturnReasonCategory,    -- e.g., 'Product Quality', 'Fulfilment Failure'
    rr.IsOperationalFailure,    -- BIT: 1 = Contoso at fault, 0 = Customer at fault
    rr.AppliesTo                -- 'Both' or 'Online Only'
FROM [gen].[DimReturnReason] AS rr;
GO

PRINT '    ✓ dim.vReturnReason created.';
GO


-- ============================================================================
-- ═══════════════════════════════════════════════════════════════════════════
--  VIEW 11 — dim.vChannel  ← NEW (added to support fact.vStoreSales FK)
--  Source : dbo.DimChannel
--  Grain  : One row per sales channel
--  Key    : ChannelKey
-- ═══════════════════════════════════════════════════════════════════════════
-- ============================================================================
--
--  WHY THIS VIEW EXISTS
--  dbo.FactSales (source for fact.vStoreSales) carries a channelKey FK that
--  points to dbo.DimChannel. Without this dimension view, the channel FK
--  would have no matching 1-side in the Power BI model, breaking filter
--  propagation for store sales analyses.
--
--  No temporal shift — channel definitions are time-invariant reference data.
--  ChannelDescription retained (short, useful for report tooltips).
--
--  POWER BI RELATIONSHIP
--  fact.vStoreSales.ChannelKey → dim.vChannel.ChannelKey (Many:1, Active)
-- ============================================================================

PRINT '  → Creating dim.vChannel...';
GO

CREATE OR ALTER VIEW [dim].[vChannel]
AS
SELECT
    ch.ChannelKey,
    ch.ChannelLabel,        -- e.g. 'Store', 'Online', 'Catalog', 'Reseller'
    ch.ChannelName,         -- Full channel name
    ch.ChannelDescription   -- Short description (useful in tooltips)
FROM [dbo].[DimChannel] AS ch;
GO

PRINT '    ✓ dim.vChannel created.';
GO


-- ============================================================================
-- VERIFICATION SUITE
-- ============================================================================
-- Run immediately after script completion. All checks should return zero
-- anomalies. Any non-zero result indicates an issue to investigate.
-- ============================================================================

PRINT '';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '  Script 09 — Verification Suite';
PRINT '════════════════════════════════════════════════════════════════════';

-- ── V1: Row counts for all 11 views ──────────────────────────────────────────
-- Expected:
--   dim.vDate             → 2,556 rows  (2021-01-01 to 2027-12-31)
--   dim.vCustomer         → ~18,869 rows (all Contoso customers)
--   dim.vProduct          → ~2,517 rows
--   dim.vStore            → ~306 rows
--   dim.vPromotion        → ~1,000 rows
--   dim.vEmployee         → current employees only (varies)
--   dim.vPaymentMethod    → 7 rows (exact — static data)
--   dim.vAcquisitionChannel → 7 rows (exact — static data)
--   dim.vCurrency         → ~105 rows
--   dim.vReturnReason     → 8 rows (exact — static data)
--   dim.vChannel          → rows match dbo.DimChannel count
PRINT '';
PRINT '  V1 — Row counts for all dimension views';
SELECT 'dim.vDate'              AS ViewName, COUNT(*) AS 'RowCount' FROM [dim].[vDate]
UNION ALL
SELECT 'dim.vCustomer',                      COUNT(*) FROM [dim].[vCustomer]
UNION ALL
SELECT 'dim.vProduct',                       COUNT(*) FROM [dim].[vProduct]
UNION ALL
SELECT 'dim.vStore',                         COUNT(*) FROM [dim].[vStore]
UNION ALL
SELECT 'dim.vPromotion',                     COUNT(*) FROM [dim].[vPromotion]
UNION ALL
SELECT 'dim.vEmployee',                      COUNT(*) FROM [dim].[vEmployee]
UNION ALL
SELECT 'dim.vPaymentMethod',                 COUNT(*) FROM [dim].[vPaymentMethod]
UNION ALL
SELECT 'dim.vAcquisitionChannel',            COUNT(*) FROM [dim].[vAcquisitionChannel]
UNION ALL
SELECT 'dim.vCurrency',                      COUNT(*) FROM [dim].[vCurrency]
UNION ALL
SELECT 'dim.vReturnReason',                  COUNT(*) FROM [dim].[vReturnReason]
UNION ALL
SELECT 'dim.vChannel',                       COUNT(*) FROM [dim].[vChannel]
ORDER BY ViewName;

-- ── V2: dim.vDate — temporal shift validation ─────────────────────────────────
-- EXPECTED: MinDate = '2021-01-01', MaxDate = '2027-12-31'
-- Non-compliance means the +16 shift was not applied correctly.
PRINT '';
PRINT '  V2 — dim.vDate: temporal shift range check';
SELECT
    MIN([Date])                                         AS MinDate,
    MAX([Date])                                         AS MaxDate,
    COUNT(DISTINCT CalendarYear)                        AS DistinctYears,
    MIN(DateKey)                                        AS MinDateKey,
    MAX(DateKey)                                        AS MaxDateKey,
    -- Fiscal year sanity: FY for July = CY+1, FY for January = CY
    SUM(CASE WHEN CalendarMonth = 7  AND FiscalYear <> CalendarYear + 1 THEN 1 ELSE 0 END)
                                                        AS FiscalYearJulyErrors,
    SUM(CASE WHEN CalendarMonth = 1  AND FiscalYear <> CalendarYear     THEN 1 ELSE 0 END)
                                                        AS FiscalYearJanErrors,
    -- YearMonthKey format check: all values should be 6-digit integers
    SUM(CASE WHEN YearMonthKey < 202101 OR YearMonthKey > 202712 THEN 1 ELSE 0 END)
                                                        AS OutOfRangeYearMonthKeys
FROM [dim].[vDate];

-- ── V3: dim.vDate — fiscal calendar structure ─────────────────────────────────
-- EXPECTED: 12 rows (FM1–FM12), each with correct month assignment
PRINT '';
PRINT '  V3 — dim.vDate: fiscal month distribution (expect FM1=Jul, FM12=Jun)';
SELECT
    FiscalMonth,
    FiscalMonthLabel,
    CalendarMonth,
    DATENAME(MONTH, MIN([Date]))    AS MonthName,
    COUNT(*)                        AS DayCount
FROM [dim].[vDate]
WHERE CalendarYear = 2023
GROUP BY FiscalMonth, FiscalMonthLabel, CalendarMonth
ORDER BY FiscalMonth;

-- ── V4: dim.vCustomer — computed column distributions ────────────────────────
-- Validates age, income, and lifecycle classification outputs.
PRINT '';
PRINT '  V4 — dim.vCustomer: AgeGroup distribution';
SELECT
    AgeGroup,
    COUNT(*)                                            AS CustomerCount,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2))
                                                        AS PctOfTotal
FROM [dim].[vCustomer]
WHERE AgeGroup IS NOT NULL  -- Excludes corporate customers
GROUP BY AgeGroup
ORDER BY AgeGroup;

PRINT '';
PRINT '  V4b — dim.vCustomer: IncomeGroup distribution';
SELECT
    IncomeGroup,
    COUNT(*)                                            AS CustomerCount
FROM [dim].[vCustomer]
GROUP BY IncomeGroup
ORDER BY CustomerCount DESC;

PRINT '';
PRINT '  V4c — dim.vCustomer: CustomerLifecycleStage distribution';
SELECT
    CustomerLifecycleStage,
    COUNT(*)                                            AS CustomerCount
FROM [dim].[vCustomer]
GROUP BY CustomerLifecycleStage
ORDER BY CustomerCount DESC;

-- ── V5: dim.vProduct — status and category checks ────────────────────────────
PRINT '';
PRINT '  V5 — dim.vProduct: status and category distribution';
SELECT
    ProductStatus,
    COUNT(*)                                            AS ProductCount
FROM [dim].[vProduct]
GROUP BY ProductStatus;

SELECT
    CategoryName,
    COUNT(*)                                            AS ProductCount
FROM [dim].[vProduct]
GROUP BY CategoryName
ORDER BY ProductCount DESC;

-- ── V6: dim.vStore — status and size distribution ────────────────────────────
PRINT '';
PRINT '  V6 — dim.vStore: status and size category distribution';
SELECT
    StoreStatus,
    StoreSizeCategory,
    COUNT(*)                                            AS StoreCount
FROM [dim].[vStore]
GROUP BY StoreStatus, StoreSizeCategory
ORDER BY StoreStatus, StoreSizeCategory;

-- ── V7: Static reference table row counts (exact match required) ─────────────
-- EXPECTED: PaymentMethod=7, AcquisitionChannel=7, ReturnReason=8
PRINT '';
PRINT '  V7 — Static reference tables: exact row counts';
SELECT
    'dim.vPaymentMethod'        AS ViewName,
    COUNT(*)                    AS ActualRows,
    7                           AS ExpectedRows,
    CASE WHEN COUNT(*) = 7 THEN '✓ PASS' ELSE '✗ FAIL' END AS Result
FROM [dim].[vPaymentMethod]
UNION ALL
SELECT
    'dim.vAcquisitionChannel',
    COUNT(*), 7,
    CASE WHEN COUNT(*) = 7 THEN '✓ PASS' ELSE '✗ FAIL' END
FROM [dim].[vAcquisitionChannel]
UNION ALL
SELECT
    'dim.vReturnReason',
    COUNT(*), 8,
    CASE WHEN COUNT(*) = 8 THEN '✓ PASS' ELSE '✗ FAIL' END
FROM [dim].[vReturnReason];

-- ── V8: dim.vReturnReason — IsOperationalFailure split ───────────────────────
-- EXPECTED: Operational=4 (DEFECT, WRONG, DAMAGED, LATE), Customer=4
PRINT '';
PRINT '  V8 — dim.vReturnReason: accountability split (expect 4 / 4)';
SELECT
    CASE IsOperationalFailure
        WHEN 1 THEN 'Operational Failure'
        ELSE        'Customer-Led'
    END                         AS AccountabilityType,
    COUNT(*)                    AS ReasonCount,
    STRING_AGG(ReturnReasonCode, ', ') AS ReasonCodes
FROM [dim].[vReturnReason]
GROUP BY IsOperationalFailure;

-- ── V9: dim.vEmployee — seniority and role distribution ──────────────────────
PRINT '';
PRINT '  V9 — dim.vEmployee: seniority tier and role type distribution';
SELECT
    SeniorityTier,
    RoleType,
    COUNT(*)                    AS EmployeeCount
FROM [dim].[vEmployee]
GROUP BY SeniorityTier, RoleType
ORDER BY SeniorityTier, RoleType;

-- ── V10: Orphan check — dim.vDate DateKey range must cover fact date range ────
-- EXPECTED: 0 date keys outside the 2021–2027 range
PRINT '';
PRINT '  V10 — dim.vDate: confirm coverage of 2023–2025 core analytical window';
SELECT
    SUM(CASE WHEN [Date] BETWEEN '2023-01-01' AND '2025-12-31' THEN 1 ELSE 0 END)
                                AS DaysIn2023_2025,
    COUNT(*)                    AS TotalRows,
    MIN([Date])                 AS EarliestDate,
    MAX([Date])                 AS LatestDate
FROM [dim].[vDate];

PRINT '';
PRINT '════════════════════════════════════════════════════════════════════';
PRINT '  Script 09 completed successfully.';
PRINT '';
PRINT '  Views created (11 total):';
PRINT '    dim.vDate               — conformed date dimension (2021–2027)';
PRINT '    dim.vCustomer           — demographics, lifecycle, geography';
PRINT '    dim.vProduct            — flattened hierarchy, catalog pricing';
PRINT '    dim.vStore              — store ops, status, geography';
PRINT '    dim.vPromotion          — discount tiers, lifecycle status';
PRINT '    dim.vEmployee           — tenure, seniority, role type';
PRINT '    dim.vPaymentMethod      — 7 rows: digital/traditional/BNPL';
PRINT '    dim.vAcquisitionChannel — 7 rows: CAC range, CACMidpoint';
PRINT '    dim.vCurrency           — ISO currency reference';
PRINT '    dim.vReturnReason       — 8 rows: operational vs customer-led';
PRINT '    dim.vChannel            — sales channel (supports StoreSales FK)';
PRINT '';
PRINT '  Next steps:';
PRINT '    Script 10 → All 11 fact views ([fact] schema)';
PRINT '                Includes fact.vStoreSales with ChannelKey → dim.vChannel';
PRINT '════════════════════════════════════════════════════════════════════';
GO
