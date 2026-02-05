/*
    FiscalPeriod - Core reference table for fiscal calendar
    Dependencies: None (base table)
*/
CREATE TABLE Planning.FiscalPeriod (
    FiscalPeriodID          INT IDENTITY(1,1) NOT NULL,
    FiscalYear              SMALLINT NOT NULL,
    FiscalQuarter           TINYINT NOT NULL,
    FiscalMonth             TINYINT NOT NULL,
    PeriodName              NVARCHAR(50) NOT NULL,
    PeriodStartDate         DATE NOT NULL,
    PeriodEndDate           DATE NOT NULL,
    IsClosed                BIT NOT NULL DEFAULT 0,
    ClosedByUserID          INT NULL,
    ClosedDateTime          DATETIME2(7) NULL,
    IsAdjustmentPeriod      BIT NOT NULL DEFAULT 0,
    WorkingDays             TINYINT NULL,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    RowVersionStamp         ROWVERSION NOT NULL,  -- ROWVERSION doesn't exist in Snowflake
    CONSTRAINT PK_FiscalPeriod PRIMARY KEY CLUSTERED (FiscalPeriodID),
    CONSTRAINT UQ_FiscalPeriod_YearMonth UNIQUE (FiscalYear, FiscalMonth),
    CONSTRAINT CK_FiscalPeriod_Quarter CHECK (FiscalQuarter BETWEEN 1 AND 4),
    CONSTRAINT CK_FiscalPeriod_Month CHECK (FiscalMonth BETWEEN 1 AND 13), -- 13 for adjustment period
    CONSTRAINT CK_FiscalPeriod_DateRange CHECK (PeriodEndDate >= PeriodStartDate)
);
GO

-- Filtered index - Snowflake doesn't support filtered indexes
CREATE NONCLUSTERED INDEX IX_FiscalPeriod_OpenPeriods 
ON Planning.FiscalPeriod (FiscalYear, FiscalMonth)
WHERE IsClosed = 0;
GO

-- Include columns in index - different syntax in Snowflake
CREATE NONCLUSTERED INDEX IX_FiscalPeriod_Dates
ON Planning.FiscalPeriod (PeriodStartDate, PeriodEndDate)
INCLUDE (FiscalYear, FiscalQuarter, FiscalMonth);
GO
