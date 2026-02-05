/*
    BudgetHeader - Budget version and scenario header
    Dependencies: FiscalPeriod
*/
CREATE TABLE Planning.BudgetHeader (
    BudgetHeaderID          INT IDENTITY(1,1) NOT NULL,
    BudgetCode              VARCHAR(30) NOT NULL,
    BudgetName              NVARCHAR(100) NOT NULL,
    BudgetType              VARCHAR(20) NOT NULL,  -- ANNUAL, QUARTERLY, ROLLING, FORECAST
    ScenarioType            VARCHAR(20) NOT NULL,  -- BASE, OPTIMISTIC, PESSIMISTIC, STRETCH
    FiscalYear              SMALLINT NOT NULL,
    StartPeriodID           INT NOT NULL,
    EndPeriodID             INT NOT NULL,
    BaseBudgetHeaderID      INT NULL,  -- For variance calculations
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    SubmittedByUserID       INT NULL,
    SubmittedDateTime       DATETIME2(7) NULL,
    ApprovedByUserID        INT NULL,
    ApprovedDateTime        DATETIME2(7) NULL,
    LockedDateTime          DATETIME2(7) NULL,
    IsLocked                AS CASE WHEN LockedDateTime IS NOT NULL THEN 1 ELSE 0 END PERSISTED,
    VersionNumber           INT NOT NULL DEFAULT 1,
    Notes                   NVARCHAR(MAX) NULL,
    -- XML column for flexible metadata - Snowflake handles XML differently (VARIANT)
    ExtendedProperties      XML NULL,
    CreatedDateTime         DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDateTime        DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_BudgetHeader PRIMARY KEY CLUSTERED (BudgetHeaderID),
    CONSTRAINT UQ_BudgetHeader_Code_Year UNIQUE (BudgetCode, FiscalYear, VersionNumber),
    CONSTRAINT FK_BudgetHeader_StartPeriod FOREIGN KEY (StartPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_EndPeriod FOREIGN KEY (EndPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_BaseBudget FOREIGN KEY (BaseBudgetHeaderID) 
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT CK_BudgetHeader_Status CHECK (StatusCode IN ('DRAFT','SUBMITTED','APPROVED','REJECTED','LOCKED','ARCHIVED'))
);
GO

-- XML index - No equivalent in Snowflake
CREATE PRIMARY XML INDEX PXML_BudgetHeader_ExtendedProperties
ON Planning.BudgetHeader (ExtendedProperties);
GO

CREATE XML INDEX SXML_BudgetHeader_ExtendedProperties_Path
ON Planning.BudgetHeader (ExtendedProperties)
USING XML INDEX PXML_BudgetHeader_ExtendedProperties
FOR PATH;
GO
