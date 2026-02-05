/*
    BudgetLineItem - Individual budget amounts by account/cost center/period
    Dependencies: BudgetHeader, GLAccount, CostCenter, FiscalPeriod
*/
CREATE TABLE Planning.BudgetLineItem (
    BudgetLineItemID        BIGINT IDENTITY(1,1) NOT NULL,
    BudgetHeaderID          INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    -- Amounts in multiple representations
    OriginalAmount          DECIMAL(19,4) NOT NULL DEFAULT 0,
    AdjustedAmount          DECIMAL(19,4) NOT NULL DEFAULT 0,
    FinalAmount             AS (OriginalAmount + AdjustedAmount) PERSISTED,  -- Computed persisted
    LocalCurrencyAmount     DECIMAL(19,4) NULL,
    ReportingCurrencyAmount DECIMAL(19,4) NULL,
    StatisticalQuantity     DECIMAL(18,6) NULL,
    UnitOfMeasure           VARCHAR(10) NULL,
    -- Spreading pattern for forecast
    SpreadMethodCode        VARCHAR(10) NULL,  -- EVEN, SEASONAL, CUSTOM, PRIOR_YEAR
    SeasonalityFactor       DECIMAL(8,6) NULL,
    -- Source tracking
    SourceSystem            VARCHAR(30) NULL,
    SourceReference         VARCHAR(100) NULL,
    ImportBatchID           UNIQUEIDENTIFIER NULL,  -- GUID type
    -- Allocation tracking
    IsAllocated             BIT NOT NULL DEFAULT 0,
    AllocationSourceLineID  BIGINT NULL,
    AllocationPercentage    DECIMAL(8,6) NULL,
    -- Audit columns
    LastModifiedByUserID    INT NULL,
    LastModifiedDateTime    DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    RowHash                 AS HASHBYTES('SHA2_256', 
                               CONCAT(CAST(GLAccountID AS VARCHAR), '|',
                                      CAST(CostCenterID AS VARCHAR), '|',
                                      CAST(FiscalPeriodID AS VARCHAR), '|',
                                      CAST(/*FinalAmount*/ OriginalAmount + AdjustedAmount AS VARCHAR))) PERSISTED,  -- HASHBYTES computed
    CONSTRAINT PK_BudgetLineItem PRIMARY KEY CLUSTERED (BudgetLineItemID),
    CONSTRAINT FK_BudgetLineItem_Header FOREIGN KEY (BudgetHeaderID) 
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT FK_BudgetLineItem_Account FOREIGN KEY (GLAccountID) 
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT FK_BudgetLineItem_CostCenter FOREIGN KEY (CostCenterID) 
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_BudgetLineItem_Period FOREIGN KEY (FiscalPeriodID) 
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetLineItem_AllocationSource FOREIGN KEY (AllocationSourceLineID) 
        REFERENCES Planning.BudgetLineItem (BudgetLineItemID)
);
GO

-- Unique constraint for natural key
CREATE UNIQUE NONCLUSTERED INDEX UQ_BudgetLineItem_NaturalKey
ON Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID)
WITH (IGNORE_DUP_KEY = ON);  -- IGNORE_DUP_KEY not in Snowflake
GO

-- Filtered index for allocated items
CREATE NONCLUSTERED INDEX IX_BudgetLineItem_Allocated
ON Planning.BudgetLineItem (AllocationSourceLineID, AllocationPercentage)
WHERE IsAllocated = 1;
GO

-- Columnstore for analytics
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_BudgetLineItem_Analytics
ON Planning.BudgetLineItem (
    BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
    OriginalAmount, AdjustedAmount, LocalCurrencyAmount, ReportingCurrencyAmount
);
GO
