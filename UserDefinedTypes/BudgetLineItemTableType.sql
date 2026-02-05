/*
    BudgetLineItemTableType - Table-valued parameter type for bulk budget operations
    Dependencies: None
    
    NOTE: User-defined table types (TVPs) have NO equivalent in Snowflake.
    These require significant refactoring - typically to:
    1. Temporary tables with INSERT statements
    2. JSON/VARIANT arrays
    3. Staged files
*/
CREATE TYPE Planning.BudgetLineItemTableType AS TABLE (
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    OriginalAmount          DECIMAL(19,4) NOT NULL,
    AdjustedAmount          DECIMAL(19,4) NULL,
    SpreadMethodCode        VARCHAR(10) NULL,
    Notes                   NVARCHAR(500) NULL,
    -- Table types can have indexes in SQL Server 2014+
    INDEX IX_AccountPeriod (GLAccountID, FiscalPeriodID),
    PRIMARY KEY CLUSTERED (GLAccountID, CostCenterID, FiscalPeriodID)
);
GO
