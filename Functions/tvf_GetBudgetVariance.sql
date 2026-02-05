/*
    tvf_GetBudgetVariance - Inline table-valued function for budget vs actual variance
    Dependencies: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod
    
    Challenges for Snowflake:
    - Inline TVFs compile into the query plan (no Snowflake equivalent)
    - APPLY operator usage patterns need refactoring
    - Multi-statement TVFs perform very differently
*/
CREATE FUNCTION Planning.tvf_GetBudgetVariance (
    @BaseBudgetHeaderID     INT,
    @ComparisonBudgetHeaderID INT,
    @FiscalYear             SMALLINT = NULL,
    @CostCenterID           INT = NULL,
    @AccountType            CHAR(1) = NULL,
    @VarianceThresholdPct   DECIMAL(5,2) = NULL
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    WITH BaseBudget AS (
        SELECT 
            bli.GLAccountID,
            bli.CostCenterID,
            bli.FiscalPeriodID,
            SUM(bli.FinalAmount) AS BudgetAmount
        FROM Planning.BudgetLineItem bli
        WHERE bli.BudgetHeaderID = @BaseBudgetHeaderID
        GROUP BY bli.GLAccountID, bli.CostCenterID, bli.FiscalPeriodID
    ),
    ComparisonBudget AS (
        SELECT 
            bli.GLAccountID,
            bli.CostCenterID,
            bli.FiscalPeriodID,
            SUM(bli.FinalAmount) AS ComparisonAmount
        FROM Planning.BudgetLineItem bli
        WHERE bli.BudgetHeaderID = @ComparisonBudgetHeaderID
        GROUP BY bli.GLAccountID, bli.CostCenterID, bli.FiscalPeriodID
    ),
    Combined AS (
        SELECT 
            COALESCE(b.GLAccountID, c.GLAccountID) AS GLAccountID,
            COALESCE(b.CostCenterID, c.CostCenterID) AS CostCenterID,
            COALESCE(b.FiscalPeriodID, c.FiscalPeriodID) AS FiscalPeriodID,
            ISNULL(b.BudgetAmount, 0) AS BudgetAmount,
            ISNULL(c.ComparisonAmount, 0) AS ComparisonAmount,
            ISNULL(c.ComparisonAmount, 0) - ISNULL(b.BudgetAmount, 0) AS VarianceAmount,
            CASE 
                WHEN ISNULL(b.BudgetAmount, 0) = 0 THEN NULL
                ELSE (ISNULL(c.ComparisonAmount, 0) - ISNULL(b.BudgetAmount, 0)) / b.BudgetAmount * 100
            END AS VariancePercentage
        FROM BaseBudget b
        FULL OUTER JOIN ComparisonBudget c 
            ON b.GLAccountID = c.GLAccountID
            AND b.CostCenterID = c.CostCenterID
            AND b.FiscalPeriodID = c.FiscalPeriodID
    )
    SELECT 
        comb.GLAccountID,
        gla.AccountNumber,
        gla.AccountName,
        gla.AccountType,
        comb.CostCenterID,
        cc.CostCenterCode,
        cc.CostCenterName,
        comb.FiscalPeriodID,
        fp.FiscalYear,
        fp.FiscalMonth,
        fp.PeriodName,
        comb.BudgetAmount,
        comb.ComparisonAmount,
        comb.VarianceAmount,
        comb.VariancePercentage,
        CASE 
            WHEN comb.VarianceAmount > 0 THEN 'FAVORABLE'
            WHEN comb.VarianceAmount < 0 THEN 'UNFAVORABLE'
            ELSE 'ON_TARGET'
        END AS VarianceStatus,
        CASE
            WHEN ABS(comb.VariancePercentage) > ISNULL(@VarianceThresholdPct, 100) THEN 1
            ELSE 0
        END AS ExceedsThreshold
    FROM Combined comb
    INNER JOIN Planning.GLAccount gla ON comb.GLAccountID = gla.GLAccountID
    INNER JOIN Planning.CostCenter cc ON comb.CostCenterID = cc.CostCenterID
    INNER JOIN Planning.FiscalPeriod fp ON comb.FiscalPeriodID = fp.FiscalPeriodID
    WHERE (@FiscalYear IS NULL OR fp.FiscalYear = @FiscalYear)
      AND (@CostCenterID IS NULL OR comb.CostCenterID = @CostCenterID)
      AND (@AccountType IS NULL OR gla.AccountType = @AccountType)
);
GO
