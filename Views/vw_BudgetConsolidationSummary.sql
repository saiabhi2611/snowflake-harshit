/*
    vw_BudgetConsolidationSummary - Consolidated view of budget data with hierarchy rollups
    Dependencies: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod
    
    Challenges for Snowflake:
    - Indexed view (materialized view) has different semantics
    - SCHEMABINDING and deterministic function requirements
    - CROSS APPLY with TVF
    - HierarchyID method calls
*/
CREATE VIEW Planning.vw_BudgetConsolidationSummary
WITH SCHEMABINDING
AS
SELECT 
    bh.BudgetHeaderID,
    bh.BudgetCode,
    bh.BudgetName,
    bh.BudgetType,
    bh.ScenarioType,
    bh.FiscalYear,
    fp.FiscalPeriodID,
    fp.FiscalQuarter,
    fp.FiscalMonth,
    fp.PeriodName,
    gla.GLAccountID,
    gla.AccountNumber,
    gla.AccountName,
    gla.AccountType,
    cc.CostCenterID,
    cc.CostCenterCode,
    cc.CostCenterName,
    cc.ParentCostCenterID,
    -- Aggregations for indexed view
    SUM(bli.OriginalAmount) AS TotalOriginalAmount,
    SUM(bli.AdjustedAmount) AS TotalAdjustedAmount,
    SUM(bli.OriginalAmount + bli.AdjustedAmount) AS TotalFinalAmount,
    SUM(ISNULL(bli.LocalCurrencyAmount, 0)) AS TotalLocalCurrency,
    SUM(ISNULL(bli.ReportingCurrencyAmount, 0)) AS TotalReportingCurrency,
    COUNT_BIG(*) AS LineItemCount
FROM Planning.BudgetLineItem bli
INNER JOIN Planning.BudgetHeader bh ON bli.BudgetHeaderID = bh.BudgetHeaderID
INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
INNER JOIN Planning.FiscalPeriod fp ON bli.FiscalPeriodID = fp.FiscalPeriodID
GROUP BY 
    bh.BudgetHeaderID,
    bh.BudgetCode,
    bh.BudgetName,
    bh.BudgetType,
    bh.ScenarioType,
    bh.FiscalYear,
    fp.FiscalPeriodID,
    fp.FiscalQuarter,
    fp.FiscalMonth,
    fp.PeriodName,
    gla.GLAccountID,
    gla.AccountNumber,
    gla.AccountName,
    gla.AccountType,
    cc.CostCenterID,
    cc.CostCenterCode,
    cc.CostCenterName,
    cc.ParentCostCenterID;
GO

-- Create unique clustered index to make it a true indexed/materialized view
CREATE UNIQUE CLUSTERED INDEX IX_vw_BudgetConsolidationSummary
ON Planning.vw_BudgetConsolidationSummary (
    BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID
);
GO

-- Additional indexes for common query patterns
CREATE NONCLUSTERED INDEX IX_vw_BudgetConsolidationSummary_Year
ON Planning.vw_BudgetConsolidationSummary (FiscalYear, FiscalQuarter, FiscalMonth)
INCLUDE (TotalFinalAmount, LineItemCount);
GO
